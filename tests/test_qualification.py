from dataclasses import replace
from hypothesis import given, strategies as st
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from apex.config import Config
from apex.demo_execution import ApexDemoExchange
from apex.engine import Engine, Instrument
from apex.order_authorization import EntryOrder, check_entry
from apex.qualification import AccountFence, Approval, preflight, qualification_config
from models.meta import FEATURES
from models.train import Sample, calibrate, diagnostics, fit, walk_forward
from test_apex import MINUTE, SYMBOL, ready_state
from test_live_transport import live, SimulatedBinance


def test_local_approval_exact_private_and_expires(tmp_path):
    path = tmp_path/"approval.json"
    row = dict(scope="binance-demo-qualification", fingerprint="cfg", account_alias="Acct",
               approved_by="operator", expires_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())
    path.write_text(json.dumps(row))
    with pytest.raises(ValueError, match="PRIVATE_FILE"):
        Approval.load(path, "cfg")
    path.chmod(0o600)
    assert Approval.load(path, "cfg").account_alias == "Acct"
    with pytest.raises(ValueError, match="INVALID_OR_EXPIRED"):
        Approval.load(path, "changed")
    assert qualification_config(Config()).max_positions == 1


@pytest.mark.asyncio
async def test_cross_device_fence_sticky_and_only_owner_can_release():
    store = {}
    async def server(request):
        cmd, *args = json.loads(request.content)
        if cmd == "SET":
            key, owner, mode = args
            assert mode == "NX"
            result = None if key in store else "OK"
            if result:
                store[key] = owner
        elif cmd == "GET":
            result = store.get(args[0])
        else:
            script, keys, key, owner = args
            assert cmd == "EVAL" and keys == "1"
            result = int(store.get(key) == owner)
            if result:
                del store[key]
        return httpx.Response(200, json={"result": result})
    async with httpx.AsyncClient(base_url="https://fake.invalid", transport=httpx.MockTransport(server)) as client:
        a, b = AccountFence(client, "Acct"), AccountFence(client, "Acct")
        await a.acquire()
        with pytest.raises(ValueError, match="ALREADY_OWNED"):
            await b.acquire()
        await b.release()
        await a.check()
        with pytest.raises(ValueError, match="FENCE_LOST"):
            await b.check()
        await a.release()
        await b.acquire()
        await b.check()


def test_order_fingerprint_and_stale_or_underfunded_order():
    state = ready_state()
    order = EntryOrder(SYMBOL, "buy", ".1", "LIMIT_IOC", "102.25", 101.2, 103.3,
                       .2, 10, "cfg", "cascade_v2", 1, "acct")
    now = MINUTE+5000
    assert not check_entry(order, state.book, now, now, 1500, .002, 100, 100, .0005)
    for field, value in (("account_version", "other"), ("stop", 101.1), ("quantity", ".2"), ("strategy_version", 2)):
        assert replace(order, **{field: value}).digest != order.digest
    assert "STALE_OR_UNSYNCED_MARKET" in check_entry(order, state.book, now+1501, now, 1500, .002, 100, 100, .0005)
    assert "INSUFFICIENT_MARGIN" in check_entry(order, state.book, now, now, 1500, .002, 100, .1, .0005)


@pytest.mark.asyncio
async def test_qualification_preflight_is_get_only_and_checks_account_alias(live, tmp_path):
    policy = tmp_path/"operator.json"
    policy.write_text(json.dumps(dict(mode="testnet", dedicated_account=True, futures_only=True,
        withdrawals_disabled=True, ip_whitelist_verified=True, last_rotated_at=datetime.now(timezone.utc).isoformat())))
    live.cfg["rollout"]["operator_policy_file"] = str(policy)
    live.load_markets = AsyncMock()
    live.refresh_account = AsyncMock()
    live.get_open_orders = AsyncMock(return_value=[])
    live.leverage_brackets = AsyncMock(return_value=[{"initialLeverage": 10}])
    live._equity, live._free = 100., 100.
    async def request(method, path, *args, **kwargs):
        assert method == "GET"
        return {"/fapi/v3/balance": [{"asset": "USDT", "accountAlias": "Acct"}],
                "/fapi/v1/accountConfig": {"canTrade": True, "dualSidePosition": False, "multiAssetsMargin": False},
                "/fapi/v1/symbolConfig": [{"symbol": SYMBOL, "marginType": "ISOLATED"}]}[path]
    live._request = request
    try:
        result = await preflight(live, [SYMBOL], expected_alias="Acct")
        assert result["gate"] == "READ_ONLY_ACCOUNT_CHECK_PASSED"
        assert result["order_submission_verified"] is False
        with pytest.raises(ValueError, match="DEMO_ACCOUNT_MISMATCH"):
            await preflight(live, [SYMBOL], expected_alias="other")
        live.get_open_orders.return_value = [{}]
        with pytest.raises(ValueError, match="FLAT_FUNDED_ACCOUNT"):
            await preflight(live, [SYMBOL], expected_alias="Acct")
    finally:
        await live.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("change_at_send", [False, True])
async def test_qualified_order_uses_real_gate_and_fence_in_http_mock(live, monkeypatch, change_at_send):
    cfg = qualification_config(Config())
    live.cfg["leverage"] = 10
    live.cfg["exchange"]["leverage_policy"] = "fixed"
    exchange = ApexDemoExchange(live.cfg, SimpleNamespace(acquire=AsyncMock()))
    exchange.markets = live.markets
    exchange.approval = Approval("approved-cfg", "Acct", "operator",
                                 datetime.now(timezone.utc)+timedelta(hours=1))
    exchange.fence = SimpleNamespace(check=AsyncMock())
    state = ready_state()
    now = MINUTE+5000
    monkeypatch.setattr("apex.demo_execution.time.time_ns", lambda: now*1_000_000)
    server = SimulatedBinance()
    server.wallet = 1000.
    server.price = 102.01
    calls = []
    account_calls = 0
    async def transport(request):
        nonlocal account_calls
        path = request.url.path
        if path == "/fapi/v3/account":
            account_calls += 1
            if change_at_send and account_calls == 3:
                server.wallet -= 1
        if path == "/fapi/v3/positionRisk" and server.qty:
            return httpx.Response(200, json=[dict(symbol=SYMBOL, positionAmt=str(server.qty),
                entryPrice="102.01", markPrice="102.01", liquidationPrice="91", positionInitialMargin="10")])
        if path == "/fapi/v1/order" and request.method == "POST":
            client_id = request.url.params["newClientOrderId"]
            assert exchange.intents[client_id]["params"]["apex_plan"]["order_fingerprint"]
            calls.append(client_id)
        return await server(request)
    await exchange.client.aclose()
    exchange.client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(transport))
    engine = Engine(cfg, {SYMBOL: Instrument(.001, 5)})
    decision, = engine.decide({SYMBOL: state}, now, 1000, 1000, 1000, [], {})[0]
    safety = SimpleNamespace(allow_entry=AsyncMock(return_value=True),
                             kill=SimpleNamespace(day_start_equity=1000., state=SimpleNamespace(daily_pnl=0.)))
    try:
        kwargs = dict(market_state=state, exposures=[], high_water=1000, correlated={})
        if change_at_send:
            with pytest.raises(ValueError, match="ACCOUNT_CHANGED_AT_SEND"):
                await exchange.enter_apex(decision, cfg, safety, **kwargs)
            assert not calls
        else:
            result = await exchange.enter_apex(decision, cfg, safety, **kwargs)
            assert len(calls) == 1 and result["signal_id"] == calls[0]
            assert sum(order["algoStatus"] == "NEW" for order in server.algos.values()) == 2
            from journal.wal import load_durable
            intent = load_durable(exchange.intent_path)[calls[0]]
            assert intent["params"]["apex_plan"]["stop"] == decision.plan.stop
    finally:
        await exchange.close()
        await live.close()


def test_ml_walk_forward_provenance_and_drift():
    count = len(FEATURES)
    rows = [Sample(t+i, t+i, t+i+4, [float(i)]*count, label)
            for t in (0, 100, 200) for i, label in enumerate(("target", "stop", "timeout"))]
    train, calibration, test = next(walk_forward(rows, [100, 200, 300], 1))
    assert max(s.label_end_ms for s in train) < min(s.decision_ms for s in calibration)
    model = calibrate(fit(train, "synthetic", iterations=2), calibration)
    assert diagnostics(model, test)["class_counts"]["target"] == 1
    with pytest.raises(ValueError, match="provenance"):
        Sample(5, 5, 10, [0.]*count, "target",
               dict(event_ms=2, book_received_ms=6, available_ms=5))


@given(st.floats(min_value=2., max_value=100., allow_nan=False, allow_infinity=False))
def test_four_candidate_window_never_double_reserves_cash(cash):
    from research.replay import Replay
    cfg = Config()
    state = ready_state()
    engine = Engine(cfg, {SYMBOL: Instrument(.001, 0.)})
    candidate, = engine.decide({SYMBOL: state}, MINUTE+5000, 100, 100, 100, [], {})[0]
    decisions = []
    for raw, side in ((SYMBOL, "buy"), ("ETHUSDT", "buy"), ("SOLUSDT", "sell"), ("BNBUSDT", "sell")):
        decisions.append(replace(candidate, signal=replace(candidate.signal, symbol=raw, side=side),
                                 exposure=replace(candidate.exposure, symbol=raw, side=side),
                                 plan=replace(candidate.plan, margin=4.)))
    simulator = SimpleNamespace(cfg=cfg, decide=lambda *args: (decisions, []))
    a, b = Replay(simulator, equity=100), Replay(simulator, equity=100)
    for replay in (a, b):
        replay.balance = cash
        replay.high_water = cash
        replay.select_orders(MINUTE+5000)
        assert replay.orders.margin <= cash
        assert sum(p.stop_risk for p in replay.orders.exposures()) <= cash*cfg.aggregate_risk
        if cash == 100.:
            assert len(replay.orders.pending) == 4
        if cash <= 4.:
            assert not replay.orders.pending
        ids = {o.id for o in replay.orders.pending.values()}
        assert len(ids) == len(replay.orders.pending)
    assert a.orders.events == b.orders.events
