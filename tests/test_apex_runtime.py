import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from apex.config import Config
from apex.demo_execution import ApexDemoExchange
from apex.engine import Engine, Instrument
from apex.runtime import PaperSession, run_session, settings
from research.replay import Replay
from test_apex import MINUTE, SYMBOL, depth, paired_tape, ready_state, trade
from test_live_transport import SimulatedBinance, live


def event(symbol, ms, data):
    return dict(kind="event", symbol=symbol, received_ms=MINUTE+ms, data={**data, "s": symbol})


def test_multi_symbol_reservations_share_window_and_release_on_fill_and_expiry():
    other = "ETHUSDT"
    cfg = Config()
    replay = Replay(Engine(cfg, {s: Instrument(.001, 5) for s in (SYMBOL, other)}))
    replay.states = {s: ready_state() for s in (SYMBOL, other)}
    replay.states[other].book.symbol = other
    replay.apply(event(SYMBOL, 5001, trade(4, MINUTE+5001, qty=1)))
    replay.apply(event(other, 5050, depth(103, 103, 102, MINUTE+5050)))
    replay.apply(event(SYMBOL, 5080, depth(103, 103, 102, MINUTE+5080)))
    assert set(replay.orders.pending) == {SYMBOL, other}
    intents = replay.orders.events[:]
    assert len({e["id"] for e in intents}) == 2
    assert all(e["time"] == MINUTE+5080 for e in intents)
    initial = replay.orders.margin
    replay.apply(event(SYMBOL, 5181, depth(104, 104, 103, MINUTE+5181)))
    assert replay.orders.margin < initial
    assert set(replay.open) == {SYMBOL}
    replay.apply(dict(kind="reset_depth", symbol=other, received_ms=MINUTE+6600, data={}))
    assert not replay.orders.pending
    assert replay.orders.events[-1]["status"] == "EXPIRED"
    assert replay.report()["reserved_risk"] == 0


def test_order_admission_cannot_double_spend_unreserved_margin():
    cfg = replace(Config(), margin_fraction=1.)
    state = ready_state()
    engine = Engine(cfg, {SYMBOL: Instrument(.001, 0.)})
    decision, = engine.decide({SYMBOL: state}, MINUTE+5000, 100, 100, 100, [], {})[0]
    a = replace(decision, plan=replace(decision.plan, margin=60))
    b = replace(a, signal=replace(a.signal, symbol="ETHUSDT"),
                exposure=replace(a.exposure, symbol="ETHUSDT", side="sell"))
    replay = Replay(SimpleNamespace(cfg=cfg, decide=lambda *args: ([a, b], [])))
    replay.select_orders(MINUTE+5000)
    assert len(replay.orders.pending) == 1
    assert any(s["reason"] == "INSUFFICIENT_UNRESERVED_MARGIN" for s in replay.signals)


@pytest.mark.asyncio
async def test_paper_runtime_uses_apex_and_resumes_position(tmp_path, monkeypatch):
    monkeypatch.setattr("apex.runtime.time.time_ns", lambda: MINUTE*1_000_000)
    cfg = Config()
    initial = settings(cfg, {SYMBOL: {"step": .001, "min_notional": 5}}, 100, None)

    async def first(symbols, consume):
        assert symbols == (SYMBOL,)
        await consume(paired_tape()[:8])

    with PaperSession(tmp_path) as session:
        session.load(create=initial)
        await run_session(session, feed=first)
        assert session.replay.open[SYMBOL].decision.signal.strategy == "cascade_v2"
        opening = session.replay.open[SYMBOL].order_id
        assert session.replay.balance < 100
    with PaperSession(tmp_path) as session:
        session.load()
        assert session.replay.open[SYMBOL].order_id == opening
        assert not session.replay.states
        async def second(symbols, consume):
            await consume([
                dict(kind="snapshot", symbol=SYMBOL, received_ms=MINUTE+5400,
                     data=dict(lastUpdateId=200, bids=[[105, 50]], asks=[[105.02, 50]])),
                event(SYMBOL, 5500, depth(200, 201, 199, MINUTE+5500)),
            ])
        await run_session(session, feed=second)
        assert not session.replay.open
        trade_row, = session.replay.trades
        assert trade_row["order_id"] == opening and trade_row["reason"] == "TARGET"
        assert trade_row["net_pnl"] == pytest.approx(session.replay.balance-100)
    with PaperSession(tmp_path) as session:
        session.load()
        assert session.replay.trades == [trade_row]


def test_paper_storage_failure_prevents_order_and_second_writer(tmp_path, monkeypatch):
    with PaperSession(tmp_path) as session:
        session.load(create=settings(Config(), {SYMBOL: {"step": .001, "min_notional": 5}}, 100, None))
        with pytest.raises(ValueError, match="already has a writer"):
            with PaperSession(tmp_path):
                pass
        def fsync_failure(*_):
            raise OSError("disk full")
        monkeypatch.setattr("apex.runtime.os.fsync", fsync_failure)
        with pytest.raises(OSError, match="disk full"):
            session.commit(paired_tape()[:8])
        assert session.storage_failed and not session.replay.orders.events and not session.replay.open


def test_apex_demo_cli_gate_runs_before_config_or_network(monkeypatch, capsys):
    import run_demo
    monkeypatch.setattr("sys.argv", ["run_demo.py", "--strategy", "apex", "--enable-demo"])
    monkeypatch.setattr(run_demo, "load_config", lambda: pytest.fail("Config should not load"))
    with pytest.raises(SystemExit):
        run_demo.main()
    assert "DEMO_SOAK_PENDING" in capsys.readouterr().err


@pytest.mark.asyncio
@pytest.mark.parametrize("response", ["lost_ack", "empty", "partial"])
async def test_apex_demo_adapter_reuses_durable_intent_and_native_protection(live, monkeypatch, response):
    live.cfg["exchange"]["leverage_policy"] = "fixed"
    live.cfg["leverage"] = 10
    exchange = ApexDemoExchange(live.cfg, SimpleNamespace(acquire=AsyncMock()))
    exchange.markets = live.markets
    exchange.fence = SimpleNamespace(check=AsyncMock())
    server = SimulatedBinance()
    server.wallet = 1000.
    posts = []
    async def transport(request):
        path = request.url.path
        if path == "/fapi/v3/positionRisk" and server.qty:
            return httpx.Response(200, json=[dict(symbol=SYMBOL, positionAmt=str(server.qty),
                entryPrice="100", markPrice="100", liquidationPrice="90", positionInitialMargin="10")])
        if path == "/fapi/v1/order" and request.method == "POST":
            p = request.url.params
            assert p["type"] == "LIMIT" and p["timeInForce"] == "IOC"
            assert exchange.intents[p["newClientOrderId"]]["status"] == "pending"
            posts.append(p["newClientOrderId"])
            if response == "empty":
                data = dict(orderId=1, clientOrderId=p["newClientOrderId"], status="EXPIRED", executedQty="0")
                return httpx.Response(200, json=data)
            if response == "partial":
                data = server.fill(p["side"], float(p["quantity"])/2, p["newClientOrderId"])
                data["status"] = "EXPIRED"
                return httpx.Response(200, json=data)
            await server(request)
            raise httpx.ReadTimeout("lost mock response", request=request)
        return await server(request)
    await exchange.client.aclose()
    exchange.client = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(transport))
    state = ready_state()
    now = MINUTE+5000
    monkeypatch.setattr("apex.demo_execution.time.time_ns", lambda: now*1_000_000)
    monkeypatch.setattr(exchange, "authorize_apex_entry", lambda: None)
    cfg = Config()
    engine = Engine(cfg, {SYMBOL: Instrument(.002, 5)})
    decision, = engine.decide({SYMBOL: state}, now, 1000, 1000, 1000, [], {})[0]
    decision = replace(decision, signal=replace(decision.signal, reference=100.),
                       plan=replace(decision.plan, stop=99.2, target=101.2))
    safety = SimpleNamespace(allow_entry=AsyncMock(return_value=True))
    try:
        fill = await exchange.enter_apex(decision, cfg, safety, market_state=state, exposures=[],
                                         high_water=1000, correlated={})
        assert len(posts) == 1
        if response == "empty":
            assert fill is None and not exchange.trades and not server.algos
        else:
            assert fill["signal_id"] == posts[0]
            assert fill["amount"] == pytest.approx(decision.plan.quantity/(2 if response == "partial" else 1))
            assert len(server.algos) == 2
        assert await exchange.recover_execution()
        assert len(posts) == 1
    finally:
        await exchange.close()
        await live.close()


@pytest.mark.parametrize("strategy", ["liquidity_reversal", "range_reversion"])
def test_paper_decision_cycle_can_fill_each_alternative_strategy(tmp_path, monkeypatch, strategy):
    cfg = Config(enabled_strategies=(strategy,))
    tape = paired_tape()
    with PaperSession(tmp_path) as session:
        session.load(create=settings(cfg, {SYMBOL: {"step": .001, "min_notional": 5}}, 100, None))
        session.commit(tape[:5])
        state = session.replay.states[SYMBOL]
        features = state.features

        def fixture_features(now, policy, btc=None):
            f = features(now, policy, btc)
            if f is None:
                return None
            return replace(f, trend=0., short_vol=.002, long_vol=.003, range_low=101.5,
                           range_high=108., recent_low=101., flow=.8, ofi=.2, absorption=.8)

        monkeypatch.setattr(state, "features", fixture_features)
        session.commit(tape[5:8])
        position = session.replay.open[SYMBOL]
        assert position.decision.signal.strategy == strategy
        assert position.exit.stop < position.exit.entry < position.exit.target
        assert position.decision.exposure.stop_risk <= 100*cfg.risk_fraction


@pytest.mark.asyncio
async def test_demo_coordinator_routes_real_strategy_decision_to_durable_adapter(live, tmp_path, monkeypatch):
    from apex.demo_runtime import ApexDemoRuntime
    from apex.demo_execution import DEMO_BLOCKERS
    monkeypatch.setattr("apex.demo_runtime.require_demo_release", lambda: None)
    monkeypatch.setattr("paper_runtime.load_config", lambda: live.cfg)
    cfg = Config()
    bot = ApexDemoRuntime(cfg, {SYMBOL: Instrument(.001, 5)}, state_path="apex-state.json")
    bot.exchange.markets = live.markets
    bot.safety.ready = True
    bot.safety.reconcile_once = AsyncMock(return_value=True)
    bot.exchange.enter_apex = AsyncMock(return_value=None)
    bot.exchange._equity = 100.
    bot.exchange._free = 100.
    try:
        for row in paired_tape()[:8]:
            now = row["received_ms"]
            monkeypatch.setattr("apex.demo_runtime.time.time_ns", lambda: now*1_000_000)
            await bot.consume_market([row])
        assert bot.exchange.enter_apex.await_count >= 1
        args = bot.exchange.enter_apex.await_args
        assert args.args[0].signal.strategy == "cascade_v2"
        assert args.args[0].model is None
        assert args.kwargs["market_state"].book.synced
        assert (bot.high_path).exists()
        assert "DEMO_SOAK_PENDING" in DEMO_BLOCKERS
    finally:
        await bot.exchange.close()
        await live.close()


def test_paper_recovers_torn_tail_and_replays_identical_ledger(tmp_path):
    with PaperSession(tmp_path) as session:
        session.load(create=settings(Config(), {SYMBOL: {"step": .001, "min_notional": 5}}, 100, None))
        session.commit(paired_tape())
        orders = session.replay.orders.events[:]
        trades = session.replay.trades[:]
        with session.tape.open("ab") as tape:
            tape.write(b'{"received_ms":')
    with PaperSession(tmp_path) as session:
        session.load()
        assert session.replay.orders.events == orders
        assert session.replay.trades == trades
        assert session.tape.read_bytes().endswith(b"\n")
