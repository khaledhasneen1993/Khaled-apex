import asyncio
from dataclasses import replace
import json
from math import isinf
from types import SimpleNamespace
from unittest.mock import AsyncMock

from hypothesis import given, strategies as hs
import pytest

from apex.config import Config
from apex.engine import Engine, Instrument
from apex.safety import Safety, state
from evaluation.promotion import Evidence, gate
from market_data.book import BookGap, OrderBook
from market_data.features import MarketState
from models.labels import label
from models.meta import FEATURES, vector
from models.train import Sample, calibrate, fit, metrics, split
from portfolio.limits import Exposure, check, correlations
from regime.classifier import Regime, classify
from research.replay import Replay, export
from research.statistics import block_bootstrap
from risk.apex_exit import Exit, breakeven
from risk.apex_sizing import plan
from strategies.ranking import Candidate, rank
from strategies.signals import Signal, generate

SYMBOL = "BTCUSDT"
MINUTE = 2_000_000 * 60000


def depth(first, last, prev, ms, bid=None, ask=None):
    return dict(e="depthUpdate", s=SYMBOL, E=ms, U=first, u=last, pu=prev,
                b=bid or [], a=ask or [])


def trade(ident, time, price=102, qty=25):
    return dict(e="aggTrade", s=SYMBOL, E=time, T=time, a=ident, p=price, q=qty, m=False)


def snapshot():
    return dict(lastUpdateId=100, bids=[[101.99, 90]], asks=[[102.01, 90]])


def candle_rows():
    return [[MINUTE+i*60000, 100, 100.1, 99.9, 100, 100] for i in range(-21, 0)]


def ready_state():
    s = MarketState(SYMBOL)
    for candle in candle_rows():
        s.add_candle(*candle)
    s.book.snapshot(snapshot())
    s.ingest(depth(100, 101, 99, MINUTE+4900), MINUTE+4900)
    s.ingest(trade(1, MINUTE-20), MINUTE-20)
    s.ingest(trade(2, MINUTE+300), MINUTE+300)
    s.ingest(trade(3, MINUTE+5000), MINUTE+5000)
    s.ingest(depth(102, 102, 101, MINUTE+5000, [[101.99, 150]]), MINUTE+5000)
    return s


def test_book_requires_bridge_and_rebuilds_after_gap():
    book = OrderBook(SYMBOL)
    book.snapshot(snapshot())
    assert not book.fresh(100, 100)
    assert book.update(depth(80, 99, 79, 100), 100) is None
    assert book.update(depth(100, 101, 99, 102), 103) == (0., 0.)
    ofi, depleted = book.update(depth(102, 102, 101, 104, [[101.99, 120]], [[102.01, 50]]), 105)
    assert ofi > 0 and depleted == pytest.approx(40*102.01)
    assert isinf(book.impact("buy", 100))
    with pytest.raises(BookGap):
        book.update(depth(104, 104, 103, 106), 106)
    assert not book.synced
    with pytest.raises(BookGap):
        book.update(depth(105, 105, 104, 107), 107)
    book.snapshot(dict(snapshot(), lastUpdateId=105))
    assert book.update(depth(105, 106, 104, 108), 108) == (0., 0.)


def test_causal_volume_uses_observed_time_and_closed_bars():
    cfg, s = Config(), ready_state()
    f = s.features(MINUTE+5000, cfg)
    assert f.spike == pytest.approx(6)
    assert f.cvd == 50 and f.flow == 1 and f.ofi > 0
    assert f.funding is None and f.oi_change is None
    assert f.recent_low == 102
    assert s.volume_by_price(MINUTE, MINUTE+5000, .01) == {10200: 50}
    s.add_candle(MINUTE, 200, 200, 200, 200, 50000)
    assert s.features(MINUTE+5000, cfg) == f
    assert s.features(MINUTE+6501, cfg) is None
    s.disconnect()
    assert s.features(MINUTE+5000, cfg) is None


def test_market_data_rejects_old_and_future_events():
    s = ready_state()
    with pytest.raises(ValueError, match="Late market event"):
        s.ingest(trade(4, MINUTE+10000), MINUTE+9000)
    assert not s.book.synced
    with pytest.raises(ValueError, match="Late market event"):
        s.ingest(trade(5, MINUTE), MINUTE+1600)


def test_regimes_and_reversal_require_separate_conditions():
    cfg, f = Config(), ready_state().features(MINUTE+5000, Config())
    assert classify(f, cfg) == Regime.UP
    assert [s.strategy for s in generate(f, Regime.UP, cfg)] == ["cascade_v2"]
    assert classify(replace(f, spread=.02), cfg) == Regime.STRESS
    assert classify(replace(f, short_vol=.08), cfg) == Regime.HIGH_VOL
    assert classify(None, cfg) == Regime.UNCERTAIN
    ranging = replace(f, price=100, trend=0., short_vol=.001, long_vol=.002,
                      range_low=99, range_high=110, recent_low=98)
    assert classify(ranging, cfg) == Regime.RANGE
    assert {s.strategy for s in generate(ranging, Regime.RANGE, cfg)} == {"liquidity_reversal", "range_reversion"}
    assert generate(replace(ranging, flow=0), Regime.RANGE, cfg) == []


def test_ranking_keeps_fresh_best_later_signal_and_discards_expired():
    cfg = Config()
    signal = Signal("A", "buy", "cascade_v2", 100, 100., Regime.UP, .8, .02, 99.)
    old = Candidate(signal, .01, .001, .01, 0., 10000.)
    better = replace(old, signal=replace(signal, symbol="B", asof_ms=160), net_edge=.02)
    assert rank([old, better], 175, cfg) == [better, old]
    assert rank([old, better], 1650, cfg) == [better]
    assert rank([old, better], 150, cfg) == [old]


@given(equity=hs.floats(min_value=100, max_value=100000, allow_nan=False),
       side=hs.sampled_from(["buy", "sell"]))
def test_position_risk_bound_holds_with_precision_and_slippage(equity, side):
    cfg = Config()
    sig = Signal("A", side, "cascade_v2", 10, 100., Regime.UP, .8, .02, 100.)
    f = SimpleNamespace(atr=.6, short_vol=.01, long_vol=.008, flow=.2)
    p = plan(sig, f, cfg, equity, equity, .001, 5., .001)
    assert p.stop_risk <= equity*cfg.risk_fraction
    assert p.notional == p.quantity*100 and p.margin < equity*cfg.margin_fraction
    assert (p.stop < 100 < p.target) if side == "buy" else (p.target < 100 < p.stop)


@pytest.mark.parametrize("side,sign", [("buy", 1), ("sell", -1)])
def test_net_break_even_and_trailing_never_loosen(side, sign):
    cfg = Config()
    price = breakeven(100, side, cfg.fee, cfg.max_slippage)
    fill = price*(1-sign*cfg.max_slippage)
    assert sign*(fill-100)-(100+fill)*cfg.fee == pytest.approx(0, abs=1e-10)
    state = Exit(side, 100., 100-sign*2, 100+sign*20, 0)
    assert state.update(100+sign*5, 1., .01, .0005, cfg) is None
    tightened = state.stop
    assert sign*(tightened-price) >= 0
    assert state.update(100+sign*6, 3., .1, .01, cfg) is None
    assert sign*(state.stop-tightened) >= 0
    assert state.update(state.stop, .1, 0., .01, cfg) == "STOP"


def test_portfolio_blocks_correlation_and_drawdown():
    cfg = Config()
    a = Exposure("BTC", "buy", 10, .3, .01)
    b = Exposure("ETH", "buy", 10, .3, .01)
    assert check([a], b, 100, 100, {}, cfg) == "HIGH_PORTFOLIO_CORRELATION"
    assert check([a], b, 100, 100, {("BTC", "ETH"): 0.0}, cfg) is None
    assert check([], b, 90, 100, {}, cfg) == "DRAWDOWN_OR_INVALID_EQUITY"
    assert correlations({"A": dict(enumerate(range(20))), "B": dict(enumerate(range(20)))})["A", "B"] == 1


def test_model_split_purges_overlap_and_prevents_future_features(tmp_path):
    width = len(FEATURES)
    samples = [Sample(t, t, t+5, [float(i)]*width, outcome)
               for base in (0, 100, 200) for i, outcome in enumerate(("target", "stop", "timeout"))
               for t in [base+i*10]]
    train, calibration, test = split([*samples, Sample(88, 88, 96, [0.]*width, "target")], 100, 200, 5)
    assert [len(batch) for batch in (train, calibration, test)] == [3, 3, 3]
    model = calibrate(fit(train, "test"), calibration)
    assert sum(model.probabilities(test[0].row)) == pytest.approx(1)
    assert 0 <= metrics(model, test)["brier_multiclass"] <= 2
    with pytest.raises(ValueError, match="point-in-time"):
        Sample(10, 11, 15, [0.]*width, "target")
    f = ready_state().features(MINUTE+5000, Config())
    with pytest.raises(ValueError, match="Future"):
        vector(f, Regime.UP, MINUTE+4999, 0)
    assert label(10, 10, [0.]*width, [(20, 95)], "buy", 103, 97, 100, 20).label == "stop"
    assert label(10, 10, [0.]*width, [(35, 105)], "buy", 103, 97, 100, 20) is None


def test_model_failure_fails_closed_without_risk_override():
    class Broken:
        def probabilities(self, row):
            raise ValueError("broken")
    cfg, state = Config(), ready_state()
    engine = Engine(cfg, {SYMBOL: Instrument(.001, 5)}, Broken())
    decisions, rejected = engine.decide({SYMBOL: state}, MINUTE+5000, 100, 100, 100, [], {})
    assert not decisions and (SYMBOL, "MODEL_INFERENCE_FAILED") in rejected
    assert gate(Evidence(0, 0, 0, False, False, False, 0, 0),
                Evidence(0, 0, 0, False, False, False, 0, 0))


def test_replay_uses_post_latency_depth_and_exports_funded_close(tmp_path):
    cfg = Config()
    engine = Engine(cfg, {SYMBOL: Instrument(.001, 5.)})
    replay = Replay(engine)
    replay.states[SYMBOL] = ready_state()
    def emit(kind, t, data):
        replay.apply(dict(received_ms=MINUTE+t, symbol=SYMBOL, kind=kind, data=data))
    emit("event", 5001, trade(4, MINUTE+5001, qty=1))
    emit("event", 5080, depth(103, 103, 102, MINUTE+5080))
    assert replay.orders.pending and not replay.open
    emit("event", 5180, trade(5, MINUTE+5180, qty=1))
    assert not replay.open
    emit("event", 5181, depth(104, 104, 103, MINUTE+5181))
    assert replay.open
    emit("funding_settlement", 5300, dict(settled_ms=MINUTE+5300, rate=.0001, mark=102))
    emit("event", 5400, depth(105, 105, 104, MINUTE+5400,
                              [[101.99, 0], [104., 150]], [[102.01, 0], [104.02, 90]]))
    assert not replay.open and replay.trades[0]["funding"] > 0
    assert replay.trades[0]["net_pnl"] == pytest.approx(replay.balance-100)
    target = tmp_path/"export"
    export(replay, target)
    assert json.loads((target/"report.json").read_text())["metrics"]["trades"] == 1
    assert (target/"signals.csv").exists()


def test_safety_priority_and_repeatable_resampling():
    assert state(data_ok=False, unresolved=True) == Safety.PROTECTIVE
    assert state(data_ok=False) == Safety.DEGRADED
    assert state(data_ok=True, warning=True) == Safety.WARNING
    assert block_bootstrap([1]*30) == (1, 1)


@pytest.mark.asyncio
async def test_concurrent_paper_opens_do_not_duplicate_accounting():
    from paper_account import PaperExchange
    from test_continuous import StreamData
    exchange = PaperExchange(StreamData())
    results = await asyncio.gather(*(exchange.create_order("BTC/USDT:USDT", "market", "buy", .1)
                                     for _ in range(2)), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in results) == 1
    assert len(exchange.trades) == 1


@pytest.mark.asyncio
async def test_paper_direct_order_cannot_reenable_fixed_notional_bypass():
    from paper_account import PaperExchange
    from test_continuous import StreamData
    exchange = PaperExchange(StreamData())
    with pytest.raises(ValueError, match="risk budget"):
        await exchange.create_order("BTC/USDT:USDT", "market", "buy", 1., {"use_paper_notional": True})
    assert exchange.balance == 100 and not exchange.trades


def test_ohlcv_segments_do_not_trade_across_gap():
    from research.ohlcv import Bar, segments, simulate
    bars = [Bar(i*60000, 100, 100, 100, 100, 10) for i in range(20)]
    bars.extend(Bar(i*60000, 100, 100, 100, 100, 10) for i in range(30, 50))
    assert [len(part) for part in segments(bars)] == [20, 20]
    assert simulate(bars[:20], stop_pct=.08, from_ms=0)["metrics"]["trades"] == 0


def test_paired_signal_diagnostic_is_not_a_trade_simulator():
    from evaluation.paired import champion_prefilter, PairedDiagnostic
    s = ready_state()
    s.ingest(depth(103, 103, 102, MINUTE+5100, [[101.99, 400]]), MINUTE+5100)
    rows = [*candle_rows(), [MINUTE, 100, 102, 100, 102, 500]]
    assert champion_prefilter(rows, s.book, MINUTE+5100, 1500) == "buy"
    assert champion_prefilter(rows, s.book, MINUTE+6700, 1500) is None
    assert PairedDiagnostic(Config()).report()["fidelity"] == "paired_signal_diagnostic"


def test_model_approval_binds_artifact_and_configuration(tmp_path):
    from dataclasses import asdict
    from hashlib import sha256
    from models.meta import LABELS, Model
    cfg = Config()
    model = Model("fixture", [0.]*len(FEATURES), [1.]*len(FEATURES),
                  [[0.]*(len(FEATURES)+1) for _ in LABELS], 1.)
    path, approval = tmp_path/"model.json", tmp_path/"approval.json"
    path.write_text(json.dumps(dict(features=FEATURES, labels=LABELS, model=asdict(model))))
    approval.write_text(json.dumps(dict(artifact_sha256=sha256(path.read_bytes()).hexdigest(),
                                        config_sha256=cfg.fingerprint, scope="shadow", approved_by="operator")))
    assert Model.load_approved(path, approval, cfg.fingerprint).version == "fixture"
    with pytest.raises(ValueError, match="approval"):
        Model.load_approved(path, approval, replace(cfg, max_age_ms=2000).fingerprint)
    path.write_text(path.read_text()+"\n")
    with pytest.raises(ValueError, match="approval"):
        Model.load_approved(path, approval, cfg.fingerprint)


def test_demo_entrypoint_requires_enable_flag(monkeypatch):
    import run_demo
    monkeypatch.setattr("sys.argv", ["run_demo.py"])
    config = AsyncMock(side_effect=AssertionError("must stop before runtime construction"))
    monkeypatch.setattr(run_demo, "load_config", config)
    with pytest.raises(SystemExit) as error:
        run_demo.main()
    assert error.value.code == 2
    config.assert_not_called()


def test_paper_export_reads_state_without_creating_client(tmp_path):
    from tools.paper_export import export
    source = tmp_path/"state.json"
    source.write_text(json.dumps(dict(settings={"execution_mode": "paper"}, trades=[])))
    target = tmp_path/"output"
    export(source, target)
    assert json.loads((target/"paper.json").read_text()) == dict(fills=[], opened=[], closed=[])
    assert (target/"closed.csv").exists()
    source.write_text(json.dumps(dict(settings={"execution_mode": "testnet"}, trades=[])))
    with pytest.raises(ValueError, match="Paper state"):
        export(source, tmp_path/"blocked")


def test_replay_partial_entry_and_sticky_stop_keep_funding_and_fees():
    from research.replay import Experiment
    replay = Replay(Engine(Config(), {SYMBOL: Instrument(.001, 5.)}),
                    experiment=Experiment(partial_fills=True))
    replay.states[SYMBOL] = ready_state()

    def emit(kind, ms, data):
        replay.apply(dict(received_ms=MINUTE+ms, symbol=SYMBOL, kind=kind, data=data))

    emit("event", 5001, trade(4, MINUTE+5001, qty=1))
    emit("event", 5080, depth(103, 103, 102, MINUTE+5080))
    planned = replay.orders.pending[SYMBOL].decision.plan.quantity
    emit("event", 5181, depth(104, 104, 103, MINUTE+5181, ask=[[102.01, .100]]))
    position = replay.open[SYMBOL]
    assert planned > position.amount == .100
    emit("funding_settlement", 5300, dict(settled_ms=MINUTE+5300, rate=.0001, mark=102))
    emit("event", 5400, depth(105, 105, 104, MINUTE+5400, bid=[[101.99, 0], [100, .040]]))
    assert position.amount == pytest.approx(.06)
    assert position.exit_reason == "STOP"
    emit("event", 5500, depth(106, 106, 105, MINUTE+5500))
    assert len(replay.exit_fills) == 1
    emit("event", 5600, depth(107, 107, 106, MINUTE+5600, bid=[[100, 0], [101.95, .070]]))
    assert not replay.open
    result, = replay.trades
    assert result["exit"] == pytest.approx((.04*100+.06*101.95)/.1)
    assert result["net_pnl"] == pytest.approx(replay.balance-100)
    assert result["funding"] == pytest.approx(.1*102*.0001)
    assert sum(f["amount"] for f in replay.exit_fills) == pytest.approx(.1)
    assert replay.report()["partial_entries"] == 1


def test_scale_out_is_limit_constrained_and_runs_only_once():
    from research.replay import Experiment
    replay = Replay(Engine(Config(), {SYMBOL: Instrument(.001, 5.)}),
                    experiment=Experiment(scale_out_fraction=.5, scale_out_r=.5))
    replay.states[SYMBOL] = ready_state()

    def emit(ms, data):
        replay.apply(dict(received_ms=MINUTE+ms, symbol=SYMBOL, kind="event", data=data))

    emit(5001, trade(4, MINUTE+5001, qty=1))
    emit(5080, depth(103, 103, 102, MINUTE+5080))
    emit(5181, depth(104, 104, 103, MINUTE+5181))
    position = replay.open[SYMBOL]
    original_risk = position.decision.exposure.stop_risk
    emit(5400, depth(105, 105, 104, MINUTE+5400,
                     bid=[[101.99, 0], [102.5, 150]], ask=[[102.01, 0], [102.52, 90]]))
    fill, = replay.exit_fills
    assert fill["reason"] == "SCALE_OUT"
    assert 0 < position.amount < position.initial_amount
    assert position.decision.exposure.stop_risk < original_risk
    emit(5500, depth(106, 106, 105, MINUTE+5500))
    assert len(replay.exit_fills) == 1
    emit(5600, depth(107, 107, 106, MINUTE+5600,
                     bid=[[102.5, 0], [104., 150]], ask=[[102.52, 0], [104.02, 90]]))
    assert [f["reason"] for f in replay.exit_fills] == ["SCALE_OUT", "TARGET"]
    assert replay.trades[0]["net_pnl"] == pytest.approx(replay.balance-100)


def paired_tape():
    def row(kind, ms, data):
        return dict(kind=kind, received_ms=MINUTE+ms, symbol=SYMBOL, data=data)

    return [
        row("candles", 0, candle_rows()),
        row("event", 0, trade(1, MINUTE)),
        row("snapshot", 4500, snapshot()),
        row("event", 4900, depth(100, 101, 99, MINUTE+4900)),
        row("event", 5000, trade(2, MINUTE+5000, qty=60)),
        row("event", 5080, depth(102, 102, 101, MINUTE+5080, bid=[[101.99, 400]])),
        row("event", 5090, dict(e="kline", E=MINUTE+5090, s=SYMBOL,
                                k=dict(t=MINUTE, i="1m", x=False, o=100., h=102., l=100., c=102., v=500.))),
        row("event", 5250, depth(103, 103, 102, MINUTE+5250)),
        row("event", 5360, depth(104, 104, 103, MINUTE+5360)),
        row("funding_settlement", 5400, dict(settled_ms=MINUTE+5400, rate=.0001, mark=102)),
        row("event", 5500, depth(105, 105, 104, MINUTE+5500,
                                 [[101.99, 0], [105, 400]], [[102.01, 0], [105.02, 90]])),
        row("event", 5600, depth(106, 106, 105, MINUTE+5600,
                                 [[105, 0], [103, 400]], [[105.02, 0], [103.02, 90]])),
    ]


def test_costed_comparison_uses_same_tape_and_funding_but_never_promotes():
    from evaluation.costed import compare
    report = compare(paired_tape(), Config(), {SYMBOL: Instrument(.001, 5.)}, equity=1000)
    assert report["source_events"] == len(paired_tape())
    assert report["promotion_eligible"] is False
    assert report["champion"]["funding_settlements"] == report["challenger"]["funding_settlements"] == 1
    assert report["champion"]["metrics"]["trades"] == report["challenger"]["metrics"]["trades"] == 1
    assert report["champion"]["by_strategy"]["champion_proxy"]["count"] == 1
    assert report["challenger"]["by_strategy"]["cascade_v2"]["count"] == 1


def test_sample_builder_freezes_model_inputs_before_fills_and_censors_reset():
    from models.build_samples import SampleBuilder
    from models.meta import FEATURES
    tape = paired_tape()
    engine = Engine(Config(), {SYMBOL: Instrument(.001, 5.)})
    builder = SampleBuilder(Replay(engine, equity=1000), 2000)
    samples = [sample for row in tape if (sample := builder.apply(row)) is not None]
    sample, = samples
    assert sample.label == "target"
    assert sample.decision_ms == MINUTE+5080
    assert sample.label_end_ms == MINUTE+5500
    assert len(sample.row) == len(FEATURES)
    builder = SampleBuilder(Replay(engine, equity=1000), 2000)
    for row in tape[:8]:
        assert builder.apply(row) is None
    assert builder.active
    assert builder.apply(dict(kind="reset_depth", symbol=SYMBOL, received_ms=MINUTE+5300, data={})) is None
    assert builder.censored == 1
    assert not builder.active


def test_delayed_fill_rechecks_reduced_risk_budget():
    replay = Replay(Engine(Config(), {SYMBOL: Instrument(.001, 5.)}))
    replay.states[SYMBOL] = ready_state()

    def emit(ms, data):
        replay.apply(dict(received_ms=MINUTE+ms, symbol=SYMBOL, kind="event", data=data))

    emit(5001, trade(4, MINUTE+5001, qty=1))
    emit(5080, depth(103, 103, 102, MINUTE+5080))
    assert replay.orders.pending
    replay.balance = 97.
    emit(5181, depth(104, 104, 103, MINUTE+5181))
    assert not replay.open
    assert any(item["reason"] == "RISK_BUDGET_CHANGED_AT_FILL" for item in replay.signals)


def test_replay_time_stop_waits_for_thesis_loss(monkeypatch):
    from research.replay import Experiment
    replay = Replay(Engine(Config(), {SYMBOL: Instrument(.001, 5.)}),
                    experiment=Experiment(thesis_timeout_ms=100))
    state = ready_state()
    replay.states[SYMBOL] = state
    features = state.features

    def changing_features(now, cfg, btc=None):
        observed = features(now, cfg, btc)
        if observed is not None and now >= MINUTE+5400:
            return replace(observed, trend=0.)
        return observed

    monkeypatch.setattr(state, "features", changing_features)

    def emit(ms, data):
        replay.apply(dict(received_ms=MINUTE+ms, symbol=SYMBOL, kind="event", data=data))

    emit(5001, trade(4, MINUTE+5001, qty=1))
    emit(5080, depth(103, 103, 102, MINUTE+5080))
    emit(5181, depth(104, 104, 103, MINUTE+5181))
    emit(5320, depth(105, 105, 104, MINUTE+5320))
    assert replay.open
    emit(5400, depth(106, 106, 105, MINUTE+5400))
    assert replay.trades[0]["reason"] == "TIME_STOP"
