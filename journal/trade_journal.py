from risk.liq_math import direction


def exit_record(pos, reason, exit_price, closed_at, exit_fee):
    """Price PnL is unlevered; net USDT includes both actual fill fees."""
    pnl_pct = (exit_price - pos["entry"]) / pos["entry"] * direction(pos["side"])
    net = pnl_pct * pos["entry"] * pos["amount"] - pos["entry_fee"] - exit_fee
    attribution = ("trailing" if pos.get("trail_moved") else "breakeven") if reason == "stop_loss" and pos.get("breakeven_armed", False) else reason
    return {"signal_id": pos["signal_id"], "side": "long" if direction(pos["side"]) > 0 else "short",
            "entry": pos["entry"], "stop_initial": pos.get("stop_initial"), "stop_final": pos.get("sl"),
            "tp": pos.get("tp"), "stop_pct_initial": pos.get("stop_pct_initial"), "tp_pct": pos.get("tp_pct"),
            "rr_planned": pos.get("rr"), "breakeven_armed": pos.get("breakeven_armed", False),
            "exit_reason": reason, "exit_price": exit_price, "pnl_pct": pnl_pct,
            "time_in_trade_sec": max(0.0, closed_at - pos["opened_at"]),
            "atr_at_entry": pos.get("atr_at_entry"), "leverage": pos.get("leverage"),
            "stop_capped": pos.get("stop_capped"), "risk_capped": pos.get("risk_capped"),
            "exit_attribution": attribution, "net_pnl_usdt": net,
            "atr_source": pos.get("atr_source")}
