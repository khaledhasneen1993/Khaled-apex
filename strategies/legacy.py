"""Pure signal predicates shared with the original Cascade scanner."""


def cascade_signal(rows, movement, volume):
    if len(rows) < 15:
        return None
    change = (rows[-1][4]-rows[-3][4])/rows[-3][4]
    reference = sum(r[5] for r in rows[-15:-3])/12
    spike = rows[-1][5]/reference if reference > 0 else 0
    if abs(change) <= movement or spike <= volume:
        return None
    return {"side": "buy" if change > 0 else "sell", "price": rows[-1][4], "ret": change, "spike": spike}


def imbalance_matches(side, imbalance, threshold=.35):
    return imbalance > threshold if side == "buy" else side == "sell" and imbalance < -threshold
