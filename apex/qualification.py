"""Operator approval and account-wide fencing for restricted Binance Demo."""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from math import isfinite
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import json
import os

import httpx

from exchange.demo import validate_demo
from exchange.rollout import require_operator_policy, strategy_fingerprint

DAILY_LOSS = .005
RELEASE_BLOCKERS = ("DEMO_SOAK_PENDING", "STRATEGY_EDGE_UNPROVEN")


def fingerprint(payload):
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def source_fingerprint():
    root = Path(__file__).resolve().parents[1]
    files = [root/name for name in ("run_demo.py", "baseline.json", "paper_runtime.py",
             "paper_account.py", "continuous_scanner.py", "binance_cascade_hunter_dynamic_v2.py",
             "binance_cascade_hunter_dynamic_v2_paper.py")]
    for name in ("apex", "exchange", "infra", "journal", "market_data", "models", "portfolio", "regime", "risk", "strategies"):
        files.extend((root/name).glob("*.py"))
    return fingerprint({str(p.relative_to(root)): sha256(p.read_bytes()).hexdigest() for p in sorted(files)})


def qualification_config(cfg):
    from dataclasses import replace
    return replace(cfg, max_positions=1, risk_fraction=.0025, aggregate_risk=.0025,
                   cluster_risk=.0025)


def settings_digest(cfg, instruments, baseline, endpoint):
    validate_demo(baseline)
    if cfg.leverage > 10 or baseline["leverage"] != cfg.leverage or baseline["exchange"]["leverage_policy"] != "fixed":
        raise ValueError("QUALIFICATION_REQUIRES_FIXED_LEVERAGE_AT_MOST_10")
    return fingerprint(dict(apex=cfg.fingerprint, baseline=strategy_fingerprint(baseline),
                            instruments={s: asdict(i) for s, i in instruments.items()},
                            endpoint=endpoint, venue=baseline["exchange"]["base_url_testnet"],
                            feed=baseline["exchange"]["ws_url_testnet"], fee=baseline["fee_rate"],
                            protection=baseline["exchange"]["working_type"],
                            code=source_fingerprint(), daily_loss=DAILY_LOSS))


def redis_endpoint():
    endpoint = os.environ.get("APEX_FENCE_URL", "").rstrip("/")
    parts = urlsplit(endpoint)
    if (parts.scheme != "https" or not parts.hostname or not parts.hostname.endswith(".upstash.io")
            or parts.netloc != parts.hostname or parts.path or parts.query or parts.fragment):
        raise ValueError("QUALIFICATION_REQUIRES_UPSTASH_HTTPS_ENDPOINT")
    return endpoint


@dataclass(frozen=True)
class Approval:
    digest: str
    account_alias: str
    approved_by: str
    expires_at: datetime

    @classmethod
    def load(cls, path, expected):
        file = Path(path)
        if not file.is_file() or file.stat().st_mode & 0o077:
            raise ValueError("QUALIFICATION_APPROVAL_REQUIRES_PRIVATE_FILE")
        row = json.loads(file.read_text())
        expiry = datetime.fromisoformat(row["expires_at"])
        if expiry.tzinfo is None:
            raise ValueError("QUALIFICATION_EXPIRY_REQUIRES_TIMEZONE")
        hours = (expiry-datetime.now(timezone.utc)).total_seconds()/3600
        alias = row["account_alias"]
        if (row.get("scope") != "binance-demo-qualification" or row.get("fingerprint") != expected
                or not isinstance(alias, str) or not alias or not alias.isascii()
                or not isinstance(row.get("approved_by"), str) or not row["approved_by"].strip()
                or not 0 < hours <= 96):
            raise ValueError("QUALIFICATION_APPROVAL_INVALID_OR_EXPIRED")
        return cls(expected, alias, row["approved_by"], expiry)

    def check_entry(self):
        if datetime.now(timezone.utc) >= self.expires_at:
            raise ValueError("QUALIFICATION_APPROVAL_EXPIRED")


class AccountFence:
    """Sticky ownership. No expiry, automatic stealing or unsafe crash release."""

    def __init__(self, client, account_alias):
        self.client = client
        self.key = "apex:binance-demo:"+sha256(account_alias.encode()).hexdigest()
        self.owner = uuid4().hex

    async def command(self, *args):
        try:
            response = await self.client.post("/", json=list(args))
            response.raise_for_status()
            payload = response.json()
            if "error" in payload or "result" not in payload:
                raise ValueError()
            return payload["result"]
        except (httpx.HTTPError, ValueError, TypeError):
            raise ValueError("ACCOUNT_FENCE_UNAVAILABLE") from None

    async def acquire(self):
        if await self.command("SET", self.key, self.owner, "NX") != "OK":
            raise ValueError("ACCOUNT_ALREADY_OWNED")

    async def check(self):
        if await self.command("GET", self.key) != self.owner:
            raise ValueError("ACCOUNT_FENCE_LOST")

    async def release(self):
        await self.command("EVAL",
            "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0",
            "1", self.key, self.owner)


async def account_alias(exchange):
    rows = await exchange._request("GET", "/fapi/v3/balance", retry=True)
    aliases = {r.get("accountAlias") for r in rows if r.get("asset") == "USDT"}
    if len(aliases) != 1 or not all(isinstance(a, str) and a and a.isascii() for a in aliases):
        raise ValueError("DEMO_ACCOUNT_ALIAS_UNVERIFIED")
    return aliases.pop()


async def preflight(exchange, symbols, *, expected_alias=None):
    validate_demo(exchange.cfg)
    require_operator_policy(exchange.cfg, "testnet")
    alias = await account_alias(exchange)
    if expected_alias is not None and alias != expected_alias:
        raise ValueError("DEMO_ACCOUNT_MISMATCH")
    account = await exchange._request("GET", "/fapi/v1/accountConfig", retry=True)
    if (account.get("canTrade") is not True or account.get("dualSidePosition") is not False
            or account.get("multiAssetsMargin") is not False):
        raise ValueError("DEMO_REQUIRES_TRADING_ENABLED_ONE_WAY_SINGLE_ASSET")
    await exchange.load_markets()
    await exchange.refresh_account()
    if (not all(isfinite(v) and v > 0 for v in (exchange.equity, exchange.free))
            or exchange.positions or await exchange.get_open_orders()):
        raise ValueError("DEMO_QUALIFICATION_REQUIRES_FLAT_FUNDED_ACCOUNT")
    from exchange.orders import TERMINAL_ORDER_STATUSES
    for intent in exchange.intents.values():
        if intent["status"] == "rejected":
            continue
        record = exchange.executions.get(exchange.execution_key(intent["symbol"], intent.get("order_id")), {})
        if (intent["status"] == "pending" or (intent["type"] == "market" and
                (record.get("order", {}).get("status") not in TERMINAL_ORDER_STATUSES
                 or record.get("fill_price_pending") or record.get("risk_error")
                 or record.get("trade", {}).get("accounting_status") == "pending"))
                or intent["type"] != "market" and not intent.get("protection_committed")):
            raise ValueError("DEMO_LOCAL_INTENT_UNRESOLVED")
    if exchange.pending_protection or any(f.get("status") != "resolved" for f in exchange.management_faults.values()):
        raise ValueError("DEMO_LOCAL_PROTECTION_UNRESOLVED")
    for raw in symbols:
        symbol = exchange.symbol(raw)
        rules = await exchange.leverage_brackets(symbol)
        config = await exchange._request("GET", "/fapi/v1/symbolConfig", {"symbol": raw}, retry=True)
        if (not isinstance(config, list) or len(config) != 1 or config[0].get("symbol") != raw
                or config[0].get("marginType") != "ISOLATED"):
            raise ValueError("DEMO_SYMBOL_REQUIRES_ISOLATED_MARGIN")
        if not any(int(r["initialLeverage"]) >= exchange.cfg["leverage"] for r in rules):
            raise ValueError("DEMO_CONFIGURED_LEVERAGE_UNAVAILABLE")
    return dict(account_alias=alias, mode="testnet", equity=exchange.equity, free=exchange.free,
                symbols=list(symbols), gate="READ_ONLY_ACCOUNT_CHECK_PASSED",
                order_submission_verified=False, protection_verified=False)
