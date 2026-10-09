"""Seeded generator for synthetic banking CDC events and card transactions.

The generator first simulates a *true* event timeline (customer and account inserts,
updates, deletes and card transactions), then simulates an imperfect delivery layer
on top of it, which is what the pipeline has to cope with:

* events are delivered in micro-batch files by event time, but a fraction arrive
  one batch late (out-of-order across files);
* rows inside a file are shuffled (out-of-order within a file);
* a fraction of events are re-delivered (duplicate ``event_id`` / ``txn_id``);
* a few lines per file are malformed (truncated JSON, wrong types, missing keys,
  invalid op codes, unparseable timestamps);
* formatting noise: mixed timestamp formats, lower-case currency, messy e-mails.

All data is synthetic. Output layout::

    <out>/customers/customers_batch_001.jsonl
    <out>/accounts/accounts_batch_001.jsonl
    <out>/transactions/transactions_batch_001.jsonl
    <out>/_manifest.json
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from lakehouse.log import get_logger, setup_logging

log = get_logger(__name__)

FIRST_NAMES = [
    "Avery", "Jordan", "Riley", "Casey", "Morgan", "Quinn", "Rowan", "Sage",
    "Emerson", "Harper", "Parker", "Reese", "Skyler", "Dakota", "Hayden", "Kendall",
]  # fmt: skip
LAST_NAMES = [
    "Ashford", "Bramley", "Calloway", "Dunmore", "Ellery", "Fairbanks", "Greystone",
    "Hollis", "Ingram", "Jarvis", "Kingsley", "Lockwood", "Merriweather", "Northcott",
]  # fmt: skip
CITIES = ["Riverton", "Lakeside", "Fairview", "Hillcrest", "Maplewood", "Brookfield", "Oakridge"]
SEGMENTS = ["RETAIL", "PREMIER", "PRIVATE", "SMALL_BUSINESS"]
RISK_RATINGS = ["LOW", "MEDIUM", "HIGH"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT_CARD"]
MERCHANT_CATEGORIES = {
    "GROCERY": (8, 180),
    "FUEL": (20, 120),
    "RESTAURANTS": (10, 150),
    "TRAVEL": (60, 600),
    "ONLINE_RETAIL": (5, 400),
    "UTILITIES": (30, 250),
    "ENTERTAINMENT": (8, 120),
    "HEALTH": (15, 300),
}
CHANNELS = ["POS", "ECOM", "ATM"]


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    customers: int = 200
    days: int = 30
    batches: int = 3
    start: str = "2026-01-01T00:00:00+00:00"
    late_ratio: float = 0.05
    duplicate_ratio: float = 0.03
    malformed_per_file: int = 3
    txns_per_account_day: float = 0.6

    @property
    def start_dt(self) -> datetime:
        return datetime.fromisoformat(self.start).astimezone(UTC)

    @property
    def end_dt(self) -> datetime:
        return self.start_dt + timedelta(days=self.days)


class _Gen:
    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    # helpers ------------------------------------------------------------------
    def uid(self, prefix: str) -> str:
        return f"{prefix}-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]}"

    def between(self, lo: datetime, hi: datetime) -> datetime:
        if hi <= lo:
            return lo
        secs = int((hi - lo).total_seconds())
        return lo + timedelta(seconds=self.rng.randint(0, secs))

    def fmt_ts(self, ts: datetime) -> str:
        """Mostly ISO-8601, with some format noise the silver layer must normalise."""
        roll = self.rng.random()
        if roll < 0.07:
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        if roll < 0.10:
            return str(int(ts.timestamp() * 1000))
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    # timeline -----------------------------------------------------------------
    def simulate(self) -> dict[str, list[tuple[datetime, dict[str, Any]]]]:
        cfg, rng = self.cfg, self.rng
        events: dict[str, list[tuple[datetime, dict[str, Any]]]] = defaultdict(list)
        start, end = cfg.start_dt, cfg.end_dt

        for i in range(cfg.customers):
            cid = f"C{i + 1:05d}"
            onboarding_window = 0.15 if i < cfg.customers * 0.7 else 0.85
            created = self.between(start, start + timedelta(days=cfg.days * onboarding_window))
            first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
            state = {
                "customer_id": cid,
                "first_name": first,
                "last_name": last,
                "email": f"{first}.{last}{i}@example.com".lower(),
                "city": rng.choice(CITIES),
                "segment": rng.choices(SEGMENTS, weights=[70, 20, 4, 6])[0],
                "risk_rating": rng.choices(RISK_RATINGS, weights=[70, 25, 5])[0],
            }
            events["customers"].append((created, self._cust_event("I", created, state)))

            t = created
            for _ in range(rng.choices([0, 1, 2, 3], weights=[35, 35, 20, 10])[0]):
                t = self.between(t + timedelta(hours=1), end - timedelta(hours=1))
                if t >= end:
                    break
                attr = rng.choice(["email", "city", "segment", "risk_rating"])
                if attr == "email":
                    state["email"] = f"{first}.{last}.{rng.randint(10, 99)}@example.org".lower()
                elif attr == "city":
                    state["city"] = rng.choice([c for c in CITIES if c != state["city"]])
                elif attr == "segment":
                    state["segment"] = rng.choice([s for s in SEGMENTS if s != state["segment"]])
                else:
                    state["risk_rating"] = rng.choice([r for r in RISK_RATINGS if r != state["risk_rating"]])
                events["customers"].append((t, self._cust_event("U", t, state)))

            deleted_at = None
            if rng.random() < 0.05:
                deleted_at = self.between(t + timedelta(hours=2), end - timedelta(minutes=5))
                if deleted_at < end:
                    events["customers"].append((deleted_at, self._cust_event("D", deleted_at, {"customer_id": cid})))
                else:
                    deleted_at = None

            for j in range(rng.choices([1, 2, 3], weights=[55, 35, 10])[0]):
                self._account(events, cid, f"A{i + 1:05d}{j + 1}", created, deleted_at)
        return events

    def _cust_event(self, op: str, ts: datetime, image: dict[str, Any]) -> dict[str, Any]:
        rec = {"event_id": self.uid("ce"), "op": op, "event_ts": self.fmt_ts(ts), **image}
        if op != "D" and self.rng.random() < 0.05:  # formatting noise
            rec["email"] = f"  {rec['email'].upper()} "
        return rec

    def _account(self, events, cid, aid, created, customer_deleted_at) -> None:
        cfg, rng = self.cfg, self.rng
        end = cfg.end_dt
        opened = self.between(created, created + timedelta(days=2))
        if opened >= end:
            return
        acct = {
            "account_id": aid,
            "customer_id": cid,
            "account_type": rng.choices(ACCOUNT_TYPES, weights=[55, 30, 15])[0],
            "status": "ACTIVE",
            "currency": "usd" if rng.random() < 0.08 else "USD",
            "opened_date": opened.date().isoformat(),
        }
        events["accounts"].append((opened, self._acct_event("I", opened, acct)))

        closed_at = None
        if customer_deleted_at is not None:
            closed_at = customer_deleted_at - timedelta(minutes=30)
        elif rng.random() < 0.05:
            closed_at = self.between(opened + timedelta(days=3), end - timedelta(hours=1))
        if rng.random() < 0.08:
            frozen = self.between(opened + timedelta(hours=6), (closed_at or end) - timedelta(hours=2))
            if frozen < (closed_at or end):
                events["accounts"].append((frozen, self._acct_event("U", frozen, {**acct, "status": "FROZEN"})))
                unfrozen = frozen + timedelta(hours=rng.randint(1, 48))
                if unfrozen < (closed_at or end):
                    events["accounts"].append((unfrozen, self._acct_event("U", unfrozen, acct)))
        if closed_at is not None and opened < closed_at < end:
            acct_closed = {**acct, "status": "CLOSED"}
            events["accounts"].append((closed_at, self._acct_event("U", closed_at, acct_closed)))

        # Transactions: opening deposit then daily activity until close/end.
        deposit_ts = opened + timedelta(minutes=1)
        events["transactions"].append(
            (deposit_ts, self._txn(aid, deposit_ts, rng.randint(500, 5000), "CREDIT", None, "TRANSFER"))
        )
        horizon = closed_at or end
        day = opened
        while day < horizon:
            n = sum(1 for _ in range(3) if rng.random() < cfg.txns_per_account_day / 3)
            for _ in range(n):
                ts = self.between(day, min(day + timedelta(days=1), horizon))
                if ts <= deposit_ts or ts >= horizon:
                    continue
                if rng.random() < 0.15:  # salary / refunds / payments in
                    amount = round(rng.uniform(300, 1500), 2)
                    events["transactions"].append((ts, self._txn(aid, ts, amount, "CREDIT", None, "TRANSFER")))
                else:
                    mcc = rng.choice(list(MERCHANT_CATEGORIES))
                    lo, hi = MERCHANT_CATEGORIES[mcc]
                    amount = round(rng.uniform(lo, hi), 2)
                    channel = rng.choice(CHANNELS)
                    events["transactions"].append((ts, self._txn(aid, ts, amount, "DEBIT", mcc, channel)))
            day += timedelta(days=1)

    def _acct_event(self, op: str, ts: datetime, image: dict[str, Any]) -> dict[str, Any]:
        return {"event_id": self.uid("ae"), "op": op, "event_ts": self.fmt_ts(ts), **image}

    def _txn(self, aid, ts, amount, direction, mcc, channel) -> dict[str, Any]:
        return {
            "txn_id": self.uid("tx"),
            "account_id": aid,
            "txn_ts": self.fmt_ts(ts),
            "amount": float(amount),
            "currency": "USD" if self.rng.random() > 0.05 else "usd",
            "direction": direction,
            "merchant_category": mcc,
            "channel": channel,
        }

    # delivery -----------------------------------------------------------------
    def deliver(self, events) -> tuple[dict[str, list[list[str]]], dict[str, Any]]:
        cfg, rng = self.cfg, self.rng
        window = (cfg.end_dt - cfg.start_dt) / cfg.batches
        files: dict[str, list[list[str]]] = {}
        manifest: dict[str, Any] = {}
        for entity in ("customers", "accounts", "transactions"):
            batches: list[list[str]] = [[] for _ in range(cfg.batches)]
            stats = {"events": 0, "late": 0, "duplicates": 0, "malformed": 0}
            for ts, rec in sorted(events[entity], key=lambda e: e[0]):
                b = min(cfg.batches - 1, int((ts - cfg.start_dt) / window))
                if b < cfg.batches - 1 and rng.random() < cfg.late_ratio:
                    b += 1
                    stats["late"] += 1
                line = json.dumps(rec)
                batches[b].append(line)
                stats["events"] += 1
                if rng.random() < cfg.duplicate_ratio:
                    batches[min(cfg.batches - 1, b + rng.randint(0, 1))].append(line)
                    stats["duplicates"] += 1
            for b, lines in enumerate(batches):
                bad = self._malformed(entity, b)
                lines.extend(bad)
                stats["malformed"] += len(bad)
                rng.shuffle(lines)
            files[entity] = batches
            manifest[entity] = stats
        return files, manifest

    def _malformed(self, entity: str, batch: int) -> list[str]:
        key = {"customers": "customer_id", "accounts": "account_id", "transactions": "txn_id"}[entity]
        templates = [
            lambda: f'{{"{key}": "BAD-{batch}", "op": "U", "event_ts": "2026-01-0',  # truncated
            lambda: json.dumps({"event_id": self.uid("bad"), "op": "U", "event_ts": "2026-01-05T10:00:00Z"}),
            lambda: json.dumps(
                {"event_id": self.uid("bad"), "op": "X", "event_ts": "2026-01-05T10:00:00Z", key: "BAD-OP"}
            ),
            lambda: json.dumps({"event_id": self.uid("bad"), "op": "I", "event_ts": "05/01/2026 10am", key: "BAD-TS"}),
        ]
        if entity == "transactions":
            templates = [
                templates[0],
                lambda: json.dumps({"txn_id": self.uid("bad"), "txn_ts": "2026-01-05T10:00:00Z", "amount": 12.5}),
                lambda: json.dumps(
                    {"txn_id": self.uid("bad"), "account_id": "A000011", "txn_ts": "2026-01-05T10:00:00Z",
                     "amount": "twelve", "direction": "DEBIT"}
                ),
                lambda: json.dumps(
                    {"txn_id": self.uid("bad"), "account_id": "A000011", "txn_ts": "yesterday",
                     "amount": 3.5, "direction": "DEBIT"}
                ),
            ]  # fmt: skip
        return [self.rng.choice(templates)() for _ in range(self.cfg.malformed_per_file)]


def generate(out_dir: str | Path, cfg: GeneratorConfig | None = None) -> dict[str, Any]:
    """Generate all landing files under ``out_dir``. Returns the manifest."""
    cfg = cfg or GeneratorConfig()
    out = Path(out_dir)
    gen = _Gen(cfg)
    files, manifest = gen.deliver(gen.simulate())
    for entity, batches in files.items():
        (out / entity).mkdir(parents=True, exist_ok=True)
        for b, lines in enumerate(batches, start=1):
            (out / entity / f"{entity}_batch_{b:03d}.jsonl").write_text("\n".join(lines) + "\n")
    payload = {"config": asdict(cfg), "entities": manifest}
    (out / "_manifest.json").write_text(json.dumps(payload, indent=2))
    log.info("Generated landing data in %s: %s", out, manifest)
    return payload


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Generate synthetic banking CDC landing files.")
    p.add_argument("--out", default="data/landing")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--customers", type=int, default=200)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--batches", type=int, default=3)
    args = p.parse_args(argv)
    setup_logging()
    generate(
        args.out,
        GeneratorConfig(seed=args.seed, customers=args.customers, days=args.days, batches=args.batches),
    )


if __name__ == "__main__":
    main()
