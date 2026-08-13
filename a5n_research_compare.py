"""Read-only A5-N baseline vs shadow-B evidence summary."""
from __future__ import annotations

import collections
import json
from pathlib import Path
from typing import Any


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def latest_run(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    latest = max(str(x.get("scan_started_at", "")) for x in rows)
    return [x for x in rows if str(x.get("scan_started_at", "")) == latest]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = latest_run(rows)
    return {
        "sample_count": len(rows),
        "states": dict(collections.Counter(str(x.get("strategy_state", "UNKNOWN")) for x in rows)),
        "reject_reasons": dict(collections.Counter(r for x in rows for r in (x.get("reject_reason") or []))),
    }


def main() -> None:
    root = Path("ledgers")
    report = {
        "baseline_premarket": summarize(read_jsonl(root / "a5n_premarket_ledger.jsonl")),
        "variant_b_premarket": summarize(read_jsonl(root / "a5n_b_shadow_premarket_ledger.jsonl")),
        "baseline_intraday": summarize(read_jsonl(root / "a5n_signal_ledger.jsonl")),
        "variant_b_intraday": summarize(read_jsonl(root / "a5n_b_shadow_signal_ledger.jsonl")),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
