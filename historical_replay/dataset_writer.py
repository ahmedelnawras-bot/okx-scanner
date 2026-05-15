from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .state import DATA_KEY, LOCAL_DATA_PATH


def append_records(records: Iterable[dict[str, Any]], redis_client: Any | None = None, mirror_path: str | Path | None = None) -> dict[str, Any]:
    rows = []
    for record in records or []:
        if isinstance(record, dict):
            rows.append(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    if not rows:
        return {"ok": True, "written": 0}

    if redis_client is not None:
        redis_client.rpush(DATA_KEY, *rows)

    path = Path(mirror_path or LOCAL_DATA_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(row + "\n")
    return {"ok": True, "written": len(rows)}


def read_recent_records(redis_client: Any | None = None, limit: int = 20) -> list[dict[str, Any]]:
    rows: list[str | bytes] = []
    if redis_client is not None:
        rows = redis_client.lrange(DATA_KEY, max(0, -int(limit)), -1) or []
    elif LOCAL_DATA_PATH.exists():
        with LOCAL_DATA_PATH.open("r", encoding="utf-8") as fh:
            rows = fh.readlines()[-int(limit):]

    decoded: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, bytes):
            row = row.decode("utf-8", errors="replace")
        try:
            value = json.loads(str(row).strip())
            if isinstance(value, dict):
                decoded.append(value)
        except Exception:
            continue
    return decoded
