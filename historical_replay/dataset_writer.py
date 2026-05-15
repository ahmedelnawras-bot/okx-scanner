from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any, Iterable, Iterator

from .state import DATA_KEY, LOCAL_DATA_PATH


REDIS_CHUNK_SIZE = 1000


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


def iter_record_rows(redis_client: Any | None = None, chunk_size: int = REDIS_CHUNK_SIZE) -> Iterator[str]:
    """Yield raw JSONL rows from Redis when available, otherwise from local mirror."""
    chunk_size = max(100, int(chunk_size or REDIS_CHUNK_SIZE))
    if redis_client is not None:
        try:
            total = int(redis_client.llen(DATA_KEY) or 0)
        except Exception:
            total = 0
        start = 0
        while start < total:
            end = min(total - 1, start + chunk_size - 1)
            rows = redis_client.lrange(DATA_KEY, start, end) or []
            for row in rows:
                if isinstance(row, bytes):
                    row = row.decode("utf-8", errors="replace")
                text = str(row).strip()
                if text:
                    yield text
            start = end + 1
        return

    if LOCAL_DATA_PATH.exists():
        with LOCAL_DATA_PATH.open("r", encoding="utf-8") as fh:
            for row in fh:
                text = row.strip()
                if text:
                    yield text


def iter_records(redis_client: Any | None = None, chunk_size: int = REDIS_CHUNK_SIZE) -> Iterator[dict[str, Any]]:
    for row in iter_record_rows(redis_client=redis_client, chunk_size=chunk_size):
        try:
            value = json.loads(row)
            if isinstance(value, dict):
                yield value
        except Exception:
            continue


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


def write_dataset_from_store(redis_client: Any | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    """Materialize Redis/local replay records into a JSONL file."""
    path = Path(output_path or LOCAL_DATA_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as fh:
        for row in iter_record_rows(redis_client=redis_client):
            fh.write(row.rstrip("\n") + "\n")
            count += 1
    return {"ok": True, "path": str(path), "records": count, "size_bytes": path.stat().st_size if path.exists() else 0}


def create_dataset_zip(redis_client: Any | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    """Write replay JSONL and compress it for Telegram/file download."""
    jsonl_result = write_dataset_from_store(redis_client=redis_client, output_path=output_path or LOCAL_DATA_PATH)
    jsonl_path = Path(jsonl_result["path"])
    zip_path = jsonl_path.with_suffix(jsonl_path.suffix + ".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(jsonl_path, arcname=jsonl_path.name)
    return {
        "ok": True,
        "jsonl_path": str(jsonl_path),
        "zip_path": str(zip_path),
        "records": int(jsonl_result.get("records") or 0),
        "jsonl_size_bytes": int(jsonl_result.get("size_bytes") or 0),
        "zip_size_bytes": zip_path.stat().st_size if zip_path.exists() else 0,
    }
