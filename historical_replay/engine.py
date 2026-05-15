from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

from .state import append_log, clear_stop, get_status, set_status, stop_requested, utc_now_iso

_WORKER_LOCK = threading.Lock()
_WORKER_THREAD: threading.Thread | None = None


def _run_replay_worker(days: int, symbols_limit: int, timeframe: str, redis_client: Any | None = None) -> None:
    """Safe v1 replay worker shell.

    This first implementation intentionally does not touch live bot state.  It
    prepares the isolated replay run/state and leaves the heavy candle-by-candle
    implementation behind a separate package boundary.  The Telegram controls
    and Redis namespace are now stable, so the full downloader/replayer can be
    added without changing the live trading loop again.
    """
    run_id = f"replay_{int(days)}d_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    status = get_status(redis_client)
    status.update({
        "running": True,
        "state": "initializing",
        "run_id": run_id,
        "days": int(days),
        "symbols_limit": int(symbols_limit),
        "timeframe": timeframe,
        "started_at": utc_now_iso(),
        "completed_at": "",
        "progress_pct": 0.0,
        "symbols_total": int(symbols_limit),
        "symbols_done": 0,
        "records": 0,
        "normal": 0,
        "quality_candidates": 0,
        "execution_candidates": 0,
        "blocked_by_limits": 0,
        "message": "Replay control layer initialized. Full candle replay implementation is isolated in historical_replay/ and ready for the next patch.",
    })
    set_status(status, redis_client)
    append_log(f"Replay run initialized: {run_id}", redis_client)

    # Keep this worker short and safe in the live bot process. Heavy replay will
    # be enabled as a Railway Job/worker so it cannot block live scans.
    time.sleep(1.0)
    if stop_requested(redis_client):
        status.update({"running": False, "state": "stopped", "completed_at": utc_now_iso(), "message": "Replay stopped before heavy processing started."})
        set_status(status, redis_client)
        append_log("Replay stopped by user.", redis_client)
        return

    status.update({
        "running": False,
        "state": "ready_for_worker",
        "completed_at": utc_now_iso(),
        "progress_pct": 0.0,
        "message": "Historical Replay controls are installed. Next patch will add OKX historical candle replay runner under historical_replay/ without touching live execution.",
    })
    set_status(status, redis_client)
    append_log("Replay control layer ready; heavy runner not started in live process.", redis_client)


def start_replay(days: int = 45, symbols_limit: int = 200, timeframe: str = "15m", redis_client: Any | None = None) -> dict[str, Any]:
    global _WORKER_THREAD
    with _WORKER_LOCK:
        status = get_status(redis_client)
        if bool(status.get("running")):
            return {"ok": False, "message": "Historical replay is already running.", "status": status}
        clear_stop(redis_client)
        _WORKER_THREAD = threading.Thread(
            target=_run_replay_worker,
            kwargs={"days": days, "symbols_limit": symbols_limit, "timeframe": timeframe, "redis_client": redis_client},
            daemon=True,
            name="historical-replay-control",
        )
        _WORKER_THREAD.start()
        status.update({
            "running": True,
            "state": "starting",
            "days": int(days),
            "symbols_limit": int(symbols_limit),
            "timeframe": timeframe,
            "started_at": utc_now_iso(),
            "message": "Historical replay start requested.",
        })
        status = set_status(status, redis_client)
        return {"ok": True, "message": "Historical replay start requested.", "status": status}
