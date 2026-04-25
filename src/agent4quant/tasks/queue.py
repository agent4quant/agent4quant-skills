from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import threading
import uuid


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LocalTaskQueue:
    def __init__(self, root: str = "output/api-tasks", max_workers: int = 2) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="a4q-task")
        self._lock = threading.Lock()

    def _path(self, task_id: str) -> Path:
        return self.root / f"{task_id}.json"

    def _write(self, record: dict) -> dict:
        path = self._path(str(record["task_id"]))
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(path)
        return record

    def _read(self, task_id: str) -> dict:
        path = self._path(task_id)
        if not path.exists():
            raise FileNotFoundError(f"Task not found: {task_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _update(self, task_id: str, **changes) -> dict:
        with self._lock:
            record = self._read(task_id)
            record.update(changes)
            return self._write(record)

    def submit(self, *, task_type: str, payload: dict, runner) -> dict:
        task_id = uuid.uuid4().hex
        record = {
            "task_id": task_id,
            "task_type": task_type,
            "status": "queued",
            "created_at": _utc_now(),
            "started_at": None,
            "completed_at": None,
            "payload": payload,
            "result": None,
            "error": None,
        }
        with self._lock:
            self._write(record)
        self._executor.submit(self._run, task_id, runner)
        return record

    def _run(self, task_id: str, runner) -> None:
        self._update(task_id, status="running", started_at=_utc_now())
        try:
            result = runner()
        except Exception as exc:  # pragma: no cover - failure path depends on submitted task
            self._update(
                task_id,
                status="failed",
                completed_at=_utc_now(),
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                },
            )
            return
        self._update(
            task_id,
            status="completed",
            completed_at=_utc_now(),
            result=result,
        )

    def get(self, task_id: str) -> dict:
        return self._read(task_id)

    def list(self, limit: int = 20, status: str | None = None, task_type: str | None = None) -> list[dict]:
        items = []
        for path in self.root.glob("*.json"):
            record = json.loads(path.read_text(encoding="utf-8"))
            if status and record.get("status") != status:
                continue
            if task_type and record.get("task_type") != task_type:
                continue
            items.append(record)

        items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return items[:limit]

    def summary(self) -> dict:
        items = self.list(limit=100000)
        by_status: dict[str, int] = {}
        by_task_type: dict[str, int] = {}
        for item in items:
            status = str(item.get("status") or "unknown")
            task_type = str(item.get("task_type") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            by_task_type[task_type] = by_task_type.get(task_type, 0) + 1

        return {
            "total": len(items),
            "by_status": dict(sorted(by_status.items())),
            "by_task_type": dict(sorted(by_task_type.items())),
        }
