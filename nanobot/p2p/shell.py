"""P2P shell: filesystem-backed inter-agent coordination.

All state is stored in the mailbox filesystem; this class is stateless.
Restarting the gateway restores all task state by scanning files.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Literal

from loguru import logger


class P2PShell:
    """Stateless P2P coordination shell backed by the mailbox filesystem."""

    def __init__(self, agent_id: str, mailboxes_root: str):
        self.agent_id = agent_id
        self.root = Path(mailboxes_root).expanduser()
        self.inbox = self.root / agent_id / "inbox"
        self.processed = self.root / agent_id / "processed"
        self.links_dir = self.root / "_links"
        self.windows_dir = self.root / "_windows"

        for d in (self.inbox, self.processed, self.links_dir, self.windows_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover(self, capability: str, top_k: int = 3) -> list[dict[str, Any]]:
        """Read _registry.json and return candidates matching capability."""
        registry = self._load_json(self.root / "_registry.json", default={})
        candidates: list[dict[str, Any]] = []
        for aid, info in registry.items():
            if aid == self.agent_id:
                continue
            caps = info.get("capabilities", [])
            if capability.lower() in " ".join(caps).lower():
                candidates.append({"agent_id": aid, **info})
        # Sort: idle first, then by current task load
        candidates.sort(key=lambda x: (x.get("status") != "idle", x.get("current_tasks", 0)))
        return candidates[:top_k]

    def heartbeat(self, description: str, capabilities: list[str]) -> None:
        """Write self state into the shared _registry.json."""
        registry = self._load_json(self.root / "_registry.json", default={})
        registry[self.agent_id] = {
            "description": description,
            "capabilities": capabilities,
            "status": "idle",
            "last_heartbeat": int(time.time()),
            "endpoint": "",
        }
        self._atomic_write(self.root / "_registry.json", registry)

    # ------------------------------------------------------------------
    # Task dispatch
    # ------------------------------------------------------------------

    def dispatch(
        self,
        to: str,
        parent_task_id: str | None,
        description: str,
        deadline_seconds: int = 300,
        allow_redelegation: bool = True,
    ) -> dict[str, Any]:
        """Write a task into the target agent's inbox and return a receipt."""
        task_id = (
            f"{parent_task_id}.{int(time.time())}"
            if parent_task_id
            else f"root_{int(time.time())}"
        )

        depth = self._get_depth(parent_task_id) if parent_task_id else 0
        if depth >= 3:
            return {"status": "rejected", "reason": "max_depth_exceeded"}

        if parent_task_id and self._is_ancestor(to, parent_task_id):
            return {"status": "rejected", "reason": "ancestry_loop"}

        if not self._circuit_allow(to):
            failover = self._find_failover(to)
            return {"status": "circuit_open", "failover_to": failover}

        target_inbox = self.root / to / "inbox"
        target_inbox.mkdir(parents=True, exist_ok=True)
        if list(target_inbox.glob(f"task_{task_id}_from_{self.agent_id}_*.json")):
            return {"status": "dispatched", "task_id": task_id, "note": "cached"}

        ancestry = (
            (self._get_ancestry(parent_task_id) + [self.agent_id])
            if parent_task_id
            else [self.agent_id]
        )

        msg: dict[str, Any] = {
            "version": "p2p/v1",
            "type": "task_dispatch",
            "from": self.agent_id,
            "to": to,
            "task_id": task_id,
            "ancestry": ancestry,
            "depth": depth + 1,
            "payload": {
                "description": description,
                "allow_redelegation": allow_redelegation,
            },
            "deadline": int(time.time()) + deadline_seconds,
            "timestamp": int(time.time()),
        }

        path = target_inbox / f"task_{task_id}_from_{self.agent_id}_{os.urandom(4).hex()}.json"
        self._atomic_write(path, msg)
        logger.info("P2P dispatch: {} -> {} (task_id={})", self.agent_id, to, task_id)
        return {"status": "dispatched", "task_id": task_id, "depth": depth + 1}

    def poll(self, task_id: str) -> dict[str, Any]:
        """Scan inbox/processed and return task status."""
        # Check processed results first
        results = list(self.processed.glob(f"result_{task_id}_from_*.json"))
        if results:
            data = self._load_json(results[0])
            payload = data.get("payload", {})
            return {
                "status": payload.get("outcome", "completed"),
                "result": payload.get("content", ""),
                "from": data["from"],
            }

        # Check inbox for results (not yet moved to processed)
        inbox_results = list(self.inbox.glob(f"result_{task_id}_from_*.json"))
        if inbox_results:
            data = self._load_json(inbox_results[0])
            payload = data.get("payload", {})
            return {
                "status": payload.get("outcome", "completed"),
                "result": payload.get("content", ""),
                "from": data["from"],
            }

        # Check inbox for pending task dispatches
        pending = list(self.inbox.glob(f"task_{task_id}_from_*.json"))
        if pending:
            data = self._load_json(pending[0])
            deadline = data.get("deadline", 0)
            elapsed = int(time.time() - data["timestamp"])
            if time.time() > deadline:
                return {"status": "timeout", "elapsed": elapsed}
            return {"status": "pending", "elapsed": elapsed}

        return {"status": "not_found"}

    # ------------------------------------------------------------------
    # Aggregation (broadcast + check)
    # ------------------------------------------------------------------

    def broadcast(
        self,
        task_id: str,
        subtasks: list[dict[str, Any]],
        aggregation_timeout: int = 30,
    ) -> dict[str, Any]:
        """Write bid requests to candidate agents and create a window descriptor."""
        targets: list[tuple[str, str]] = []  # (subtask_id, agent_id)
        for sub in subtasks:
            caps = sub.get("capability", "")
            found = self.discover(caps, top_k=3)
            targets.extend([(sub["subtask_id"], a["agent_id"]) for a in found])

        for subtask_id, target in targets:
            msg: dict[str, Any] = {
                "version": "p2p/v1",
                "type": "bid_request",
                "from": self.agent_id,
                "to": target,
                "task_id": task_id,
                "subtask_id": subtask_id,
                "payload": sub,
                "deadline": int(time.time()) + aggregation_timeout,
                "timestamp": int(time.time()),
            }
            target_inbox = self.root / target / "inbox"
            target_inbox.mkdir(parents=True, exist_ok=True)
            path = target_inbox / f"bid_{task_id}_{subtask_id}_from_{self.agent_id}.json"
            self._atomic_write(path, msg)

        window: dict[str, Any] = {
            "task_id": task_id,
            "mode": "bid",
            "expected": len(targets),
            "deadline": int(time.time()) + aggregation_timeout,
            "created_at": int(time.time()),
        }
        self._atomic_write(self.windows_dir / f"{task_id}.json", window)
        logger.info(
            "P2P broadcast: {} invited {} agents for task_id={}",
            self.agent_id,
            len(targets),
            task_id,
        )
        return {"status": "bidding_opened", "task_id": task_id, "invited": len(targets)}

    def check_aggregation(self, task_id: str) -> dict[str, Any]:
        """Lazily check aggregation status by scanning files."""
        window_path = self.windows_dir / f"{task_id}.json"
        if not window_path.exists():
            return {"status": "no_window"}

        window = self._load_json(window_path)
        mode = window.get("mode", "bid")
        deadline = window.get("deadline", 0)

        pattern = f"{mode}_{task_id}_*_from_*.json"
        entries: list[dict[str, Any]] = []
        for f in self.inbox.glob(pattern):
            data = self._load_json(f)
            entries.append(
                {
                    "from": data.get("from", ""),
                    "subtask_id": data.get("subtask_id", ""),
                    "payload": data.get("payload", {}),
                }
            )

        is_timeout = time.time() > deadline
        is_full = window.get("expected") and len(entries) >= window["expected"]

        if is_timeout or is_full:
            self._atomic_write(
                self.processed / f"window_{task_id}.json",
                {**window, "closed_at": int(time.time()), "received": len(entries)},
            )
            window_path.unlink(missing_ok=True)
            return {
                "status": "closed",
                "mode": mode,
                "entries": entries,
                "reason": "timeout" if is_timeout else "full",
            }

        return {
            "status": "pending",
            "received": len(entries),
            "expected": window.get("expected"),
            "seconds_remaining": max(0, deadline - int(time.time())),
        }

    # ------------------------------------------------------------------
    # Result reporting
    # ------------------------------------------------------------------

    def report_result(
        self,
        to: str,
        task_id: str,
        outcome: Literal["completed", "failed", "aborted"],
        content: str,
        callback: dict[str, Any] | None = None,
    ) -> None:
        """Worker calls this to write a result into the manager's inbox."""
        msg: dict[str, Any] = {
            "version": "p2p/v1",
            "type": "result",
            "from": self.agent_id,
            "to": to,
            "task_id": task_id,
            "payload": {"outcome": outcome, "content": content},
            "timestamp": int(time.time()),
        }
        if callback:
            msg["callback"] = callback
        target_inbox = self.root / to / "inbox"
        target_inbox.mkdir(parents=True, exist_ok=True)
        path = target_inbox / f"result_{task_id}_from_{self.agent_id}_{os.urandom(4).hex()}.json"
        self._atomic_write(path, msg)
        logger.info("P2P result: {} -> {} (task_id={}, outcome={})", self.agent_id, to, task_id, outcome)

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------

    def finalize(self, task_id: str, outcome: str, reason: str = "") -> None:
        """Move all task files from inbox to processed and mark outcome."""
        for src in list(self.inbox.glob(f"*{task_id}*")):
            data = self._load_json(src)
            data.setdefault("payload", {})
            data["payload"]["outcome"] = outcome
            data["payload"]["reason"] = reason
            dst = self.processed / src.name
            self._atomic_write(dst, data)
            src.unlink(missing_ok=True)
        logger.info("P2P finalize: task_id={} outcome={}", task_id, outcome)

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def _circuit_allow(self, to: str) -> bool:
        link = self._load_json(
            self.links_dir / f"{to}.json",
            default={"failures": 0, "last_failure": 0, "open": False},
        )
        if not link.get("open"):
            return True
        backoff = 300 * (2 ** max(0, link.get("failures", 0) - 3))
        if time.time() - link.get("last_failure", 0) > backoff:
            link["open"] = False
            self._atomic_write(self.links_dir / f"{to}.json", link)
            return True
        return False

    def record_failure(self, to: str) -> None:
        link = self._load_json(
            self.links_dir / f"{to}.json",
            default={"failures": 0, "last_failure": 0, "open": False},
        )
        link["failures"] = link.get("failures", 0) + 1
        link["last_failure"] = int(time.time())
        if link["failures"] >= 3:
            link["open"] = True
        self._atomic_write(self.links_dir / f"{to}.json", link)

    def record_success(self, to: str) -> None:
        link = self._load_json(
            self.links_dir / f"{to}.json",
            default={"failures": 0, "last_failure": 0, "open": False},
        )
        link["failures"] = 0
        link["open"] = False
        self._atomic_write(self.links_dir / f"{to}.json", link)

    # ------------------------------------------------------------------
    # Inbox scanning (for HeartbeatService)
    # ------------------------------------------------------------------

    def scan_inbox(self) -> list[dict[str, Any]]:
        """Return all task_dispatch messages currently in inbox."""
        messages: list[dict[str, Any]] = []
        for f in sorted(self.inbox.glob("task_*_from_*.json"), key=lambda p: p.stat().st_mtime):
            data = self._load_json(f)
            # Skip expired tasks
            if time.time() > data.get("deadline", 0):
                continue
            data["_filename"] = f.name
            messages.append(data)
        return messages

    def scan_new_inbox(self, since: float | None = None) -> list[dict[str, Any]]:
        """Return inbox messages newer than the given timestamp."""
        messages: list[dict[str, Any]] = []
        for f in self.inbox.glob("task_*_from_*.json"):
            mtime = f.stat().st_mtime
            if since is not None and mtime <= since:
                continue
            data = self._load_json(f)
            if time.time() > data.get("deadline", 0):
                continue
            data["_filename"] = f.name
            data["_mtime"] = mtime
            messages.append(data)
        return sorted(messages, key=lambda x: x.get("_mtime", 0))

    def mark_processed(self, filename: str) -> None:
        """Move a single inbox file to processed."""
        src = self.inbox / filename
        if not src.exists():
            return
        dst = self.processed / filename
        try:
            import shutil
            shutil.move(str(src), str(dst))
        except Exception:
            logger.warning("Failed to mark processed: {}", filename)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_json(self, path: Path, default: Any | None = None) -> Any:
        if not path.exists():
            return default if default is not None else {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _atomic_write(self, path: Path, data: dict[str, Any]) -> None:
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.rename(path)

    def _get_depth(self, task_id: str) -> int:
        return task_id.count(".")

    def _is_ancestor(self, agent_id: str, parent_task_id: str) -> bool:
        for f in list(self.processed.glob(f"*{parent_task_id}*")) + list(
            self.inbox.glob(f"*{parent_task_id}*")
        ):
            data = self._load_json(f)
            if agent_id in data.get("ancestry", []):
                return True
        return False

    def _get_ancestry(self, task_id: str) -> list[str]:
        for f in list(self.processed.glob(f"*{task_id}*")) + list(
            self.inbox.glob(f"*{task_id}*")
        ):
            data = self._load_json(f)
            return data.get("ancestry", [])
        return []

    def _find_failover(self, to: str) -> str | None:
        registry = self._load_json(self.root / "_registry.json", default={})
        target_caps = registry.get(to, {}).get("capabilities", [])
        for aid, info in registry.items():
            if aid == to:
                continue
            if any(c in info.get("capabilities", []) for c in target_caps):
                return aid
        return None
