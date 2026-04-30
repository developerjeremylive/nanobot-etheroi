"""P2P tools for inter-agent task dispatch and coordination."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from nanobot.agent.tools.base import Tool
from nanobot.bus.events import OutboundMessage


class DispatchTaskTool(Tool):
    """Asynchronously dispatch a task to another agent. Non-blocking."""

    def __init__(self, shell: "P2PShell"):
        self._shell = shell

    @property
    def name(self) -> str:
        return "dispatch_task"

    @property
    def description(self) -> str:
        return (
            "Dispatch a task to a specific target agent. Returns immediately with a receipt. "
            "The target agent will process the task independently. Use poll_task_result later to check completion. "
            "Do NOT block waiting for results."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Target agent ID"},
                "task_description": {"type": "string", "description": "Clear description of the task"},
                "parent_task_id": {"type": "string", "description": "Parent task ID for ancestry tracking"},
                "deadline_seconds": {"type": "integer", "default": 300, "description": "Task deadline in seconds"},
                "allow_redelegation": {"type": "boolean", "default": True, "description": "Whether the target may re-delegate"},
            },
            "required": ["to", "task_description"],
        }

    async def execute(
        self,
        to: str,
        task_description: str,
        parent_task_id: str | None = None,
        deadline_seconds: int = 300,
        allow_redelegation: bool = True,
        **kwargs: Any,
    ) -> str:
        result = self._shell.dispatch(
            to=to,
            parent_task_id=parent_task_id,
            description=task_description,
            deadline_seconds=deadline_seconds,
            allow_redelegation=allow_redelegation,
        )
        if result.get("status") == "rejected":
            return f"Error: dispatch rejected — {result.get('reason', 'unknown')}"
        if result.get("status") == "circuit_open":
            failover = result.get("failover_to")
            return f"Error: circuit open for {to}. Failover candidate: {failover or 'none'}"
        return (
            f"Dispatched to {to}. Task ID: {result.get('task_id')}. "
            f"Depth: {result.get('depth', 0)}."
        )


class PollTaskResultTool(Tool):
    """Poll the status of a previously dispatched task."""

    def __init__(self, shell: "P2PShell"):
        self._shell = shell

    @property
    def name(self) -> str:
        return "poll_task_result"

    @property
    def description(self) -> str:
        return (
            "Check the current status of a task you previously dispatched. "
            "Returns completed, pending, timeout, failed, or not_found. "
            "Call this proactively — do not wait for automatic notifications."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Task ID returned by dispatch_task"},
            },
            "required": ["task_id"],
        }

    async def execute(self, task_id: str, **kwargs: Any) -> str:
        result = self._shell.poll(task_id)
        status = result.get("status")
        if status == "not_found":
            return f"Task {task_id} not found."
        if status == "pending":
            return f"Task {task_id} is pending (elapsed {result.get('elapsed', '?')}s)."
        if status == "timeout":
            return f"Task {task_id} timed out after {result.get('elapsed', '?')}s."
        if status in ("completed", "failed", "aborted"):
            from_agent = result.get("from", "unknown")
            content = result.get("result", "")
            preview = content[:500] + "..." if len(content) > 500 else content
            return f"Task {task_id} is {status} (from {from_agent}).\n\n{preview}"
        return f"Task {task_id} status: {status}"


class BroadcastTaskTool(Tool):
    """Broadcast subtasks to discover capable agents."""

    def __init__(self, shell: "P2PShell"):
        self._shell = shell

    @property
    def name(self) -> str:
        return "broadcast_task"

    @property
    def description(self) -> str:
        return (
            "Announce subtasks to the agent network to collect BIDs. "
            "Returns immediately. Use check_aggregation later to see which agents responded. "
            "Each subtask should include a capability hint for matching."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Your task identifier"},
                "subtasks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "subtask_id": {"type": "string"},
                            "description": {"type": "string"},
                            "capability": {"type": "string", "description": "Required capability, e.g. 'web_search'"},
                            "budget_seconds": {"type": "integer", "default": 300},
                        },
                        "required": ["subtask_id", "description", "capability"],
                    },
                },
                "aggregation_timeout": {"type": "integer", "default": 30, "description": "Seconds to wait for BIDs"},
            },
            "required": ["task_id", "subtasks"],
        }

    async def execute(
        self,
        task_id: str,
        subtasks: list[dict[str, Any]],
        aggregation_timeout: int = 30,
        **kwargs: Any,
    ) -> str:
        result = self._shell.broadcast(task_id, subtasks, aggregation_timeout)
        invited = result.get("invited", 0)
        return f"Broadcast opened for {task_id}. Invited {invited} agent(s). Use check_aggregation to collect BIDs."


class CheckAggregationTool(Tool):
    """Check the status of a broadcast aggregation window."""

    def __init__(self, shell: "P2PShell"):
        self._shell = shell

    @property
    def name(self) -> str:
        return "check_aggregation"

    @property
    def description(self) -> str:
        return (
            "Check whether a previously broadcast task has collected enough BIDs or timed out. "
            "Returns the list of responding agents and their bids, or a pending status with counts."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "task_id": {"type": "string", "description": "Task ID used in broadcast_task"},
            },
            "required": ["task_id"],
        }

    async def execute(self, task_id: str, **kwargs: Any) -> str:
        result = self._shell.check_aggregation(task_id)
        status = result.get("status")
        if status == "no_window":
            return f"No broadcast window found for {task_id}."
        if status == "pending":
            received = result.get("received", 0)
            expected = result.get("expected", "?")
            remaining = result.get("seconds_remaining", 0)
            return (
                f"Aggregation pending for {task_id}: "
                f"{received}/{expected} received, {remaining}s remaining."
            )
        if status == "closed":
            entries = result.get("entries", [])
            lines = [f"Aggregation closed for {task_id} ({result.get('reason', '')}):", ""]
            for e in entries:
                agent = e.get("from", "unknown")
                sub = e.get("subtask_id", "")
                lines.append(f"- {agent} bid for {sub}")
            return "\n".join(lines)
        return f"Unknown aggregation status for {task_id}: {status}"


class ReportUserTool(Tool):
    """Deliver a final answer to the user."""

    def __init__(
        self,
        send_callback: Callable[[OutboundMessage], Awaitable[None]] | None = None,
        default_channel: str = "",
        default_chat_id: str = "",
    ):
        self._send_callback = send_callback
        self._default_channel = default_channel
        self._default_chat_id = default_chat_id

    @property
    def name(self) -> str:
        return "report_user"

    @property
    def description(self) -> str:
        return (
            "Report the final answer to the user. Use this when you have gathered enough results. "
            "Status 'partial' means some subtasks are incomplete — list them in pending_items."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "final_answer": {"type": "string", "description": "Complete answer for the user"},
                "status": {"type": "string", "enum": ["success", "partial", "failed"]},
                "pending_items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Incomplete items when status is partial",
                },
                "task_summary": {"type": "string", "description": "Optional brief summary"},
            },
            "required": ["final_answer", "status"],
        }

    async def execute(
        self,
        final_answer: str,
        status: str,
        pending_items: list[str] | None = None,
        task_summary: str = "",
        **kwargs: Any,
    ) -> str:
        if not self._send_callback:
            return "Error: report_user not configured (no send callback)"

        parts = [final_answer]
        if pending_items:
            parts.append(f"\n\nPending items:\n" + "\n".join(f"- {i}" for i in pending_items))
        if task_summary:
            parts.append(f"\n\nSummary: {task_summary}")

        content = "\n".join(parts)
        msg = OutboundMessage(
            channel=self._default_channel,
            chat_id=self._default_chat_id,
            content=content,
        )
        await self._send_callback(msg)
        return f"Reported to user (status={status})."


class FinalizeTaskTool(Tool):
    """Force-finalize a task and close its sessions."""

    def __init__(self, shell: "P2PShell", session_manager: "SessionManager | None" = None):
        self._shell = shell
        self._session_manager = session_manager

    @property
    def name(self) -> str:
        return "finalize_task"

    @property
    def description(self) -> str:
        return (
            "Terminate a task and all its subtasks. Use when the user says 'stop', "
            "or when a task is fundamentally blocked. outcome can be completed, failed, or aborted."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "outcome": {"type": "string", "enum": ["completed", "failed", "aborted"]},
                "reason": {"type": "string", "description": "Why the task was finalized"},
            },
            "required": ["task_id", "outcome"],
        }

    async def execute(
        self,
        task_id: str,
        outcome: str,
        reason: str = "",
        **kwargs: Any,
    ) -> str:
        self._shell.finalize(task_id, outcome, reason)
        if self._session_manager:
            self._session_manager.finalize_task_session(task_id)
        return f"Task {task_id} finalized with outcome={outcome}."
