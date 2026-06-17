"""Script-triggered automation runner."""

from __future__ import annotations

import asyncio
import importlib.util
import json
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import AutomationDefinition, AutomationsConfig


@dataclass
class AutomationContext:
    state: dict[str, Any]
    log: Any


class ScriptAutomationService:
    """Poll configured script triggers and publish matching agent turns."""

    def __init__(
        self,
        config: AutomationsConfig,
        *,
        workspace_path: Path,
        bus: MessageBus,
    ) -> None:
        self.config = config
        self.workspace_path = workspace_path
        self.bus = bus
        self._running = False
        self._inflight: dict[str, asyncio.Task[Any]] = {}

    @property
    def enabled(self) -> bool:
        return self.config.enabled and any(job.enabled for job in self.config.jobs)

    async def run(self) -> None:
        self._running = True
        while self._running:
            await self.poll_once()
            await asyncio.sleep(self.config.interval_s)

    def stop(self) -> None:
        self._running = False

    async def poll_once(self) -> None:
        if not self.enabled:
            return
        for job in self.config.jobs:
            if job.enabled:
                await self._poll_script(job)

    async def _poll_script(self, job: AutomationDefinition) -> None:
        task = self._inflight.get(job.id)
        if task is not None and not task.done():
            return
        if task is not None:
            with suppress(Exception):
                task.result()
        self._inflight.pop(job.id, None)

        state_path = self.workspace_path / "automations" / f"{_safe_name(job.id)}.json"
        state = _load_state(state_path)
        ctx = AutomationContext(
            state=state,
            log=logger.bind(automation=job.id),
        )
        script_path = _resolve_script_path(self.workspace_path, job.trigger.script)
        task = asyncio.create_task(asyncio.to_thread(_call_trigger, script_path, ctx))
        self._inflight[job.id] = task

        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=job.trigger.timeout_s)
        except TimeoutError:
            logger.warning(
                "Automation '{}' trigger timed out after {}s",
                job.id,
                job.trigger.timeout_s,
            )
            _save_state(state_path, state)
            return
        except Exception as exc:
            logger.warning("Automation '{}' trigger failed: {}", job.id, exc)
            _save_state(state_path, state)
            return
        finally:
            if task.done():
                self._inflight.pop(job.id, None)

        try:
            payloads = _payloads(result)
        except RuntimeError as exc:
            logger.warning("Automation '{}' trigger returned invalid result: {}", job.id, exc)
            _save_state(state_path, state)
            return

        for payload in payloads:
            try:
                content = _render_message(job.message, payload)
            except (KeyError, ValueError, IndexError) as exc:
                logger.warning("Automation '{}' message template failed: {}", job.id, exc)
                continue
            if not content.strip():
                logger.warning("Automation '{}' produced an empty message", job.id)
                continue
            if _already_seen(state, payload):
                continue
            await self.bus.publish_inbound(
                InboundMessage(
                    channel=job.channel,
                    sender_id=f"automation:{job.id}",
                    chat_id=job.chat_id,
                    content=content,
                    metadata={
                        "_automation": {
                            "id": job.id,
                            "trigger": {"kind": job.trigger.kind},
                            "payload": payload,
                        }
                    },
                    session_key_override=job.session_key,
                )
            )
        _save_state(state_path, state)


def _call_trigger(script_path: Path, ctx: AutomationContext) -> Any:
    spec = importlib.util.spec_from_file_location(
        f"nanobot_automation_{_safe_name(script_path.stem)}",
        script_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load trigger script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    trigger = getattr(module, "trigger", None)
    if not callable(trigger):
        raise RuntimeError(f"{script_path} must define trigger(ctx)")
    return trigger(ctx)


def _payloads(result: Any) -> list[dict[str, Any]]:
    if result is True:
        return [{}]
    if not result:
        return []
    if isinstance(result, dict):
        return [result]
    if isinstance(result, list) and all(isinstance(item, dict) for item in result):
        return result
    raise RuntimeError("trigger(ctx) must return bool, dict, or list[dict]")


def _already_seen(state: dict[str, Any], payload: dict[str, Any]) -> bool:
    value = payload.get("id")
    if not value:
        return False
    seen = state.setdefault("_seen_ids", [])
    if value in seen:
        return True
    seen.append(value)
    del seen[:-1000]
    return False


def _render_message(template: str, payload: dict[str, Any]) -> str:
    if template:
        return template.format(**payload)
    message = payload.get("message")
    return str(message) if message is not None else ""


def _resolve_script_path(workspace_path: Path, script: str) -> Path:
    path = Path(script).expanduser()
    return path if path.is_absolute() else workspace_path / path


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Automation state file '{}' is unreadable; starting empty", path)
        return {}
    return data if isinstance(data, dict) else {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in value) or "automation"
