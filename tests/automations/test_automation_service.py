import json

import pytest

from nanobot.automations.script import ScriptAutomationService
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import (
    AutomationDefinition,
    AutomationsConfig,
    AutomationTriggerConfig,
)


@pytest.mark.asyncio
async def test_script_trigger_publishes_message_and_persists_state(tmp_path):
    script = tmp_path / "trigger.py"
    script.write_text(
        """
def trigger(ctx):
    count = ctx.state.get("count", 0) + 1
    ctx.state["count"] = count
    if count == 1:
        return {"id": "first", "title": "New PR", "url": "https://example.test/pr/1"}
    return False
""".strip(),
        encoding="utf-8",
    )
    bus = MessageBus()
    cfg = AutomationsConfig(
        enabled=True,
        jobs=[
            AutomationDefinition(
                id="github-pr",
                trigger=AutomationTriggerConfig(script=str(script)),
                message="Review {title}: {url}",
                channel="websocket",
                chat_id="chat-1",
            )
        ],
    )

    service = ScriptAutomationService(cfg, workspace_path=tmp_path, bus=bus)
    await service.poll_once()
    await service.poll_once()

    msg = await bus.consume_inbound()
    assert msg.channel == "websocket"
    assert msg.sender_id == "automation:github-pr"
    assert msg.chat_id == "chat-1"
    assert msg.content == "Review New PR: https://example.test/pr/1"
    assert msg.metadata["_automation"]["id"] == "github-pr"
    assert msg.metadata["_automation"]["trigger"] == {"kind": "script"}
    assert bus.inbound_size == 0
    assert json.loads((tmp_path / "automations" / "github-pr.json").read_text())["count"] == 2


@pytest.mark.asyncio
async def test_script_trigger_dedupes_payload_ids(tmp_path):
    script = tmp_path / "trigger.py"
    script.write_text(
        """
def trigger(ctx):
    return {"id": "same", "message": "run once"}
""".strip(),
        encoding="utf-8",
    )
    bus = MessageBus()
    cfg = AutomationsConfig(
        enabled=True,
        jobs=[
            AutomationDefinition(
                id="dedupe",
                trigger=AutomationTriggerConfig(script=str(script)),
                channel="websocket",
                chat_id="chat-1",
            )
        ],
    )

    service = ScriptAutomationService(cfg, workspace_path=tmp_path, bus=bus)
    await service.poll_once()
    await service.poll_once()

    assert (await bus.consume_inbound()).content == "run once"
    assert bus.inbound_size == 0


def test_automation_config_accepts_camel_case_trigger_fields():
    cfg = AutomationsConfig.model_validate(
        {
            "enabled": True,
            "intervalS": 3,
            "jobs": [
                {
                    "id": "local",
                    "trigger": {
                        "kind": "script",
                        "script": "triggers/local.py",
                        "timeoutS": 2,
                    },
                    "chatId": "chat-1",
                    "sessionKey": "websocket:chat-1",
                }
            ],
        }
    )

    assert cfg.interval_s == 3
    assert cfg.jobs[0].trigger.kind == "script"
    assert cfg.jobs[0].trigger.timeout_s == 2
    assert cfg.jobs[0].chat_id == "chat-1"
    assert cfg.jobs[0].session_key == "websocket:chat-1"
