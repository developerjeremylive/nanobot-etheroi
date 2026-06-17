# Automation Triggers

Use automation triggers when a task should run because a condition became true,
not only because a clock fired.

An automation owns a trigger. Existing scheduled automations are cron-triggered
through the `cron` tool and WebUI Automations view. This page covers the script
trigger kind: a local Python script that nanobot polls while `nanobot gateway`
is running.

## Configure

Merge an `automations` block into your config:

```json
{
  "automations": {
    "enabled": true,
    "intervalS": 10,
    "jobs": [
      {
        "id": "review-new-pr",
        "trigger": {
          "kind": "script",
          "script": "triggers/github_pr.py",
          "timeoutS": 5
        },
        "message": "Review this PR:\n{title}\n{url}",
        "channel": "websocket",
        "chatId": "default"
      }
    ]
  }
}
```

Relative script paths are resolved from the active workspace. With the example
above, create:

```text
<workspace>/triggers/github_pr.py
```

Then restart the gateway:

```bash
nanobot gateway
```

## Script Interface

Each script must define `trigger(ctx)`:

```python
def trigger(ctx):
    latest = get_latest_pr()
    if ctx.state.get("last_pr") == latest["id"]:
        return False

    ctx.state["last_pr"] = latest["id"]
    return {
        "id": latest["id"],
        "title": latest["title"],
        "url": latest["url"],
    }
```

Return values:

| Return value | Effect |
| --- | --- |
| `False` or `None` | Do nothing |
| `True` | Trigger once with an empty payload |
| `dict` | Trigger once and use the dict as the message payload |
| `list[dict]` | Trigger once per payload |

`ctx.state` is a persistent dict stored at:

```text
<workspace>/automations/<automation-id>.json
```

If a payload contains `id`, nanobot remembers recent IDs and skips duplicates.

## Message Rendering

`message` is formatted with Python `str.format` using the payload:

```json
"message": "Review {title}: {url}"
```

If `message` is empty, the payload can provide a `message` field:

```python
return {"id": "build-123", "message": "Build 123 failed"}
```

## Safety

Scripts are trusted local code. They run in the nanobot gateway process, so keep
them short, idempotent, and defensive. Use `timeoutS` to keep a slow condition
check from blocking future polls.
