# Agent Reliability Lessons from Evaluation Trajectories

This note collects reliability improvements observed while reviewing long-running
agent evaluation trajectories. The examples came from Terminal-Bench style tasks,
but the recommendations are not benchmark-specific. They apply to normal user
workflows where nanobot builds software, runs tests, inspects logs, handles
provider failures, and decides when work is complete.

Use this document as a design brief when discussing future agent-runtime changes.

## Current Patch Series

The current reliability patch series focuses on three general runtime problems:

- provider error recovery for transient backend failures;
- a stronger completion gate before `complete_goal`;
- structured tool-result feedback after failed commands or verification checks.

Those changes are useful, but trajectory review shows more product-level work is
needed. The biggest opportunities are below.

## 1. Structured Long-Output Retrieval

Current behavior persists oversized tool output under `.nanobot/tool-results` and
returns a reference like:

```text
[tool output persisted]
Full output saved to: ...
Preview:
...
(Read the saved file if you need the full output.)
```

This was designed for durability and context control: users can run very large
build, test, or log commands without forcing the whole output into every future
model call. That product goal is sound.

The problem is the model-facing contract. When the assistant sees a workspace
file path plus an instruction to read the full output, it often spends extra
turns paging through its own internal tool-output files. In long tasks this can
consume the same budget the agent needs for solving the actual problem.

Prefer this contract instead:

```text
exit_code: 1
duration_s: 83.4
truncated: true
stdout_head: ...
stdout_tail: ...
stderr_tail: ...
error_summary:
- failed assertion ...
- missing file ...
tool_output_id: call_abc123
```

Then expose a dedicated retrieval tool:

```text
get_tool_output(
  id,
  mode="tail" | "head" | "grep" | "range",
  pattern?,
  offset?,
  limit?
)
```

The full output can still be stored for audit and recovery, but the default path
should be structured summaries and targeted retrieval, not `read_file` pagination
over internal artifacts.

Recommended properties:

- Store full output outside the task workspace when possible.
- Return head, tail, exit code, elapsed time, and a short failure summary by default.
- Make grep/tail/range retrieval explicit and cheap.
- Avoid user-facing wording that nudges the model to read the whole output.
- Keep `read_file` from becoming the primary recovery mechanism for tool output.

## 2. Runtime State Isolation

Runtime state should not share the same namespace as user/task artifacts unless
the user explicitly asks for that.

For normal project work this reduces surprise. For automated runs it prevents
agent bookkeeping from polluting the workspace or colliding with task files.

Recommended defaults:

- Add or document a `stateDir` / `--state-dir` path for sessions, checkpoints,
  tool-output blobs, temporary runtime files, and internal recovery state.
- Keep generated task artifacts in the workspace, but keep agent runtime state in
  the state directory.
- Make atomic writes robust when state directories are cleaned or moved.
- Keep workspace-visible files limited to files the user or task asked nanobot to
  create.

This is not only about benchmarks. It also makes nanobot easier to embed in CI,
SDK flows, containers, and project repositories where users care about clean
working trees.

## 3. Completion Should Mean Verified Completion

`complete_goal` should not be only bookkeeping. For non-trivial work, completion
should force the assistant to summarize how it knows the work is done.

The tool schema can stay lightweight, but the model-facing contract should require:

- `verification_summary`;
- `commands_run`;
- `artifacts_created`;
- `remaining_failures`;
- an honest note when verification was impossible.

This reduces a common failure mode: the agent creates a plausible artifact, sees
one partial success, and ends without checking the verifier-like condition the
user actually cares about.

This is a product feature, not an evaluation trick. Users want the same thing
when they ask for a bug fix, migration, data artifact, or local setup.

## 4. Tool Result Feedback Should Be Actionable

Raw terminal output is often too noisy. The agent needs a compact explanation of
what changed after a command.

Command and verification tools should return structured signals such as:

- `exit_code`;
- `timed_out`;
- `duration_s`;
- failed test names;
- assertion messages;
- missing files;
- likely transient provider/backend errors;
- a concise next-action hint.

The goal is not to hide raw output. The goal is to make the first follow-up turn
useful without requiring the model to mine thousands of log lines.

## 5. Provider and Infrastructure Recovery

Provider failures such as `server_error`, `service_unavailable`,
`server_is_overloaded`, `rate_limit`, and usage-limit style errors are different
from task failure.

Recommended behavior:

- Classify transient provider and infrastructure errors separately from agent
  solution errors.
- Retry transient failures with bounded backoff.
- Preserve the task state and current conversation when retrying.
- Surface provider exhaustion clearly to the user.
- Record retry counts and final error classes in run metadata.

This helps chat users, SDK callers, and evaluation harnesses for the same reason:
provider instability should not silently look like bad agent reasoning.

## 6. Focused Task Profile

Nanobot is designed for long-lived personal and team workflows, so the default
runtime includes durable memory, sessions, heartbeat, cron, rich tools, and
workspace state. That is useful in normal operation.

Some tasks are different: a user wants one bounded solve in a clean workspace.
For those cases, nanobot should offer a focused runtime profile.

The profile should be generic, not benchmark-specific:

- ephemeral memory by default;
- no heartbeat or cron unless requested;
- minimal tool surface;
- state stored outside the workspace;
- concise system prompt tuned for direct artifact creation and verification;
- bounded command defaults;
- no hidden benchmark deadlines or task-specific hints.

This profile would be useful for CI, coding interviews, containerized jobs,
one-shot SDK calls, and evaluation suites.

## 7. Tool Surface Should Be Explicit

Comparing agent outcomes without recording available tools is misleading. A run
with vision, browser, or web search is not the same method as a terminal-only run.

Nanobot should make tool surface easy to inspect and record:

- enabled tools;
- web search/fetch status;
- image or vision handling status;
- browser/computer-use status;
- shell sandbox and network policy;
- max tool iterations and model context settings.

This is not just for leaderboards. It helps users reproduce runs and understand
why an agent succeeded or failed.

## 8. Multimodal Inputs Need First-Class Handling

When users provide images, screenshots, videos, PDFs, plots, or GUI captures,
the agent should not have to improvise a fragile workflow every time.

Recommended direction:

- Add a clear read/analyze path for local images when the configured model or
  tool stack supports vision.
- Provide fallback helpers for OCR, frame extraction, image diffs, and metadata
  inspection when vision is unavailable.
- Return explicit capability errors when no vision backend is configured.
- Record whether the final answer relied on vision, OCR, or shell-only analysis.

This improves ordinary desktop, WebUI, and support workflows, not only visual
benchmark tasks.

## 9. Process Management for Long Commands

Long commands should be managed as processes with stable status, not as repeated
large one-shot outputs.

Recommended behavior:

- Encourage `exec(..., yield_time_ms=...)` for commands expected to run long.
- Return `session_id`, status, elapsed time, recent output, and whether stdin is
  open.
- Provide polling with tail/summary, not full log replay.
- Make timeouts and killed processes explicit.
- Keep full logs available through structured retrieval.

This aligns the shell tool with how users debug builds and services in real
projects.

## 10. Evidence to Keep Watching

Trajectory review repeatedly showed these patterns:

- extra turns spent reading `.nanobot/tool-results` instead of acting on a
  structured log summary;
- timeouts after long exploratory loops;
- completion after partial verification;
- workspace pollution from runtime state;
- failures that should be classified as provider or infrastructure issues;
- visual tasks solved differently depending on whether a vision/browser tool was
  available.

These are not isolated benchmark quirks. They are runtime ergonomics issues that
surface whenever an agent does sustained technical work.

## Non-Goals

These changes should not:

- add benchmark-specific skills or hidden task knowledge;
- expose hidden deadlines or scorer internals to the model;
- mark timeouts as success;
- silently discard full logs without an audit path;
- make normal interactive nanobot sessions less durable.

The goal is a cleaner agent-runtime contract: compact by default, recoverable on
demand, explicit about tools, and honest about verification.

