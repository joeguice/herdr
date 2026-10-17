# Prompt queue

A Herdr plugin that keeps a per-conversation queue of prompts you want to send
later, once the conversation reaches the right stage. Open the queue for the
focused agent pane, then add, edit, reorder, or delete entries. You can also
insert one into the agent's prompt. An inserted prompt is removed from the
queue when you submit it.

It uses only the public plugin API and needs Python 3 (standard library only) on
Linux or macOS.

## Install

```bash
herdr plugin link /path/to/herdr/plugins/prompt-queue
```

Bind a key to open the queue for the focused pane. Custom command keys belong
to the Herdr server's `~/.config/herdr/config.toml`:

```toml
[[keys.command]]
key = ["ctrl+q", "prefix+shift+q"]
type = "plugin_action"
command = "joeguice.prompt-queue.open"
description = "prompt queue"
```

`ctrl+q` is a direct chord, so programs inside panes no longer receive it; drop
it from the list if you need it there. Pressing it again inside the popup
closes the queue. A native client attached with `herdr --remote` only binds
custom commands when it uses `--remote-keybindings server`. With the default
`local` keybindings, Herdr does not bind them at all.

To show the count in the Agent sidebar, add the `$queue` token to a row. It
reads like `3 queued` and disappears when the queue is empty:

```toml
[ui.sidebar.agents]
rows = [["state_icon", "state_text", "machine", "workspace", "tab"], ["agent", "$queue"]]
```

Run `herdr server reload-config` after editing.

## Using the queue

| Key | Action |
| --- | --- |
| `enter` / `i` / double-click | Insert the selected prompt into the agent's input without submitting it, then close |
| `s` | Submit the selected prompt now and remove it |
| `a` | Add a prompt after the selection |
| `e` | Edit the selected prompt |
| `d` / `delete` | Delete the selected prompt |
| `J` / `K` | Move the selected prompt down / up |
| `u` | Undo: restore the most recently removed prompt (deleted, sent, or submitted) |
| `p` | Unmark an inserted prompt you decided not to submit |
| `o` | Other conversations' queues; `enter` moves one into this conversation, `x` twice deletes it |
| `j` / `k`, arrows, wheel, click | Select |
| `q` / `esc` / `ctrl+q` | Close |

Adding and editing happen in the popup. `esc` saves and returns to the queue;
`ctrl+c` discards the edit. `enter` starts a new line, and the arrow keys,
`home`/`end` (or `ctrl+a`/`ctrl+e`), `backspace`, and `delete` move and edit
as usual. Long lines wrap to the popup width.

The inserted text is a normal bracketed paste, so you can edit it in the agent's
input before pressing Enter. The entry shows ⏳ until the agent starts working.
Herdr's `working` status is the submit signal. If the agent starts working for
another reason while an inserted prompt is still unsent, the entry is removed
anyway; press `u` to bring it back.

### When a conversation ends with prompts queued

Herdr plugins can't intercept an agent's own `/quit` or `/exit`, but quitting
loses nothing: the queue stays with the conversation's session. When a
conversation leaves its pane with prompts still queued, the plugin shows a
Herdr toast (with the needs-attention sound) and the command to resume it,
such as `claude --resume <id>` or `codex resume <id>`. A conversation leaves
its pane when the agent exits, the pane closes, or a new session such as
`/clear` replaces it.

### From scripts or agents

The same operations are available on the command line, so an agent can queue
its own follow-ups. `--pane` defaults to `$HERDR_PANE_ID`:

```bash
P=/path/to/herdr/plugins/prompt-queue/prompt_queue.py
python3 $P add "After the tests pass, write the PR description"
echo "multi-line prompt" | python3 $P add -
python3 $P list [--json] [--all]
python3 $P edit <id> "new text"
python3 $P remove <id>
python3 $P undo
python3 $P insert <id>      # paste without submitting
python3 $P send <id>        # submit now
```

IDs can be shortened to any unique prefix.

## Which conversation a queue belongs to

A queue is keyed by the agent's native session reference, the
`agent_session` binding (source, kind, and ID) that Herdr stores for each pane
and uses to resume the agent after a restart. When Herdr or the AgentHost
session guard brings the conversation back, in the same pane or another one,
its queue comes back with it. Panes without a session reference use a
`pane:<id>` queue. When that pane later reports a session, its entries move
into the session's queue.

If a conversation comes back under a new session ID, its old queue stays
intact. Use `o` to move it into the current conversation.

## Crash durability

State lives in `~/.local/state/herdr/plugins/joeguice.prompt-queue/`:

- `queues.json`: every queue, with a bounded history of removed entries. Each
  change is written to a temporary file, fsynced, atomically renamed over the
  old file, and followed by a directory fsync. Writers serialize on a file lock.
- `queues.json.bak`: the previous version. If `queues.json` is unreadable, it
  is moved aside as `queues.json.corrupt-*` and the backup is restored.
- `drafts/`: prompts being added or edited, saved about once a second while
  you type. If the popup dies mid-edit, the draft is applied the next time you
  open that conversation's queue.
- `reported-counts.json`: a disposable cache of sidebar counts. The startup
  hook rebuilds it.

A Herdr crash, a WSL crash, or a Windows restart loses at most the last second
of typing in an open edit.

## Tests

```bash
cd plugins/prompt-queue && python3 -m unittest discover -s tests
```
