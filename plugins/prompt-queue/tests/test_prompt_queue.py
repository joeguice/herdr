import json
import os
import pathlib
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import prompt_queue as pq  # noqa: E402

REAL_HERDR = pq.Herdr


def claude_pane(pane_id="w1:p1", session="sess-1"):
    pane = {"pane_id": pane_id, "agent": "claude", "cwd": "/repo/project",
            "terminal_id": "term_" + pane_id}
    if session:
        pane["agent_session"] = {"source": "herdr:claude", "agent": "claude", "kind": "id", "value": session}
    return pane


CODEX_SESSION = "01a14008-062c-7a10-999f-81da2f6fbcfb"


def codex_pane(title=None):
    return {
        "pane_id": "w1:p1", "agent": "codex", "cwd": "/repo/project",
        "terminal_id": "term_w1:p1",
        "agent_session": {"source": "herdr:codex", "agent": "codex", "kind": "id", "value": CODEX_SESSION},
        "terminal_title_stripped": title if title is not None else CODEX_SESSION + " | project",
        "terminal_title": title if title is not None else CODEX_SESSION + " | project",
    }


class FakeHerdr:
    def __init__(self, panes):
        self.panes_by_id = {pane["pane_id"]: pane for pane in panes}
        self.calls = []
        self.codex_lock_ids = [CODEX_SESSION]

    def pane(self, pane_id):
        if pane_id not in self.panes_by_id:
            raise pq.HerdrError("pane_not_found", pane_id)
        return self.panes_by_id[pane_id]

    def panes(self):
        return list(self.panes_by_id.values())

    def selected_codex_session(self, pane):
        prefix = pq.codex_title_prefix(pane)
        selected = [sid for sid in self.codex_lock_ids if prefix and sid.startswith(prefix)]
        return selected[0] if len(selected) == 1 else None

    def paste(self, pane_id, text, *, expected):
        self.calls.append(("paste", pane_id, text))

    def submit(self, pane_id, text, is_agent, *, expected):
        self.calls.append(("submit", pane_id, text, is_agent))

    def report_count(self, pane_id, count):
        self.calls.append(("count", pane_id, count))

    def notify(self, title, body):
        self.calls.append(("notify", title, body))


class CheckedTransportTests(unittest.TestCase):
    def test_insert_uses_only_checked_bracketed_paste_endpoint(self):
        client = pq.Herdr("unused")
        expected = pq.conversation_for_pane({**claude_pane(), "terminal_title": "Claude session"})
        with mock.patch.object(client, "request", return_value={}) as request:
            client.paste("w1:p1", "queued text", expected=expected)
        request.assert_called_once_with("pane.send_input_checked", {
            "pane_id": "w1:p1", "text": "queued text", "expected_terminal_id": "term_w1:p1",
            "expected_agent_session": expected.session, "expected_terminal_title": "Claude session"})

    def test_send_uses_only_checked_agent_prompt_and_explicit_null_title(self):
        client = pq.Herdr("unused")
        expected = pq.conversation_for_pane(claude_pane())
        with mock.patch.object(client, "request", return_value={}) as request:
            client.submit("w1:p1", "queued text", True, expected=expected)
        request.assert_called_once_with("agent.prompt_checked", {
            "target": "w1:p1", "text": "queued text", "expected_terminal_id": "term_w1:p1",
            "expected_agent_session": expected.session, "expected_terminal_title": None})

    def test_unavailable_checked_methods_never_fall_back_to_legacy_input(self):
        client = pq.Herdr("unused")
        expected = pq.conversation_for_pane(claude_pane())
        for name, action in (("pane.send_input_checked", lambda: client.paste("w1:p1", "kept", expected=expected)),
                             ("agent.prompt_checked", lambda: client.submit("w1:p1", "kept", True, expected=expected))):
            with self.subTest(method=name):
                with mock.patch.object(client, "request", side_effect=pq.HerdrError("unknown_method", "checked API required")) as request:
                    with self.assertRaises(pq.HerdrError):
                        action()
                    self.assertEqual(request.call_count, 1)
                    self.assertEqual(request.call_args.args[0], name)

    def test_missing_terminal_identity_refuses_before_request(self):
        client = pq.Herdr("unused")
        expected = pq.conversation_for_pane(claude_pane())
        expected.terminal_id = None
        with mock.patch.object(client, "request") as request:
            with self.assertRaises(pq.QueueError):
                client.paste("w1:p1", "kept", expected=expected)
            request.assert_not_called()


class CodexProcessProofTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.cwd = self.root / "repo"
        self.cwd.mkdir()
        self.locks = self.root / "codex/thread-writer-locks"
        self.proc = self.root / "proc"
        self.process = self.proc / "123"
        (self.process / "fd").mkdir(parents=True)
        (self.process / "exe").symlink_to(self.root / "bin/codex")
        (self.process / "cwd").symlink_to(self.cwd)
        (self.process / "stat").write_text("123 (codex) S " + "0 " * 18 + "123456\n")
        (self.process / "fd/55").symlink_to(self.locks / (CODEX_SESSION + ".lock"))
        self.pane = {**codex_pane(CODEX_SESSION[:29] + "... | project"), "cwd": str(self.cwd)}
        self.info = {"pane_id": "w1:p1", "foreground_processes": [
            {"name": "codex", "pid": 123, "cwd": str(self.cwd)}]}
        self.herdr = pq.Herdr(proc_root=self.proc, codex_locks=self.locks)
        self.request = mock.patch.object(self.herdr, "request", return_value={"process_info": self.info})
        self.request.start()
        self.addCleanup(self.request.stop)

    def test_one_unique_matching_uuid_is_resolved_among_unrelated_retained_locks(self):
        (self.process / "fd/56").symlink_to(self.locks / "01a14008-062c-7a10-888f-81da2f6fbcfb.lock")
        self.assertEqual(self.herdr.selected_codex_session(self.pane), CODEX_SESSION)
        self.assertEqual(pq.resolved_conversation(self.herdr, self.pane).session["value"], CODEX_SESSION)

    def test_two_matching_uuid_prefixes_refuse_native_ownership(self):
        (self.process / "fd/56").symlink_to(self.locks / (CODEX_SESSION[:-1] + "c.lock"))
        self.assertIsNone(self.herdr.selected_codex_session(self.pane))
        self.assertEqual(pq.resolved_conversation(self.herdr, self.pane).key, "pane:w1:p1")

    def test_unavailable_wrong_pane_and_wrong_process_evidence_fail_closed(self):
        self.info["pane_id"] = "other:pane"
        self.assertIsNone(self.herdr.selected_codex_session(self.pane))
        self.info["pane_id"] = "w1:p1"
        (self.process / "exe").unlink()
        (self.process / "exe").symlink_to(self.root / "bin/python")
        self.assertIsNone(self.herdr.selected_codex_session(self.pane))
        with mock.patch.object(self.herdr, "request", side_effect=pq.HerdrError("unavailable", "test")):
            self.assertIsNone(self.herdr.selected_codex_session(self.pane))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = pq.Store(pathlib.Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_missing_file_reads_empty_without_writing(self):
        self.assertEqual(self.store.read()["queues"], {})
        self.assertFalse(self.store.path.exists())

    def test_transaction_persists_and_keeps_previous_version_as_backup(self):
        conversation = pq.conversation_for_pane(claude_pane())
        with self.store.transaction() as data:
            pq.add_entry(data, conversation, "first")
        with self.store.transaction() as data:
            pq.add_entry(data, conversation, "second")
        main = json.loads(self.store.path.read_text())
        backup = json.loads(self.store.backup_path.read_text())
        self.assertEqual([e["text"] for e in main["queues"][conversation.key]["entries"]], ["first", "second"])
        self.assertEqual([e["text"] for e in backup["queues"][conversation.key]["entries"]], ["first"])
        self.assertEqual(oct(self.store.path.stat().st_mode & 0o777), "0o600")

    def test_unchanged_transaction_does_not_write(self):
        with self.store.transaction():
            pass
        self.assertFalse(self.store.path.exists())

    def test_corrupt_file_recovers_from_backup_and_quarantines_damage(self):
        conversation = pq.conversation_for_pane(claude_pane())
        with self.store.transaction() as data:
            pq.add_entry(data, conversation, "kept")
        with self.store.transaction() as data:
            pq.add_entry(data, conversation, "lost in torn write")
        self.store.path.write_text('{"version": 1, "queues": {')
        data = self.store.read()
        self.assertEqual([e["text"] for e in data["queues"][conversation.key]["entries"]], ["kept"])
        self.assertTrue(list(self.store.directory.glob("queues.json.corrupt-*")))
        self.assertIn("Recovered queues from backup", self.store.notices[0])
        self.assertEqual(json.loads(self.store.path.read_text())["queues"][conversation.key]["entries"][0]["text"], "kept")

    def test_corrupt_file_without_backup_starts_empty_and_keeps_evidence(self):
        self.store.directory.mkdir(parents=True, exist_ok=True)
        self.store.path.write_text("not json")
        self.assertEqual(self.store.read()["queues"], {})
        self.assertTrue(list(self.store.directory.glob("queues.json.corrupt-*")))

    def test_newer_schema_is_never_overwritten(self):
        self.store.directory.mkdir(parents=True, exist_ok=True)
        self.store.path.write_text(json.dumps({"version": 99, "queues": {}}))
        with self.assertRaises(pq.QueueError):
            self.store.read()
        self.assertEqual(json.loads(self.store.path.read_text())["version"], 99)


class ConversationTests(unittest.TestCase):
    def test_incomplete_or_different_agent_binding_remains_unassigned(self):
        for field, value in (("source", ""), ("agent", "codex"), ("kind", None), ("value", "")):
            with self.subTest(field=field):
                pane = claude_pane()
                pane["agent_session"][field] = value
                self.assertEqual(pq.conversation_for_pane(pane).key, "pane:w1:p1")

    def test_session_binding_is_the_key(self):
        conversation = pq.conversation_for_pane(claude_pane())
        self.assertEqual(conversation.key, "session:herdr:claude:id:sess-1")
        self.assertEqual(conversation.label, "claude · project")

    def test_pane_without_session_falls_back_to_pane_key(self):
        conversation = pq.conversation_for_pane(claude_pane(session=None))
        self.assertEqual(conversation.key, "pane:w1:p1")

    def test_same_session_in_a_different_pane_keeps_its_queue(self):
        before = pq.conversation_for_pane(claude_pane("w1:p1"))
        after = pq.conversation_for_pane(claude_pane("w9:p4"))
        self.assertEqual(before.key, after.key)


class QueueOperationTests(unittest.TestCase):
    def setUp(self):
        self.data = pq.empty_data()
        self.conversation = pq.conversation_for_pane(claude_pane())

    def queue(self):
        return self.data["queues"][self.conversation.key]

    def texts(self):
        return [entry["text"] for entry in self.queue()["entries"]]

    def test_add_normalizes_and_rejects_empty(self):
        entry = pq.add_entry(self.data, self.conversation, "  run tests\x1b[201~\r\nthen fix  \n")
        self.assertEqual(entry["text"], "run tests[201~\nthen fix")
        with self.assertRaises(pq.QueueError):
            pq.add_entry(self.data, self.conversation, " \n\t")

    def test_add_at_index_edit_move_and_prefix_ids(self):
        first = pq.add_entry(self.data, self.conversation, "one")
        pq.add_entry(self.data, self.conversation, "three")
        pq.add_entry(self.data, self.conversation, "two", index=1)
        self.assertEqual(self.texts(), ["one", "two", "three"])
        pq.edit_entry(self.queue(), first["id"][:4], "uno")
        self.assertEqual(pq.move_entry(self.queue(), first["id"], 5), 2)
        self.assertEqual(self.texts(), ["two", "three", "uno"])

    def test_remove_and_undo_restore_position(self):
        pq.add_entry(self.data, self.conversation, "a")
        middle = pq.add_entry(self.data, self.conversation, "b")
        pq.add_entry(self.data, self.conversation, "c")
        pq.remove_entry(self.queue(), middle["id"], "deleted")
        self.assertEqual(self.texts(), ["a", "c"])
        restored = pq.restore_last(self.queue())
        self.assertEqual(restored["id"], middle["id"])
        self.assertEqual(self.texts(), ["a", "b", "c"])
        self.assertIsNone(pq.restore_last(self.queue()))

    def test_history_is_bounded(self):
        for index in range(pq.HISTORY_LIMIT + 5):
            entry = pq.add_entry(self.data, self.conversation, f"p{index}")
            pq.remove_entry(self.queue(), entry["id"], "deleted")
        self.assertEqual(len(self.queue()["history"]), pq.HISTORY_LIMIT)

    def test_consume_pending_only_for_the_inserted_pane_after_grace(self):
        inserted = pq.add_entry(self.data, self.conversation, "inserted")
        pq.add_entry(self.data, self.conversation, "waiting")
        now = time.time()
        pq.mark_pending(self.queue(), inserted["id"], "w1:p1", now)
        self.assertEqual(pq.consume_pending(self.data, "w1:p1", now + 0.1, conversation_key=self.conversation.key), [])
        self.assertEqual(pq.consume_pending(self.data, "w2:p1", now + 5, conversation_key=self.conversation.key), [])
        consumed = pq.consume_pending(self.data, "w1:p1", now + 5, conversation_key=self.conversation.key)
        self.assertEqual([entry["id"] for _, entry in consumed], [inserted["id"]])
        self.assertEqual(self.texts(), ["waiting"])
        self.assertEqual(self.queue()["history"][-1]["reason"], "submitted")
        self.assertNotIn("pending", pq.restore_last(self.queue()))

    def test_clear_pending(self):
        entry = pq.add_entry(self.data, self.conversation, "x")
        pq.mark_pending(self.queue(), entry["id"], "w1:p1", 0)
        self.assertTrue(pq.clear_pending(self.queue(), entry["id"]))
        self.assertEqual(pq.consume_pending(self.data, "w1:p1", 100, conversation_key=self.conversation.key), [])

    def test_pane_queue_can_be_adopted_explicitly(self):
        bare = pq.conversation_for_pane(claude_pane(session=None))
        pq.add_entry(self.data, bare, "queued before session")
        pq.add_entry(self.data, self.conversation, "already here")
        self.assertEqual(pq.merge_queue(self.data, bare.key, self.conversation), 1)
        self.assertNotIn("pane:w1:p1", self.data["queues"])
        self.assertEqual(self.texts(), ["already here", "queued before session"])

    def test_merge_and_delete_queue(self):
        other = pq.conversation_for_pane(claude_pane("w2:p1", session="old"))
        entry = pq.add_entry(self.data, other, "from old session")
        pq.mark_pending(self.data["queues"][other.key], entry["id"], "w2:p1", 0)
        self.assertEqual(pq.merge_queue(self.data, other.key, self.conversation), 1)
        self.assertNotIn("pending", self.queue()["entries"][0])
        pq.delete_queue(self.data, self.conversation.key)
        self.assertEqual(self.data["deleted_queues"][-1]["key"], self.conversation.key)


class CommandTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = pathlib.Path(self.tmp.name)
        self.fake = FakeHerdr([claude_pane()])
        patcher = mock.patch.object(pq, "Herdr", return_value=self.fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)

    def run_cli(self, *argv, env=None):
        with mock.patch.dict(os.environ, env or {}, clear=False):
            return pq.main(["--state-dir", str(self.state), *argv])

    def queue_texts(self):
        data = pq.Store(self.state).read()
        return [e["text"] for e in data["queues"]["session:herdr:claude:id:sess-1"]["entries"]]


class CommandTests(CommandTestCase):
    def test_codex_working_after_idle_switch_retains_old_unsent_insert(self):
        other = "01a14008-062c-7a10-888f-81da2f6fbcfb"
        self.fake.codex_lock_ids.append(other)
        self.fake.panes_by_id["w1:p1"] = codex_pane()
        key = "session:herdr:codex:id:" + CODEX_SESSION
        with mock.patch.object(pq, "input_runtime", return_value={"boot": "same", "socket": "same"}):
            self.run_cli("add", "--pane", "w1:p1", "not submitted in A")
            entry = pq.Store(self.state).read()["queues"][key]["entries"][0]
            self.run_cli("insert", "--pane", "w1:p1", entry["id"])
            self.fake.panes_by_id["w1:p1"] = codex_pane(other[:29] + "... | project")
            event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
            with mock.patch.object(pq.time, "time", return_value=time.time() + 10):
                self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
            queue = pq.Store(self.state).read()["queues"][key]
            self.assertEqual([e["text"] for e in queue["entries"]], ["not submitted in A"])
            self.assertNotIn("pending", queue["entries"][0])
            self.assertEqual(queue["history"], [])
            self.assertEqual(self.fake.calls[-1], ("count", "w1:p1", 0))
            self.fake.panes_by_id["w1:p1"] = codex_pane()
            with mock.patch.object(pq.time, "time", return_value=time.time() + 20):
                self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
            self.assertEqual(pq.Store(self.state).read()["queues"][key]["history"], [])

    def test_codex_new_content_uses_unresolved_queue_when_native_binding_is_stale(self):
        self.fake.panes_by_id["w1:p1"] = codex_pane()
        self.run_cli("add", "--pane", "w1:p1", "original A")
        key = "session:herdr:codex:id:" + CODEX_SESSION
        old_queue = pq.Store(self.state).read()["queues"][key]
        other = "01a14008-062c-7a10-888f-81da2f6fbcfb"
        self.fake.codex_lock_ids.append(other)
        self.fake.panes_by_id["w1:p1"] = codex_pane(other[:29] + "... | project")
        self.run_cli("add", "--pane", "w1:p1", "new text after switch")
        queues = pq.Store(self.state).read()["queues"]
        self.assertEqual(queues[key], old_queue)
        self.assertEqual(queues["pane:w1:p1"]["entries"][0]["text"], "new text after switch")
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        self.assertEqual(app.conversation.key, "pane:w1:p1")
        self.assertEqual(app.entries[0]["text"], "new text after switch")

    def test_unbound_queue_is_preserved_when_native_binding_appears(self):
        self.fake.panes_by_id["w1:p1"] = claude_pane(session=None)
        self.run_cli("add", "--pane", "w1:p1", "unknown owner")
        before = pq.Store(self.state).read()["queues"]["pane:w1:p1"]
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="replacement")
        self.run_cli("startup")
        self.run_cli("add", "--pane", "w1:p1", "known owner")
        queues = pq.Store(self.state).read()["queues"]
        self.assertEqual(queues["pane:w1:p1"], before)
        self.assertEqual([e["text"] for e in queues["session:herdr:claude:id:replacement"]["entries"]],
                         ["known owner"])
        self.assertFalse(any(c[0] in ("paste", "submit") for c in self.fake.calls))

    def test_cli_delivery_refuses_a_changed_or_missing_native_identity(self):
        self.run_cli("add", "--pane", "w1:p1", "keep queued")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        before = (self.state / "queues.json").read_bytes()
        for command in ("insert", "send"):
            for session in (None, "replacement"):
                with self.subTest(command=command, session=session):
                    self.fake.calls.clear()
                    with mock.patch.object(self.fake, "pane", side_effect=[claude_pane(), claude_pane(session=session)]):
                        self.assertEqual(self.run_cli(command, "--pane", "w1:p1", entry["id"]), 1)
                    self.assertEqual((self.state / "queues.json").read_bytes(), before)
                    self.assertFalse(any(c[0] in ("paste", "submit") for c in self.fake.calls))

    def test_cli_delivery_refuses_an_unbound_queue(self):
        self.fake.panes_by_id["w1:p1"] = claude_pane(session=None)
        self.run_cli("add", "--pane", "w1:p1", "unknown owner")
        entry = pq.Store(self.state).read()["queues"]["pane:w1:p1"]["entries"][0]
        before = (self.state / "queues.json").read_bytes()
        for command in ("insert", "send"):
            with self.subTest(command=command):
                self.assertEqual(self.run_cli(command, "--pane", "w1:p1", entry["id"]), 1)
                self.assertEqual((self.state / "queues.json").read_bytes(), before)
        self.assertFalse(any(c[0] in ("paste", "submit") for c in self.fake.calls))

    def test_same_native_session_can_deliver_after_moving_to_another_repository_pane(self):
        self.run_cli("add", "--pane", "w1:p1", "same owner")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        self.fake.panes_by_id = {"w9:p2": {**claude_pane("w9:p2"), "cwd": "/repo/another"}}
        self.assertEqual(self.run_cli("insert", "--pane", "w9:p2", entry["id"]), 0)
        self.assertIn(("paste", "w9:p2", "same owner"), self.fake.calls)

    def test_add_insert_then_working_event_consumes_entry(self):
        self.assertEqual(self.run_cli("add", "--pane", "w1:p1", "review", "the", "diff"), 0)
        self.run_cli("add", "--pane", "w1:p1", "then", "ship")
        entry_id = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]["id"]
        self.run_cli("insert", "--pane", "w1:p1", entry_id)
        self.assertIn(("paste", "w1:p1", "review the diff"), self.fake.calls)

        event = {"event": "pane.agent_status_changed", "data": {"type": "pane_agent_status_changed", "pane_id": "w1:p1", "agent_status": "working"}}
        with mock.patch.object(pq.time, "time", return_value=time.time() + 10):
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["then ship"])
        self.assertEqual(self.fake.calls[-1], ("count", "w1:p1", 1))

    def test_idle_event_does_not_consume(self):
        self.run_cli("add", "--pane", "w1:p1", "x")
        entry_id = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]["id"]
        self.run_cli("insert", "--pane", "w1:p1", entry_id)
        event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "idle"}}
        with mock.patch.object(pq.time, "time", return_value=time.time() + 10):
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["x"])

    def test_send_submits_and_removes(self):
        self.run_cli("add", "--pane", "w1:p1", "go")
        entry_id = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]["id"]
        self.run_cli("send", "--pane", "w1:p1", entry_id)
        self.assertIn(("submit", "w1:p1", "go", True), self.fake.calls)
        self.assertEqual(self.queue_texts(), [])
        self.assertEqual(self.run_cli("undo", "--pane", "w1:p1"), 0)
        self.assertEqual(self.queue_texts(), ["go"])

    def test_hook_without_state_file_is_a_no_op(self):
        event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
        self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertFalse((self.state / "queues.json").exists())
        self.assertEqual(self.fake.calls, [])

    def test_startup_reports_counts_for_restored_sessions(self):
        self.run_cli("add", "--pane", "w1:p1", "survives restart")
        self.fake.calls.clear()
        self.run_cli("startup")
        self.assertEqual(self.fake.calls, [("count", "w1:p1", 1)])

    def test_queue_follows_session_into_a_restored_pane(self):
        self.run_cli("add", "--pane", "w1:p1", "before crash")
        # After a restore the same native session can come back in another pane.
        self.fake.panes_by_id = {"w3:p2": claude_pane("w3:p2")}
        self.run_cli("startup")
        data = pq.Store(self.state).read()
        queue = data["queues"]["session:herdr:claude:id:sess-1"]
        self.assertEqual(queue["last_pane_id"], "w3:p2")
        self.assertEqual([e["text"] for e in queue["entries"]], ["before crash"])

    def test_unsent_insert_survives_restart_and_first_working_event(self):
        with mock.patch.object(pq, "input_runtime", return_value={"boot": "before"}):
            self.run_cli("add", "--pane", "w1:p1", "not submitted")
            entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
            self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        with mock.patch.object(pq, "input_runtime", return_value={"boot": "after"}):
            self.run_cli("startup")
            event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        queue = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]
        self.assertEqual([e["text"] for e in queue["entries"]], ["not submitted"])
        self.assertNotIn("pending", queue["entries"][0])
        self.assertEqual(queue["history"], [])

    def test_late_recovery_without_startup_also_preserves_unsent_insert(self):
        with mock.patch.object(pq, "input_runtime", return_value={"boot": "before"}):
            self.run_cli("add", "--pane", "w1:p1", "keep this")
            entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
            self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        with mock.patch.object(pq, "input_runtime", return_value={"boot": "after"}):
            event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["keep this"])

    def test_working_event_does_not_consume_another_conversations_insert(self):
        self.run_cli("add", "--pane", "w1:p1", "belongs to old conversation")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="replacement")
        event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
        with mock.patch.object(pq.time, "time", return_value=time.time() + 10):
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["belongs to old conversation"])

    def test_recovered_session_clears_old_pane_insertion_marker(self):
        self.run_cli("add", "--pane", "w1:p1", "before crash")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        self.fake.panes_by_id = {"w3:p2": claude_pane("w3:p2")}
        self.run_cli("startup")
        queue = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]
        self.assertNotIn("pending", queue["entries"][0])
        self.assertIn(("count", "w3:p2", 1), self.fake.calls)

    def test_live_reconciliation_keeps_current_runtime_insert_tracking(self):
        self.run_cli("add", "--pane", "w1:p1", "live input")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        self.run_cli("startup")
        queue = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]
        self.assertIn("pending", queue["entries"][0])

    def test_startup_recovers_draft_only_store_and_reports_count_without_sending(self):
        drafts = self.state / "drafts"
        drafts.mkdir()
        (drafts / "first.md").write_text("unfinished queued message")
        (drafts / "first.json").write_text(json.dumps({
            "key": "session:herdr:claude:id:sess-1", "pid": os.getpid(),
            "boot_id": "previous-boot", "entry_id": None, "initial": "",
        }))
        with mock.patch.object(pq, "boot_id", return_value="current-boot"):
            self.assertEqual(self.run_cli("startup"), 0)
        self.assertEqual(self.queue_texts(), ["unfinished queued message"])
        self.assertEqual(self.fake.calls, [("count", "w1:p1", 1)])
        self.run_cli("startup")
        self.assertEqual(self.queue_texts(), ["unfinished queued message"])

    def test_late_recovered_session_gets_draft_and_count_on_detection(self):
        drafts = self.state / "drafts"
        drafts.mkdir()
        (drafts / "later.md").write_text("saved before recovery")
        (drafts / "later.json").write_text(json.dumps({
            "key": "session:herdr:claude:id:sess-1", "pid": 2**30,
            "entry_id": None, "initial": "",
        }))
        self.fake.panes_by_id = {"w1:p1": claude_pane(session=None)}
        self.run_cli("startup")
        self.assertFalse((self.state / "queues.json").exists())
        self.fake.panes_by_id["w1:p1"] = claude_pane()
        event = {"event": "pane.agent_detected", "data": {"pane_id": "w1:p1", "agent": "claude"}}
        self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["saved before recovery"])
        self.assertIn(("count", "w1:p1", 1), self.fake.calls)

    def test_different_session_in_same_directory_does_not_adopt_old_queue_or_draft(self):
        self.run_cli("add", "--pane", "w1:p1", "old queued prompt")
        drafts = self.state / "drafts"
        drafts.mkdir()
        (drafts / "old.md").write_text("old draft")
        (drafts / "old.json").write_text(json.dumps({
            "key": "session:herdr:claude:id:sess-1", "pid": 2**30,
            "entry_id": None, "initial": "",
        }))
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="different")
        self.fake.calls.clear()
        self.run_cli("startup")
        self.assertEqual(self.queue_texts(), ["old queued prompt"])
        self.assertTrue((drafts / "old.json").exists())
        self.assertEqual(self.fake.calls, [])

    def test_working_event_retains_recovered_edit_of_previously_inserted_prompt(self):
        self.run_cli("add", "--pane", "w1:p1", "old pasted text")
        entry = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
        self.run_cli("insert", "--pane", "w1:p1", entry["id"])
        drafts = self.state / "drafts"
        drafts.mkdir()
        (drafts / "edit.md").write_text("new unsent edited text")
        (drafts / "edit.json").write_text(json.dumps({
            "key": "session:herdr:claude:id:sess-1", "pid": 2**30,
            "entry_id": entry["id"], "initial": "old pasted text",
        }))
        event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
        with mock.patch.object(pq.time, "time", return_value=time.time() + 10):
            self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["new unsent edited text"])
        queue = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]
        self.assertEqual(queue["history"], [])

    def test_legacy_unscoped_pending_marker_is_retained_as_unsent(self):
        self.run_cli("add", "--pane", "w1:p1", "legacy unsent prompt")
        with pq.Store(self.state).transaction() as data:
            entry = data["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]
            entry["pending"] = {"pane_id": "w1:p1", "inserted_at": 0}
        event = {"event": "pane.agent_status_changed", "data": {"pane_id": "w1:p1", "agent_status": "working"}}
        self.run_cli("hook", env={"HERDR_PLUGIN_EVENT_JSON": json.dumps(event)})
        self.assertEqual(self.queue_texts(), ["legacy unsent prompt"])


class DeliveryTransitionTests(CommandTestCase):
    def test_server_rejection_or_missing_capability_preserves_ui_and_cli_queues(self):
        app = self.app_with_entry()
        entry_id = app.entries[0]["id"]
        before = (self.state / "queues.json").read_bytes()
        client = REAL_HERDR("unused")
        app.herdr = client
        with mock.patch.object(client, "pane", return_value=claude_pane()):
            for code in ("agent_session_changed", "unknown_method"):
                for command, method in (("insert", "pane.send_input_checked"), ("send", "agent.prompt_checked")):
                    for surface in ("ui", "cli"):
                        with self.subTest(code=code, command=command, surface=surface):
                            with mock.patch.object(client, "request", side_effect=pq.HerdrError(code, "test refusal")) as request:
                                if surface == "ui":
                                    self.assertFalse(getattr(app, command)())
                                else:
                                    with mock.patch.object(pq, "Herdr", return_value=client):
                                        self.assertEqual(self.run_cli(command, "--pane", "w1:p1", entry_id), 1)
                                self.assertEqual(request.call_count, 1)
                                self.assertEqual(request.call_args.args[0], method)
                            self.assertEqual((self.state / "queues.json").read_bytes(), before)

    def test_codex_ambiguous_title_prefix_never_assigns_native_ownership(self):
        self.fake.panes_by_id["w1:p1"] = codex_pane(CODEX_SESSION[:29] + "... | project")
        self.fake.codex_lock_ids.append(CODEX_SESSION[:-1] + "c")
        self.run_cli("add", "--pane", "w1:p1", "unresolved collision")
        self.assertEqual(set(pq.Store(self.state).read()["queues"]), {"pane:w1:p1"})
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        self.assertFalse(app.insert())
        self.assertFalse(app.send())

    def test_popup_add_and_edit_after_switch_keep_original_queue_unchanged(self):
        app = self.app_with_entry()
        original = pq.Store(self.state).read()["queues"][app.conversation.key]
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="replacement")
        with mock.patch.object(app, "with_editor", return_value="new unassigned prompt"):
            app.add(None)
        with mock.patch.object(app, "with_editor", return_value="edited unassigned prompt"):
            app.edit(None)
        queues = pq.Store(self.state).read()["queues"]
        self.assertEqual(queues[app.conversation.key], original)
        self.assertEqual([e["text"] for e in queues["pane:w1:p1"]["entries"]],
                         ["new unassigned prompt", "edited unassigned prompt"])

    def test_editor_owner_loss_stays_unassigned_after_original_session_returns(self):
        app = self.app_with_entry()
        original = pq.Store(self.state).read()["queues"][app.conversation.key]
        app.editor_unassigned = True
        with mock.patch.object(app, "with_editor", return_value="saved after switching away and back"):
            app.edit(None)
        queues = pq.Store(self.state).read()["queues"]
        self.assertEqual(queues[app.conversation.key], original)
        self.assertEqual(queues["pane:w1:p1"]["entries"][0]["text"], "saved after switching away and back")

    def test_codex_delivery_requires_a_matching_managed_session_title(self):
        for title in (CODEX_SESSION + " | project", CODEX_SESSION[:29] + "... | project",
                      "⠋ " + CODEX_SESSION[:29] + "... | project",
                      "⠋ | " + CODEX_SESSION[:29] + "... | project"):
            for field in ("terminal_title_stripped", "terminal_title"):
                with self.subTest(title=title, field=field):
                    pane = codex_pane(title)
                    pane[field] = pane.pop("terminal_title_stripped")
                    fake = FakeHerdr([pane])
                    self.assertEqual(pq.delivery_conversation(fake, pq.resolved_conversation(fake, pane)).session["value"],
                                     CODEX_SESSION)

    def test_codex_popup_refuses_missing_or_stale_title_even_if_binding_is_unchanged(self):
        self.fake.panes_by_id["w1:p1"] = codex_pane()
        self.run_cli("add", "--pane", "w1:p1", "owned Codex prompt")
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        before = (self.state / "queues.json").read_bytes()
        self.fake.calls.clear()
        for title in ("project", "", "01a14008-062c-7a10-888f-81da2... | project",
                      CODEX_SESSION[:29] + " | project", "project | " + CODEX_SESSION):
            with self.subTest(title=title):
                self.fake.panes_by_id["w1:p1"] = codex_pane(title)
                self.assertFalse(app.insert())
                self.assertFalse(app.send())
                self.assertEqual((self.state / "queues.json").read_bytes(), before)
                self.assertEqual(self.fake.calls, [])

    def test_codex_cli_refuses_unchanged_binding_with_switched_title(self):
        self.fake.panes_by_id["w1:p1"] = codex_pane()
        self.run_cli("add", "--pane", "w1:p1", "owned Codex prompt")
        queue = pq.Store(self.state).read()["queues"]["session:herdr:codex:id:" + CODEX_SESSION]
        before = (self.state / "queues.json").read_bytes()
        for command in ("insert", "send"):
            with self.subTest(command=command):
                self.fake.calls.clear()
                with mock.patch.object(self.fake, "pane", side_effect=[
                        codex_pane(), codex_pane("01a14008-062c-7a10-888f-81da2... | project")]):
                    self.assertEqual(self.run_cli(command, "--pane", "w1:p1", queue["entries"][0]["id"]), 1)
                self.assertEqual((self.state / "queues.json").read_bytes(), before)
                self.assertEqual(self.fake.calls, [])

    def app_with_entry(self, session="sess-1"):
        self.fake.panes_by_id["w1:p1"] = claude_pane(session=session)
        self.run_cli("add", "--pane", "w1:p1", "owned prompt")
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        self.fake.calls.clear()
        return app

    def test_popup_refuses_replacement_unbound_exited_and_missing_panes(self):
        app = self.app_with_entry()
        before = (self.state / "queues.json").read_bytes()
        replacements = [claude_pane(session="replacement"), claude_pane(session=None),
                        {**claude_pane(), "agent": None}, None]
        for replacement in replacements:
            if replacement is None:
                self.fake.panes_by_id.clear()
            else:
                self.fake.panes_by_id["w1:p1"] = replacement
            for action in (app.insert, app.send):
                with self.subTest(replacement=replacement, action=action.__name__):
                    self.assertFalse(action())
                    self.assertEqual((self.state / "queues.json").read_bytes(), before)
                    self.assertFalse(any(c[0] in ("paste", "submit") for c in self.fake.calls))

    def test_popup_refuses_delivery_of_unbound_entries_after_binding_appears(self):
        app = self.app_with_entry(session=None)
        self.fake.panes_by_id["w1:p1"] = claude_pane()
        before = (self.state / "queues.json").read_bytes()
        self.assertFalse(app.insert())
        self.assertFalse(app.send())
        self.assertEqual((self.state / "queues.json").read_bytes(), before)
        self.assertEqual(self.fake.calls, [])

    def test_popup_native_identity_survives_a_directory_change(self):
        app = self.app_with_entry()
        self.fake.panes_by_id["w1:p1"] = {**claude_pane(), "cwd": "/repo/other"}
        self.assertTrue(app.insert())
        self.assertIn(("paste", "w1:p1", "owned prompt"), self.fake.calls)
        self.assertTrue(app.send())
        self.assertIn(("submit", "w1:p1", "owned prompt", True), self.fake.calls)

    def test_popup_keeps_unbound_entries_separate_until_explicit_adoption(self):
        self.app_with_entry(session=None)
        self.fake.panes_by_id["w1:p1"] = claude_pane()
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        self.assertEqual(app.entries, [])
        self.assertEqual(app.others[0][0], "pane:w1:p1")
        app.adopt_other()
        self.assertEqual([e["text"] for e in app.entries], ["owned prompt"])
        self.assertNotIn("pane:w1:p1", pq.Store(self.state).read()["queues"])

    def test_explicit_adoption_refuses_a_stale_target_conversation(self):
        self.app_with_entry(session=None)
        self.fake.panes_by_id["w1:p1"] = claude_pane()
        app = pq.QueueApp(pq.Store(self.state), self.fake, "w1:p1")
        app.load()
        before = (self.state / "queues.json").read_bytes()
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="replacement")
        with self.assertRaises(pq.QueueError):
            app.adopt_other()
        self.assertEqual((self.state / "queues.json").read_bytes(), before)


class ConversationEndedTests(CommandTestCase):
    def event(self, name, **data):
        return {"HERDR_PLUGIN_EVENT_JSON": json.dumps({"event": name, "data": {"pane_id": "w1:p1", **data}})}

    def notifications(self):
        return [call for call in self.fake.calls if call[0] == "notify"]

    def start_with_queue(self):
        self.run_cli("add", "--pane", "w1:p1", "still to do")
        self.run_cli("hook", env=self.event("pane.agent_status_changed", agent_status="idle"))

    def test_agent_exit_with_queued_prompts_warns_once_with_resume_command(self):
        self.start_with_queue()
        # Herdr keeps the session binding after exit; only the agent label goes.
        self.fake.panes_by_id["w1:p1"] = {**claude_pane(), "agent": None}
        self.run_cli("hook", env=self.event("pane.agent_detected"))
        self.run_cli("hook", env=self.event("pane.agent_status_changed", agent_status="unknown"))
        self.assertEqual(
            self.notifications(),
            [("notify", "claude · project ended with 1 queued prompt", "Resume with: claude --resume sess-1")],
        )

    def test_new_session_in_the_same_pane_warns_about_the_previous_one(self):
        self.start_with_queue()
        self.fake.panes_by_id["w1:p1"] = claude_pane(session="sess-2")
        self.run_cli("hook", env=self.event("pane.agent_status_changed", agent_status="idle"))
        self.assertEqual(len(self.notifications()), 1)

    def test_adding_records_the_conversation_without_any_status_event(self):
        self.run_cli("add", "--pane", "w1:p1", "queued right away")
        self.run_cli("hook", env=self.event("pane.closed"))
        self.assertEqual(len(self.notifications()), 1)

    def test_closing_the_pane_warns(self):
        self.start_with_queue()
        self.run_cli("hook", env=self.event("pane.closed"))
        self.assertEqual(len(self.notifications()), 1)

    def test_no_warning_while_the_conversation_continues_or_when_queue_is_empty(self):
        self.start_with_queue()
        self.run_cli("hook", env=self.event("pane.agent_status_changed", agent_status="working"))
        self.assertEqual(self.notifications(), [])
        entry_id = pq.Store(self.state).read()["queues"]["session:herdr:claude:id:sess-1"]["entries"][0]["id"]
        self.run_cli("remove", "--pane", "w1:p1", entry_id)
        self.run_cli("hook", env=self.event("pane.closed"))
        self.assertEqual(self.notifications(), [])

    def test_resumed_conversation_is_not_reported_as_ended(self):
        self.start_with_queue()
        self.fake.panes_by_id["w1:p1"] = {**claude_pane(), "agent": None}
        self.run_cli("hook", env=self.event("pane.agent_detected"))
        self.fake.panes_by_id["w1:p1"] = claude_pane()
        self.run_cli("hook", env=self.event("pane.agent_detected", agent="claude"))
        self.run_cli("hook", env=self.event("pane.agent_status_changed", agent_status="working"))
        self.assertEqual(len(self.notifications()), 1)


class EditBufferTests(unittest.TestCase):
    def test_typing_newlines_and_backspace_across_lines(self):
        buffer = pq.EditBuffer()
        buffer.insert("first\tline\r\nsecond")
        self.assertEqual(buffer.text, "first    line\nsecond")
        buffer.home()
        buffer.backspace()
        self.assertEqual(buffer.text, "first    linesecond")
        self.assertEqual((buffer.row, buffer.col), (0, 13))
        buffer.newline()
        buffer.end()
        buffer.delete()
        buffer.insert("!")
        self.assertEqual(buffer.text, "first    line\nsecond!")

    def test_editing_existing_text_starts_at_the_end(self):
        buffer = pq.EditBuffer("a\nbc")
        self.assertEqual((buffer.row, buffer.col), (1, 2))
        buffer.left(); buffer.left(); buffer.left()
        self.assertEqual((buffer.row, buffer.col), (0, 1))
        buffer.right()
        self.assertEqual((buffer.row, buffer.col), (1, 0))

    def test_layout_soft_wraps_and_places_the_cursor(self):
        buffer = pq.EditBuffer("abcdefgh\nxy")
        rows = buffer.layout(3)
        self.assertEqual(rows, [(0, 0, 3), (0, 3, 6), (0, 6, 8), (1, 0, 2)])
        self.assertEqual(buffer.cursor(rows), (3, 2))
        buffer.row, buffer.col = 0, 3
        self.assertEqual(buffer.cursor(rows), (1, 0))
        buffer.row, buffer.col = 0, 8
        self.assertEqual(buffer.cursor(rows), (2, 2))

    def test_up_and_down_move_through_wrapped_rows_keeping_the_column(self):
        buffer = pq.EditBuffer("abcdefgh\nxy")
        buffer.vertical(-1, 3)
        self.assertEqual((buffer.row, buffer.col), (0, 8))
        buffer.vertical(-1, 3)
        self.assertEqual((buffer.row, buffer.col), (0, 5))
        buffer.vertical(-1, 3)
        buffer.vertical(-1, 3)
        self.assertEqual((buffer.row, buffer.col), (0, 0))
        buffer.end()
        buffer.vertical(5, 3)
        self.assertEqual((buffer.row, buffer.col), (1, 2))

    def test_wide_characters_wrap_by_display_width(self):
        buffer = pq.EditBuffer("日本語")
        self.assertEqual(buffer.layout(4), [(0, 0, 2), (0, 2, 3)])

    def test_finish_edit_ignores_empty_and_unchanged_text(self):
        self.assertIsNone(pq.finish_edit("  \n", ""))
        self.assertIsNone(pq.finish_edit("same\n", "same"))
        self.assertEqual(pq.finish_edit("new", "old"), "new")


class DraftRecoveryTests(unittest.TestCase):
    def test_changed_editor_owner_preserves_original_and_recovers_unassigned_draft(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            with store.transaction() as data:
                entry = pq.add_entry(data, conversation, "original prompt")
            draft = pq.DraftFile(store, conversation, "original prompt", entry["id"])
            draft.unassign("w1:p1")
            draft.save("edited after owner changed", time.time(), force=True)
            with mock.patch.object(pq, "draft_owner_alive", return_value=False):
                self.assertEqual(pq.recover_unassigned_drafts(store), 1)
            queues = store.read()["queues"]
            self.assertEqual(queues[conversation.key]["entries"][0]["text"], "original prompt")
            self.assertEqual(queues["pane:w1:p1"]["entries"][0]["text"], "edited after owner changed")

    def test_unbound_draft_recovers_to_original_pane_queue_after_binding_appears(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            drafts = pq.drafts_dir(store)
            drafts.mkdir()
            (drafts / "unassigned.md").write_text("unknown owner draft")
            (drafts / "unassigned.json").write_text(json.dumps({
                "key": "pane:w1:p1", "pid": 2**30, "entry_id": None, "initial": ""}))
            fake = FakeHerdr([claude_pane()])
            pq.sync_pane(store, fake, claude_pane())
            queues = store.read()["queues"]
            self.assertEqual([e["text"] for e in queues["pane:w1:p1"]["entries"]], ["unknown owner draft"])
            self.assertNotIn("session:herdr:claude:id:sess-1", queues)
            app = pq.QueueApp(store, fake, "w1:p1")
            app.load()
            self.assertEqual(app.others[0][0], "pane:w1:p1")
            self.assertEqual(app.entries, [])
            self.assertFalse(any(c[0] in ("paste", "submit") for c in fake.calls))
            self.assertEqual(list(drafts.iterdir()), [])

    def test_recovered_edit_preserves_a_newer_queue_edit_and_both_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            with store.transaction() as data:
                entry = pq.add_entry(data, conversation, "initial text")
            drafts = pq.drafts_dir(store)
            drafts.mkdir()
            (drafts / "conflict.md").write_text("abandoned draft version")
            (drafts / "conflict.json").write_text(json.dumps({
                "key": conversation.key, "entry_id": entry["id"],
                "pid": 2**30, "initial": "initial text",
            }))
            with store.transaction() as data:
                pq.edit_entry(data["queues"][conversation.key], entry["id"], "newer intentional edit")
            self.assertEqual(pq.recover_drafts(store, conversation), 1)
            entries = store.read()["queues"][conversation.key]["entries"]
            self.assertEqual([e["text"] for e in entries], ["newer intentional edit", "abandoned draft version"])

    def test_oversized_draft_is_preserved_for_manual_rescue(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            drafts = pq.drafts_dir(store)
            drafts.mkdir()
            (drafts / "large.md").write_text("x" * (pq.MAX_ENTRY_BYTES + 1))
            (drafts / "large.json").write_text(json.dumps({
                "key": conversation.key, "entry_id": None, "pid": 2**30, "initial": "",
            }))
            self.assertEqual(pq.recover_drafts(store, conversation), 0)
            self.assertTrue((drafts / "large.md").exists())
            self.assertTrue((drafts / "large.json").exists())
            self.assertFalse(store.path.exists())

    def test_active_editor_is_not_recovered(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            draft = pq.DraftFile(store, conversation, "", None)
            draft.save("still typing", time.monotonic(), force=True)
            self.assertEqual(pq.recover_drafts(store, conversation), 0)
            self.assertFalse(store.path.exists())
            self.assertTrue(draft.meta_path.exists())

    def test_reused_pid_is_not_treated_as_live_editor(self):
        meta = {"pid": os.getpid(), "boot_id": "same-boot", "process_start": "old-start"}
        with mock.patch.object(pq, "boot_id", return_value="same-boot"), mock.patch.object(pq, "process_start", return_value="new-start"):
            self.assertFalse(pq.draft_owner_alive(meta))

    def test_recovering_same_draft_again_does_not_duplicate_prompt(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            drafts = pq.drafts_dir(store)
            drafts.mkdir()
            text = "saved queued prompt"
            meta = json.dumps({"key": conversation.key, "pid": 2**30, "initial": "", "entry_id": None})
            for attempt in range(2):
                (drafts / "same.md").write_text(text)
                (drafts / "same.json").write_text(meta)
                self.assertEqual(pq.recover_drafts(store, conversation), 1 if attempt == 0 else 0)
            self.assertEqual(len(store.read()["queues"][conversation.key]["entries"]), 1)

    def test_orphaned_draft_is_recovered_as_an_edit_or_new_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = pq.Store(pathlib.Path(tmp))
            conversation = pq.conversation_for_pane(claude_pane())
            with store.transaction() as data:
                entry = pq.add_entry(data, conversation, "old text")
            drafts = pq.drafts_dir(store)
            drafts.mkdir()
            dead_pid = 2**22 + 12345
            (drafts / "a.md").write_text("new text")
            (drafts / "a.json").write_text(json.dumps({"key": conversation.key, "entry_id": entry["id"], "pid": dead_pid, "initial": "old text"}))
            (drafts / "b.md").write_text("brand new")
            (drafts / "b.json").write_text(json.dumps({"key": conversation.key, "entry_id": None, "pid": dead_pid, "initial": ""}))
            self.assertEqual(pq.recover_drafts(store, conversation), 2)
            texts = [e["text"] for e in store.read()["queues"][conversation.key]["entries"]]
            self.assertEqual(texts, ["new text", "brand new"])
            self.assertEqual(list(drafts.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
