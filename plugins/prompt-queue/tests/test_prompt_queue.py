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


def claude_pane(pane_id="w1:p1", session="sess-1"):
    pane = {"pane_id": pane_id, "agent": "claude", "cwd": "/repo/project"}
    if session:
        pane["agent_session"] = {"source": "herdr:claude", "agent": "claude", "kind": "id", "value": session}
    return pane


class FakeHerdr:
    def __init__(self, panes):
        self.panes_by_id = {pane["pane_id"]: pane for pane in panes}
        self.calls = []

    def pane(self, pane_id):
        if pane_id not in self.panes_by_id:
            raise pq.HerdrError("pane_not_found", pane_id)
        return self.panes_by_id[pane_id]

    def panes(self):
        return list(self.panes_by_id.values())

    def paste(self, pane_id, text):
        self.calls.append(("paste", pane_id, text))

    def submit(self, pane_id, text, is_agent):
        self.calls.append(("submit", pane_id, text, is_agent))

    def report_count(self, pane_id, count):
        self.calls.append(("count", pane_id, count))

    def notify(self, title, body):
        self.calls.append(("notify", title, body))


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
        self.assertEqual(pq.consume_pending(self.data, "w1:p1", now + 0.1), [])
        self.assertEqual(pq.consume_pending(self.data, "w2:p1", now + 5), [])
        consumed = pq.consume_pending(self.data, "w1:p1", now + 5)
        self.assertEqual([entry["id"] for _, entry in consumed], [inserted["id"]])
        self.assertEqual(self.texts(), ["waiting"])
        self.assertEqual(self.queue()["history"][-1]["reason"], "submitted")
        self.assertNotIn("pending", pq.restore_last(self.queue()))

    def test_clear_pending(self):
        entry = pq.add_entry(self.data, self.conversation, "x")
        pq.mark_pending(self.queue(), entry["id"], "w1:p1", 0)
        self.assertTrue(pq.clear_pending(self.queue(), entry["id"]))
        self.assertEqual(pq.consume_pending(self.data, "w1:p1", 100), [])

    def test_pane_queue_is_adopted_when_session_appears(self):
        bare = pq.conversation_for_pane(claude_pane(session=None))
        pq.add_entry(self.data, bare, "queued before session")
        pq.add_entry(self.data, self.conversation, "already here")
        self.assertEqual(pq.adopt_pane_queue(self.data, self.conversation), 1)
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


class DraftRecoveryTests(unittest.TestCase):
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
