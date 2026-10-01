import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import DomainError, RadioDB


class ReviewBatchTest(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        self.db = RadioDB(self.path)
        self.p1 = self.db.add_program("早间新闻", "talk", 30, "2026-01-01", "2026-12-31", None, 0, ["华东"])
        self.p2 = self.db.add_program("品牌广告", "ad", 5, "2026-01-01", "2026-12-31", "青柠", 0, ["华东"])
        self.day = "2026-09-28"

    def tearDown(self):
        self.db.close()
        os.unlink(self.path)

    def _two_slots(self):
        first = self.db.schedule_slot(self.day, "09:00", self.p1, "华东")
        second = self.db.schedule_slot(self.day, "10:00", self.p2, "华东")
        return first, second

    # --- 需求 1：排期/替换一变，授权窗口立即失效重算 ----------------------

    def test_schedule_change_invalidates_and_recomputes_open_batch(self):
        first, second = self._two_slots()
        self.db.record_playout(first, "09:00", 30, self.p1)
        self.db.record_playout(second, "10:00", 5, self.p2)
        batch = self.db.start_review(self.day)
        self.assertEqual("open", batch["status"])
        self.assertEqual([], [i for i in batch["items"] if i["exceptions"]])

        # 人工确认第一项
        self.db.confirm_item(batch["id"], 1)
        version_before = self.db.plan_version()

        # 新排期落入当天：计划版本抬升，进行中的批次立即重算
        third = self.db.schedule_slot(self.day, "12:00", self.p1, "华东")
        self.assertGreater(self.db.plan_version(), version_before)
        refreshed = self.db.get_batch(batch["id"])
        self.assertEqual(self.db.plan_version(), refreshed["plan_version"])
        self.assertGreaterEqual(refreshed["recompute_count"], 1)
        self.assertEqual([1, 2, 3], [i["seq"] for i in refreshed["items"]])
        # 旧项目结果指纹未变，已确认状态保留
        self.assertEqual("confirmed", refreshed["items"][0]["status"])
        self.assertEqual("pending", refreshed["items"][1]["status"])
        self.assertEqual("pending", refreshed["items"][2]["status"])

    def test_replace_invalidates_fingerprint_and_resets_changed_item(self):
        first, second = self._two_slots()
        self.db.record_playout(first, "09:00", 5, self.p2, "临时切广告")
        batch = self.db.start_review(self.day)
        item = next(i for i in batch["items"] if i["slot_id"] == first)
        self.assertTrue(any(e["kind"] == "wrong_program" for e in item["exceptions"]))
        self.db.confirm_item(batch["id"], item["seq"])

        # 替换计划节目：指纹变化，已确认项回退为待确认
        self.db.replace_slot(first, self.p2)
        refreshed = self.db.get_batch(batch["id"])
        item = next(i for i in refreshed["items"] if i["slot_id"] == first)
        self.assertEqual("pending", item["status"])
        self.assertFalse(any(e["kind"] == "wrong_program" for e in item["exceptions"]))
        self.assertEqual(self.p2, item["plan_program_id"])
        self.assertEqual(self.p2, item["actual_program_id"])

    def test_program_window_shows_intervals_and_blockers(self):
        self.db.add_blocked_window("华东", 0, "08:00", "08:30", "周一设备检修")
        window = self.db.program_window(self.p1, self.day, "华东")  # 2026-09-28 是周一
        intervals = [(i["start"], i["end"]) for i in window["intervals"]]
        self.assertIn(("00:00", "08:00"), intervals)
        self.assertIn(("08:30", "24:00"), intervals)
        self.assertTrue(window["playable"])
        blocked = self.db.program_window(self.p1, self.day, "华东", "08:10")
        self.assertFalse(blocked["playable"])
        self.assertTrue(any("禁播" in b for b in blocked["blockers"]))
        unauthorized = self.db.program_window(self.p1, self.day, "华北")
        self.assertFalse(unauthorized["playable"])
        self.assertTrue(any("未授权" in b for b in unauthorized["blockers"]))

    # --- 需求 2：并发提交只留先到结果，后到存草稿+差异 --------------------

    def test_concurrent_submit_first_wins_loser_saves_draft_with_diff(self):
        first, second = self._two_slots()
        self.db.record_playout(first, "09:00", 30, self.p1)
        winner = self.db.start_review(self.day, created_by="甲")
        loser = self.db.start_review(self.day, created_by="乙")  # 幂等，同一批次
        self.assertEqual(winner["id"], loser["id"])
        errors = []

        def submit(name, verdict, bucket):
            try:
                other = RadioDB(self.path)
                results = {i["seq"]: {"verdict": verdict, "note": f"{name}的意见"}
                           for i in other.get_batch(winner["id"])["items"]}
                bucket.append(other.submit_batch(winner["id"], name, results))
                other.close()
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        outcomes: list = []
        t1 = threading.Thread(target=submit, args=("甲", "ok", outcomes))
        t2 = threading.Thread(target=submit, args=("乙", "waived", outcomes))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual([], errors)
        self.assertEqual(2, len(outcomes))
        confirmed = [o for o in outcomes if o["outcome"] == "confirmed"]
        drafted = [o for o in outcomes if o["outcome"] == "draft"]
        self.assertEqual(1, len(confirmed))
        self.assertEqual(1, len(drafted))

        final = self.db.get_batch(winner["id"])
        self.assertEqual("confirmed", final["status"])
        draft = drafted[0]["draft"]
        self.assertEqual("draft", draft["status"])
        self.assertEqual(winner["id"], draft["based_on_batch_id"])
        self.assertTrue(draft["batch_no"].endswith("-D1"))
        # 差异逐项记录：verdict 一定不同（先到者的结论 vs 后到者的结论）
        diffs = drafted[0]["diff"]
        verdict_diffs = [d for d in diffs if d["field"] == "verdict"]
        self.assertTrue(verdict_diffs)
        winner_verdicts = {d["winner"] for d in verdict_diffs}
        loser_verdicts = {d["yours"] for d in verdict_diffs}
        self.assertEqual(1, len(winner_verdicts))
        self.assertEqual(1, len(loser_verdicts))
        self.assertNotEqual(winner_verdicts, loser_verdicts)
        self.assertTrue(winner_verdicts <= {"ok", "waived"})
        self.assertTrue(loser_verdicts <= {"ok", "waived"})

    def test_draft_cannot_be_submitted_again(self):
        first, second = self._two_slots()
        self.db.start_review(self.day)
        b = self.db.start_review(self.day)
        self.db.submit_batch(b["id"], "甲")
        result = self.db.submit_batch(b["id"], "乙", {1: {"verdict": "waived"}})
        self.assertEqual("draft", result["outcome"])
        with self.assertRaisesRegex(DomainError, "草稿"):
            self.db.submit_batch(result["draft"]["id"], "乙")

    # --- 需求 3：写入失败重试，从最后确认项继续 ---------------------------

    def test_confirm_retries_after_write_failure_and_checkpoints(self):
        first, second = self._two_slots()
        batch = self.db.start_review(self.day)
        self.db._crash_writes = 1  # 下一次提交失败
        refreshed = self.db.confirm_item(batch["id"], 1)  # 内部重试成功
        self.assertEqual("confirmed", refreshed["items"][0]["status"])
        self.assertEqual(1, refreshed["last_item_seq"])
        # 再次失败后重试，只确认第 2 项，不重复确认第 1 项
        self.db._crash_writes = 1
        refreshed = self.db.confirm_item(batch["id"], 2)
        self.assertEqual(2, refreshed["last_item_seq"])
        self.assertTrue(all(i["status"] == "confirmed" for i in refreshed["items"]))

    def test_submit_resumes_from_checkpoint_without_reconfirming(self):
        first, second = self._two_slots()
        batch = self.db.start_review(self.day)
        self.db.confirm_item(batch["id"], 1)
        self.assertEqual(1, self.db.get_batch(batch["id"])["last_item_seq"])
        # 跨请求崩溃：第 2 项的确认提交当次耗尽重试，模拟请求失败
        self.db._crash_writes = 1
        with self.assertRaises(sqlite3.OperationalError):
            self.db.confirm_item(batch["id"], 2, attempts=1)
        mid = self.db.get_batch(batch["id"])
        self.assertEqual(1, mid["last_item_seq"])  # 没有脏写
        self.assertEqual("pending", mid["items"][1]["status"])
        # 重试全部耗尽后恢复；另一次提交从检查点续跑，不重复确认第 1 项
        confirmed_seq = []
        original_confirm = self.db.confirm_item

        def spy(bid, seq=None):
            confirmed_seq.append(seq)
            return original_confirm(bid, seq)

        self.db.confirm_item = spy
        result = self.db.submit_batch(batch["id"], "甲")
        self.assertEqual([None, None], confirmed_seq)  # 两次都是“挑下一项”
        self.assertEqual("confirmed", result["outcome"])
        self.assertEqual(2, result["batch"]["last_item_seq"])

    # --- 需求 4：旧数据回填初始版本 ---------------------------------------

    def test_legacy_exceptions_backfilled_with_initial_batch(self):
        first, second = self._two_slots()
        # 直接写入无批次号的旧异常，模拟旧版本对账数据
        self.db.conn.execute(
            "INSERT INTO reconciliation_exceptions(air_date,slot_id,kind,detail,created_at) VALUES(?,?,?,?,?)",
            (self.day, first, "missed", "没有实播记录", "2026-09-01T00:00:00"),
        )
        self.db.conn.commit()
        reloaded = RadioDB(self.path)
        batches = [b for b in reloaded.list_batches(self.day)]
        legacy = [b for b in batches if b["legacy"]]
        self.assertEqual(1, len(legacy))
        batch = legacy[0]
        self.assertEqual("confirmed", batch["status"])
        self.assertEqual(1, batch["version_no"])
        self.assertTrue(batch["batch_no"].endswith("-V01"))
        self.assertEqual("系统回填", batch["confirmed_by"])
        orphans = reloaded.conn.execute(
            "SELECT COUNT(*) FROM reconciliation_exceptions WHERE batch_id IS NULL"
        ).fetchone()[0]
        self.assertEqual(0, orphans)
        # 再次发起复核是新版本，旧批次不再被当成有效
        fresh = reloaded.start_review(self.day)
        self.assertEqual(2, fresh["version_no"])
        self.assertEqual("open", fresh["status"])
        reloaded.close()

    def test_legacy_schema_with_old_unique_constraint_migrates(self):
        self.db.close()
        os.unlink(self.path)
        raw = sqlite3.connect(self.path)
        raw.executescript(
            """
            CREATE TABLE programs (
              id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, kind TEXT NOT NULL,
              duration_minutes INTEGER NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
              sponsor TEXT, cooldown_minutes INTEGER NOT NULL DEFAULT 0,
              active INTEGER NOT NULL DEFAULT 1, UNIQUE(title, start_date, end_date));
            CREATE TABLE program_regions (
              program_id INTEGER NOT NULL, region TEXT NOT NULL, PRIMARY KEY(program_id, region));
            CREATE TABLE blocked_windows (
              id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT NOT NULL, weekday INTEGER NOT NULL,
              start_time TEXT NOT NULL, end_time TEXT NOT NULL, reason TEXT NOT NULL);
            CREATE TABLE sponsor_policies (
              sponsor TEXT PRIMARY KEY, min_gap_minutes INTEGER NOT NULL);
            CREATE TABLE slots (
              id INTEGER PRIMARY KEY AUTOINCREMENT, air_date TEXT NOT NULL, start_time TEXT NOT NULL,
              duration_minutes INTEGER NOT NULL, program_id INTEGER NOT NULL, region TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'planned', replaced_from INTEGER, created_at TEXT NOT NULL);
            CREATE TABLE playout_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT, slot_id INTEGER NOT NULL,
              actual_start TEXT NOT NULL, actual_duration_minutes INTEGER NOT NULL,
              actual_program_id INTEGER, note TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL);
            CREATE TABLE reconciliation_exceptions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              air_date TEXT NOT NULL, slot_id INTEGER NOT NULL,
              kind TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL,
              UNIQUE(air_date, slot_id, kind)
            );
            INSERT INTO programs(id,title,kind,duration_minutes,start_date,end_date)
              VALUES (1,'早间新闻','talk',30,'2026-01-01','2026-12-31');
            INSERT INTO program_regions(program_id,region) VALUES (1,'华东');
            INSERT INTO slots(id,air_date,start_time,duration_minutes,program_id,region,created_at)
              VALUES (1,'2026-09-28','09:00',30,1,'华东','2026-09-01T00:00:00');
            INSERT INTO reconciliation_exceptions(air_date,slot_id,kind,detail,created_at)
              VALUES ('2026-09-28', 1, 'missed', '旧库数据', '2026-09-01T00:00:00');
            """
        )
        raw.commit(); raw.close()
        migrated = RadioDB(self.path)
        batches = migrated.list_batches("2026-09-28")
        self.assertEqual(1, len(batches))
        self.assertEqual(1, batches[0]["legacy"])
        # 同日可以并存多个批次版本的异常（旧唯一约束已移除）
        new_batch = migrated.start_review("2026-09-28")
        self.assertEqual(2, new_batch["version_no"])
        rows = migrated.conn.execute(
            "SELECT COUNT(DISTINCT batch_id) FROM reconciliation_exceptions WHERE air_date='2026-09-28'"
        ).fetchone()[0]
        self.assertEqual(2, rows)
        migrated.close()

    # --- 需求 5：复核发起/确认 + 批次版本 ---------------------------------

    def test_review_flow_and_latest_exceptions(self):
        first, second = self._two_slots()
        # 第一个节目没有实播 -> missed
        self.db.record_playout(second, "10:00", 5, self.p2)
        batch = self.db.start_review(self.day)
        missed = next(i for i in batch["items"] if i["slot_id"] == first)
        self.assertTrue(any(e["kind"] == "missed" for e in missed["exceptions"]))
        self.assertFalse(missed["window"]["playable"] is False and not missed["window"]["blockers"])
        result = self.db.submit_batch(batch["id"], "值班员")
        self.assertEqual("confirmed", result["outcome"])
        self.assertEqual(2, result["batch"]["last_item_seq"])

        # 登记迟到的实播后重新对账：生成新版本，旧 confirmed 批次保留为历史
        self.db.record_playout(first, "09:00", 30, self.p1)
        again = self.db.start_review(self.day)
        self.assertNotEqual(batch["id"], again["id"])
        self.assertEqual(2, again["version_no"])
        kinds = {(e["slot_id"], e["kind"]) for e in self.db.get_exceptions(self.day)}
        self.assertNotIn((first, "missed"), kinds)

    def test_reconcile_date_returns_exceptions_and_creates_batch(self):
        first, second = self._two_slots()
        exceptions = self.db.reconcile_date(self.day)
        slots = {e["slot_id"] for e in exceptions}
        self.assertIn(first, slots)
        self.assertIn(second, slots)
        batch = self.db.list_batches(self.day)[0]
        self.assertEqual("open", batch["status"])


if __name__ == "__main__":
    unittest.main()
