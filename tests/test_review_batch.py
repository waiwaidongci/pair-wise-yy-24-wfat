import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import DomainError, RadioDB


class ReviewBatchTest(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".db")
        os.close(handle)
        self.db = RadioDB(self.path)
        self.music = self.db.add_program("晨间轻音乐", "music", 30, "2026-01-01", "2026-12-31", "青柠", 45, ["华东"])
        self.news = self.db.add_program("城市早报", "talk", 30, "2026-01-01", "2026-12-31", None, 0, ["华东"])
        self.ad = self.db.add_program("青柠广告", "ad", 5, "2026-01-01", "2026-12-31", "青柠", 60, ["华东"])
        self.db.add_sponsor_policy("青柠", 90)
        self.db.add_blocked_window("华东", 0, "08:00", "08:30", "周一设备检修")

    def tearDown(self):
        self.db.close()
        os.unlink(self.path)

    def _air(self):
        return "2026-09-28"  # 周一

    def test_initiate_computes_playable_intervals_and_blocking(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        review = self.db.initiate_review(self._air())
        self.assertEqual("draft", review["status"])
        self.assertEqual(1, review["version"])
        self.assertEqual(1, review["total_count"])
        item = review["items"][0]
        # 08:00-08:30 禁播，可播区间应为两段
        self.assertIn(["00:00", "08:00"], item["playable_intervals"])
        self.assertIn(["08:30", "24:00"], item["playable_intervals"])
        self.assertEqual(0, item["blocked"])
        self.assertEqual("", item["blocked_reason"])
        self.assertEqual("pending", item["check_status"])

    def test_blocked_slot_reports_reason(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        # 排期后新增禁播时段，使原排期变为阻断（授权窗口漂移）
        self.db.add_blocked_window("华东", 0, "09:00", "09:30", "临时重大活动")
        review = self.db.initiate_review(self._air())
        item = review["items"][0]
        self.assertEqual(1, item["blocked"])
        self.assertIn("临时重大活动", item["blocked_reason"])

    def test_confirm_is_first_write_wins_and_loser_saves_draft_with_diff(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        review = self.db.initiate_review(self._air())
        batch_id = review["id"]

        first = self.db.confirm_review(batch_id, 1, "甲")
        self.assertEqual("confirmed", first["status"])
        self.assertEqual(2, first["batch"]["version"])
        self.assertTrue(all(i["check_status"] == "confirmed" for i in first["batch"]["items"]))

        second = self.db.confirm_review(batch_id, 1, "乙")
        self.assertEqual("superseded", second["status"])
        self.assertIsNotNone(second["winner_batch_id"])
        self.assertNotEqual(batch_id, second["draft_batch_id"])
        draft = second["batch"]
        self.assertEqual("draft", draft["status"])
        self.assertEqual(1, draft["base_version"])
        # 草稿条目保留自己的计算结果
        self.assertEqual(1, draft["total_count"])
        self.assertEqual("pending", draft["items"][0]["check_status"])

    def test_stale_version_confirm_is_rejected_after_confirm(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        review = self.db.initiate_review(self._air())
        self.db.confirm_review(review["id"], 1)
        # 再用旧版本确认同一批次 -> 后到者草稿
        again = self.db.confirm_review(review["id"], 1)
        self.assertEqual("superseded", again["status"])

    def test_schedule_or_replace_invalidates_open_and_confirmed_batches(self):
        slot = self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        review = self.db.initiate_review(self._air())
        self.db.confirm_review(review["id"], 1)
        self.assertEqual("confirmed", self.db.get_review(review["id"])["status"])

        # 替换一变，已确认批次立即失效
        self.db.replace_slot(slot, self.news)
        self.assertEqual("superseded", self.db.get_review(review["id"])["status"])

        # 重新发起草稿，再排一期 -> 草稿也失效
        review2 = self.db.initiate_review(self._air())
        self.assertEqual("draft", review2["status"])
        self.db.schedule_slot(self._air(), "10:00", self.news, "华东")
        self.assertEqual("superseded", self.db.get_review(review2["id"])["status"])

    def test_backfill_assigns_initial_version_to_slots_without_batch(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        self.db.schedule_slot(self._air(), "10:00", self.news, "华东")
        # 排期新建时没有批次号
        slots = self.db.conn.execute("SELECT batch_id FROM slots").fetchall()
        self.assertTrue(all(s["batch_id"] is None for s in slots))

        created = self.db.backfill_batches()
        self.assertEqual(1, created)
        batch = self.db.conn.execute(
            "SELECT * FROM review_batches WHERE air_date=?", (self._air(),)
        ).fetchone()
        self.assertEqual("confirmed", batch["status"])
        self.assertEqual(1, batch["version"])
        slots = self.db.conn.execute("SELECT batch_id FROM slots").fetchall()
        self.assertTrue(all(s["batch_id"] == batch["id"] for s in slots))
        # 幂等：再次回填不重复创建
        self.assertEqual(0, self.db.backfill_batches())

    def test_confirm_resumes_from_last_confirmed_item_after_failure(self):
        s1 = self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        s2 = self.db.schedule_slot(self._air(), "10:00", self.news, "华东")
        review = self.db.initiate_review(self._air())
        batch_id = review["id"]

        real_compute = self.db._compute_review_item
        calls = {"n": 0}

        def flaky(slot):
            calls["n"] += 1
            if slot["id"] == s2 and calls["n"] == 2:
                raise RuntimeError("模拟写入失败")
            return real_compute(slot)

        self.db._compute_review_item = flaky
        with self.assertRaises(RuntimeError):
            self.db.confirm_review(batch_id, 1)

        after_fail = self.db.get_review(batch_id)
        statuses = {i["slot_id"]: i["check_status"] for i in after_fail["items"]}
        # 第一条已确认，第二条失败
        self.assertEqual("confirmed", statuses[s1])
        self.assertEqual("failed", statuses[s2])

        # 恢复后重试：从最后确认项继续，只处理未确认条目
        self.db._compute_review_item = real_compute
        result = self.db.retry_review(batch_id)
        self.assertEqual("confirmed", result["status"])
        self.assertTrue(all(i["check_status"] == "confirmed" for i in result["batch"]["items"]))

    def test_list_reviews_filters_by_date(self):
        self.db.schedule_slot(self._air(), "09:00", self.music, "华东")
        r1 = self.db.initiate_review(self._air())
        self.db.initiate_review("2026-09-29")
        reviews = self.db.list_reviews(self._air())
        self.assertEqual([r1["id"]], [r["id"] for r in reviews])


if __name__ == "__main__":
    unittest.main()
