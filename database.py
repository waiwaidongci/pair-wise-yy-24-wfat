from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, time, timedelta
from pathlib import Path


class DomainError(ValueError):
    """A business-rule violation that should be shown to the API caller."""


PROGRAM_KINDS = {"music", "ad", "talk", "live"}
WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _minutes(value: str) -> int:
    if value == "24:00":
        return 24 * 60
    parsed = datetime.strptime(value, "%H:%M")
    return parsed.hour * 60 + parsed.minute


def _fmt_minutes(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _overlap(a_start: str, a_duration: int, b_start: str, b_duration: int) -> bool:
    start_a, start_b = _minutes(a_start), _minutes(b_start)
    return start_a < start_b + b_duration and start_b < start_a + a_duration


class RadioDB:
    """SQLite-backed radio scheduling service.

    The service keeps planning and actual playout separate. A replacement is
    accepted only when the complete plan remains valid; reconciliation never
    rewrites the plan, it records discrepancies for operators.
    """

    def __init__(self, path: str = "radio.db") -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
        self._schema()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS programs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              kind TEXT NOT NULL,
              duration_minutes INTEGER NOT NULL CHECK(duration_minutes > 0),
              start_date TEXT NOT NULL,
              end_date TEXT NOT NULL,
              sponsor TEXT,
              cooldown_minutes INTEGER NOT NULL DEFAULT 0 CHECK(cooldown_minutes >= 0),
              active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
              UNIQUE(title, start_date, end_date)
            );
            CREATE TABLE IF NOT EXISTS program_regions (
              program_id INTEGER NOT NULL REFERENCES programs(id) ON DELETE CASCADE,
              region TEXT NOT NULL,
              PRIMARY KEY(program_id, region)
            );
            CREATE TABLE IF NOT EXISTS blocked_windows (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              region TEXT NOT NULL,
              weekday INTEGER NOT NULL CHECK(weekday BETWEEN 0 AND 6),
              start_time TEXT NOT NULL,
              end_time TEXT NOT NULL,
              reason TEXT NOT NULL,
              CHECK(start_time < end_time)
            );
            CREATE TABLE IF NOT EXISTS sponsor_policies (
              sponsor TEXT PRIMARY KEY,
              min_gap_minutes INTEGER NOT NULL CHECK(min_gap_minutes >= 0)
            );
            CREATE TABLE IF NOT EXISTS slots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              air_date TEXT NOT NULL,
              start_time TEXT NOT NULL,
              duration_minutes INTEGER NOT NULL CHECK(duration_minutes > 0),
              program_id INTEGER NOT NULL REFERENCES programs(id),
              region TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'planned'
                CHECK(status IN ('planned','replaced','cancelled')),
              replaced_from INTEGER REFERENCES programs(id),
              created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_slots_date_region ON slots(air_date, region);
            CREATE TABLE IF NOT EXISTS playout_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              slot_id INTEGER NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
              actual_start TEXT NOT NULL,
              actual_duration_minutes INTEGER NOT NULL CHECK(actual_duration_minutes >= 0),
              actual_program_id INTEGER REFERENCES programs(id),
              note TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reconciliation_exceptions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              air_date TEXT NOT NULL,
              slot_id INTEGER NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
              kind TEXT NOT NULL,
              detail TEXT NOT NULL,
              created_at TEXT NOT NULL,
              UNIQUE(air_date, slot_id, kind)
            );
            CREATE TABLE IF NOT EXISTS review_batches (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              air_date TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'draft'
                CHECK(status IN ('draft','in_review','confirmed','superseded')),
              version INTEGER NOT NULL DEFAULT 1,
              base_version INTEGER NOT NULL DEFAULT 1,
              note TEXT NOT NULL DEFAULT '',
              created_by TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              confirmed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_review_batches_date
              ON review_batches(air_date, status);
            CREATE TABLE IF NOT EXISTS review_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES review_batches(id) ON DELETE CASCADE,
              slot_id INTEGER NOT NULL REFERENCES slots(id) ON DELETE CASCADE,
              program_id INTEGER NOT NULL,
              region TEXT NOT NULL,
              start_time TEXT NOT NULL,
              duration_minutes INTEGER NOT NULL,
              playable_intervals TEXT NOT NULL DEFAULT '[]',
              blocked INTEGER NOT NULL DEFAULT 0 CHECK(blocked IN (0,1)),
              blocked_reason TEXT NOT NULL DEFAULT '',
              diff TEXT NOT NULL DEFAULT '{}',
              check_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(check_status IN ('pending','confirmed','failed')),
              result TEXT NOT NULL DEFAULT '{}',
              UNIQUE(batch_id, slot_id)
            );
            CREATE INDEX IF NOT EXISTS idx_review_items_batch
              ON review_items(batch_id, check_status);
            """
        )
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after the initial release without touching old rows."""
        cols = [row[1] for row in self.conn.execute("PRAGMA table_info(slots)").fetchall()]
        if "batch_id" not in cols:
            self.conn.execute("ALTER TABLE slots ADD COLUMN batch_id INTEGER REFERENCES review_batches(id)")
            self.conn.commit()

    def seed_demo(self) -> None:
        existing = self.conn.execute("SELECT COUNT(*) FROM programs").fetchone()[0]
        if existing:
            return
        music = self.add_program("晨间轻音乐", "music", 30, "2026-01-01", "2026-12-31", "青柠饮品", 45, ["华东"])
        news = self.add_program("城市早报", "talk", 30, "2026-01-01", "2026-12-31", None, 0, ["华东"])
        ad = self.add_program("青柠饮品广告", "ad", 5, "2026-01-01", "2026-12-31", "青柠饮品", 60, ["华东"])
        self.add_sponsor_policy("青柠饮品", 90)
        self.add_blocked_window("华东", 0, "08:00", "08:30", "周一设备检修")
        self.schedule_slot("2026-09-28", "09:00", music, "华东")
        self.schedule_slot("2026-09-28", "10:00", news, "华东")
        self.schedule_slot("2026-09-28", "11:00", ad, "华东")

    def add_program(self, title: str, kind: str, duration_minutes: int, start_date: str, end_date: str,
                    sponsor: str | None = None, cooldown_minutes: int = 0,
                    regions: list[str] | None = None) -> int:
        if not title.strip():
            raise DomainError("节目名称不能为空")
        if kind not in PROGRAM_KINDS:
            raise DomainError(f"不支持的节目类型: {kind}")
        if duration_minutes <= 0:
            raise DomainError("节目时长必须大于0")
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        if end < start:
            raise DomainError("授权结束日期不能早于开始日期")
        if cooldown_minutes < 0:
            raise DomainError("冷却时间不能为负数")
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO programs(title,kind,duration_minutes,start_date,end_date,sponsor,cooldown_minutes) VALUES(?,?,?,?,?,?,?)",
                (title.strip(), kind, duration_minutes, start_date, end_date, (sponsor or "").strip() or None, cooldown_minutes),
            )
            program_id = int(cur.lastrowid)
            for region in regions or []:
                self.conn.execute("INSERT INTO program_regions(program_id,region) VALUES(?,?)", (program_id, region.strip()))
        return program_id

    def authorize_region(self, program_id: int, region: str) -> None:
        if not region.strip():
            raise DomainError("地区不能为空")
        with self.transaction():
            if not self.conn.execute("SELECT 1 FROM programs WHERE id=?", (program_id,)).fetchone():
                raise DomainError("节目不存在")
            self.conn.execute("INSERT OR IGNORE INTO program_regions(program_id,region) VALUES(?,?)", (program_id, region.strip()))

    def add_sponsor_policy(self, sponsor: str, min_gap_minutes: int) -> None:
        if not sponsor.strip() or min_gap_minutes < 0:
            raise DomainError("赞助商和最小间隔必须有效")
        with self.transaction():
            self.conn.execute(
                "INSERT INTO sponsor_policies(sponsor,min_gap_minutes) VALUES(?,?) "
                "ON CONFLICT(sponsor) DO UPDATE SET min_gap_minutes=excluded.min_gap_minutes",
                (sponsor.strip(), min_gap_minutes),
            )

    def add_blocked_window(self, region: str, weekday: int, start_time: str, end_time: str, reason: str) -> int:
        if weekday not in range(7) or _minutes(start_time) >= _minutes(end_time):
            raise DomainError("禁播时段参数无效")
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO blocked_windows(region,weekday,start_time,end_time,reason) VALUES(?,?,?,?,?)",
                (region.strip(), weekday, start_time, end_time, reason.strip() or "禁播"),
            )
        return int(cur.lastrowid)

    def _validate_slot(self, air_date: str, start_time: str, duration: int, program_id: int,
                       region: str, ignore_slot_id: int | None = None) -> None:
        try:
            day = datetime.strptime(air_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise DomainError("播出日期必须使用 YYYY-MM-DD") from exc
        try:
            _minutes(start_time)
        except ValueError as exc:
            raise DomainError("开始时间必须使用 HH:MM") from exc
        if duration <= 0:
            raise DomainError("排期时长必须大于0")
        program = self.conn.execute("SELECT * FROM programs WHERE id=? AND active=1", (program_id,)).fetchone()
        if not program:
            raise DomainError("节目不存在或未启用")
        if program["duration_minutes"] != duration:
            raise DomainError(f"排期时长必须等于节目时长 {program['duration_minutes']} 分钟")
        if not (program["start_date"] <= air_date <= program["end_date"]):
            raise DomainError("播出日期超出授权窗口")
        if not self.conn.execute(
            "SELECT 1 FROM program_regions WHERE program_id=? AND region=?", (program_id, region)
        ).fetchone():
            raise DomainError(f"节目未授权在{region}播出")
        end_minutes = _minutes(start_time) + duration
        blocked = self.conn.execute(
            "SELECT * FROM blocked_windows WHERE region=? AND weekday=?",
            (region, day.weekday()),
        ).fetchall()
        for window in blocked:
            if _minutes(window["start_time"]) < end_minutes and _minutes(start_time) < _minutes(window["end_time"]):
                raise DomainError(f"与禁播时段冲突: {window['reason']}")
        sql = "SELECT * FROM slots WHERE air_date=? AND region=? AND status!='cancelled'"
        params: list[object] = [air_date, region]
        if ignore_slot_id is not None:
            sql += " AND id!=?"
            params.append(ignore_slot_id)
        for existing in self.conn.execute(sql, params).fetchall():
            if _overlap(start_time, duration, existing["start_time"], existing["duration_minutes"]):
                raise DomainError(f"与排期 #{existing['id']} 时间重叠")
        if program["cooldown_minutes"]:
            previous = self.conn.execute(
                "SELECT * FROM slots WHERE air_date=? AND region=? AND program_id=? AND status!='cancelled' AND id!=? "
                "AND start_time < ? ORDER BY start_time DESC LIMIT 1",
                (air_date, region, program_id, ignore_slot_id or -1, start_time),
            ).fetchone()
            if previous:
                gap = _minutes(start_time) - (_minutes(previous["start_time"]) + previous["duration_minutes"])
                if gap < program["cooldown_minutes"]:
                    raise DomainError(f"与上一期节目间隔不足冷却时间 {program['cooldown_minutes']} 分钟")
        if program["sponsor"]:
            policy = self.conn.execute("SELECT min_gap_minutes FROM sponsor_policies WHERE sponsor=?", (program["sponsor"],)).fetchone()
            if policy:
                gap = policy["min_gap_minutes"]
                all_sponsored = self.conn.execute(
                    "SELECT s.*, p.sponsor FROM slots s JOIN programs p ON p.id=s.program_id "
                    "WHERE s.air_date=? AND s.region=? AND s.status!='cancelled' AND p.sponsor=? AND s.id!=?",
                    (air_date, region, program["sponsor"], ignore_slot_id or -1),
                ).fetchall()
                for other in all_sponsored:
                    if _overlap(start_time, duration, other["start_time"], other["duration_minutes"]):
                        raise DomainError(f"与赞助商 {program['sponsor']} 的其他节目冲突")
                    distance = abs(_minutes(start_time) - (_minutes(other["start_time"]) + other["duration_minutes"]))
                    if distance < gap:
                        raise DomainError(f"与赞助商 {program['sponsor']} 的节目间隔不足 {gap} 分钟")

    def schedule_slot(self, air_date: str, start_time: str, program_id: int, region: str) -> int:
        program = self.conn.execute("SELECT duration_minutes FROM programs WHERE id=?", (program_id,)).fetchone()
        if not program:
            raise DomainError("节目不存在")
        with self.transaction():
            self._validate_slot(air_date, start_time, int(program["duration_minutes"]), program_id, region)
            cur = self.conn.execute(
                "INSERT INTO slots(air_date,start_time,duration_minutes,program_id,region,created_at) VALUES(?,?,?,?,?,?)",
                (air_date, start_time, int(program["duration_minutes"]), program_id, region, datetime.now().isoformat()),
            )
            self._invalidate_reviews(air_date)
        return int(cur.lastrowid)

    def replace_slot(self, slot_id: int, new_program_id: int) -> dict:
        """Replace a planned item and revalidate the resulting plan atomically."""
        with self.transaction():
            slot = self.conn.execute("SELECT * FROM slots WHERE id=? AND status='planned'", (slot_id,)).fetchone()
            if not slot:
                raise DomainError("只能替换尚未播出且状态为 planned 的排期")
            program = self.conn.execute("SELECT * FROM programs WHERE id=?", (new_program_id,)).fetchone()
            if not program:
                raise DomainError("替换节目不存在")
            self._validate_slot(slot["air_date"], slot["start_time"], int(program["duration_minutes"]), new_program_id, slot["region"], slot_id)
            self.conn.execute(
                "UPDATE slots SET program_id=?, duration_minutes=?, replaced_from=?, status='replaced' WHERE id=?",
                (new_program_id, int(program["duration_minutes"]), slot["program_id"], slot_id),
            )
            self._invalidate_reviews(slot["air_date"])
        return self.get_slot(slot_id)

    def get_slot(self, slot_id: int) -> dict:
        row = self.conn.execute(
            "SELECT s.*, p.title, p.kind, p.sponsor FROM slots s JOIN programs p ON p.id=s.program_id WHERE s.id=?",
            (slot_id,),
        ).fetchone()
        if not row:
            raise DomainError("排期不存在")
        return dict(row)

    def record_playout(self, slot_id: int, actual_start: str, actual_duration_minutes: int,
                       actual_program_id: int | None = None, note: str = "") -> int:
        if not self.conn.execute("SELECT 1 FROM slots WHERE id=?", (slot_id,)).fetchone():
            raise DomainError("排期不存在")
        if actual_duration_minutes < 0:
            raise DomainError("实际时长不能为负数")
        _minutes(actual_start)
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO playout_logs(slot_id,actual_start,actual_duration_minutes,actual_program_id,note,created_at) VALUES(?,?,?,?,?,?)",
                (slot_id, actual_start, actual_duration_minutes, actual_program_id, note, datetime.now().isoformat()),
            )
        return int(cur.lastrowid)

    def reconcile_date(self, air_date: str) -> list[dict]:
        """Compare the latest playout per slot with the plan and persist exceptions."""
        try:
            datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        with self.transaction():
            self.conn.execute("DELETE FROM reconciliation_exceptions WHERE air_date=?", (air_date,))
            slots = self.conn.execute(
                "SELECT s.*, p.title, p.sponsor, p.kind FROM slots s JOIN programs p ON p.id=s.program_id "
                "WHERE s.air_date=? AND s.status!='cancelled' ORDER BY s.start_time", (air_date,)
            ).fetchall()
            exceptions: list[tuple[int, str, str]] = []
            for slot in slots:
                log = self.conn.execute(
                    "SELECT * FROM playout_logs WHERE slot_id=? ORDER BY id DESC LIMIT 1", (slot["id"],)
                ).fetchone()
                if not log:
                    exceptions.append((slot["id"], "missed", "没有实播记录"))
                    continue
                actual_program_id = log["actual_program_id"] or slot["program_id"]
                if actual_program_id != slot["program_id"]:
                    exceptions.append((slot["id"], "wrong_program", f"计划节目 #{slot['program_id']}，实播节目 #{actual_program_id}"))
                delta = log["actual_duration_minutes"] - slot["duration_minutes"]
                if abs(delta) > 30:
                    kind = "overrun" if delta > 0 else "underrun"
                    exceptions.append((slot["id"], kind, f"与计划相差 {delta:+d} 分钟"))
                actual = self.conn.execute(
                    "SELECT p.* FROM programs p WHERE p.id=?", (actual_program_id,)
                ).fetchone()
                if actual:
                    region_ok = self.conn.execute(
                        "SELECT 1 FROM program_regions WHERE program_id=? AND region=?", (actual_program_id, slot["region"])
                    ).fetchone()
                    if not region_ok or not (actual["start_date"] <= air_date <= actual["end_date"]):
                        exceptions.append((slot["id"], "out_of_license", "实播节目超出地区或日期授权"))
            for slot_id, kind, detail in exceptions:
                self.conn.execute(
                    "INSERT INTO reconciliation_exceptions(air_date,slot_id,kind,detail,created_at) VALUES(?,?,?,?,?)",
                    (air_date, slot_id, kind, detail, datetime.now().isoformat()),
                )
        return self.get_exceptions(air_date)

    def get_exceptions(self, air_date: str) -> list[dict]:
        return [dict(row) for row in self.conn.execute(
            "SELECT * FROM reconciliation_exceptions WHERE air_date=? ORDER BY slot_id, kind", (air_date,)
        ).fetchall()]

    # ------------------------------------------------------------------
    # 复核批次（review batch）
    #
    # 节目授权、排期替换、实播记录三处数据通过“复核批次”串成一条可恢复的
    # 复核链路：排期或替换一变，授权窗口立即失效重算；两人同时提交同一
    # 批次时只留下先到结果，后到者保存草稿与差异；写入失败后从最后确认项
    # 继续；旧数据没有批次号时回填初始版本。
    # ------------------------------------------------------------------

    def _invalidate_reviews(self, air_date: str) -> None:
        """排期或替换一变，该日期所有未决/已确认批次立即失效。"""
        now = datetime.now().isoformat()
        self.conn.execute(
            "UPDATE review_batches SET status='superseded', updated_at=? "
            "WHERE air_date=? AND status IN ('draft','in_review','confirmed')",
            (now, air_date),
        )

    def backfill_batches(self) -> int:
        """给没有批次号的旧排期回填初始版本（version=1 的已确认批次）。

        返回新建批次数量。幂等：已归属批次的排期不会重复处理。
        """
        rows = self.conn.execute(
            "SELECT DISTINCT air_date FROM slots WHERE batch_id IS NULL ORDER BY air_date"
        ).fetchall()
        created = 0
        now = datetime.now().isoformat()
        for row in rows:
            air_date = row["air_date"]
            with self.transaction():
                cur = self.conn.execute(
                    "INSERT INTO review_batches(air_date,status,version,base_version,note,created_by,created_at,updated_at,confirmed_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (air_date, "confirmed", 1, 1, "旧数据回填初始版本", "system", now, now, now),
                )
                batch_id = int(cur.lastrowid)
                slots = self.conn.execute(
                    "SELECT * FROM slots WHERE air_date=? AND batch_id IS NULL ORDER BY start_time",
                    (air_date,),
                ).fetchall()
                for slot in slots:
                    self.conn.execute("UPDATE slots SET batch_id=? WHERE id=?", (batch_id, slot["id"]))
                    computed = self._compute_review_item(slot)
                    self.conn.execute(
                        "INSERT INTO review_items(batch_id,slot_id,program_id,region,start_time,duration_minutes,"
                        "playable_intervals,blocked,blocked_reason,diff,check_status,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (batch_id, slot["id"], slot["program_id"], slot["region"], slot["start_time"],
                         slot["duration_minutes"], json.dumps(computed["playable_intervals"], ensure_ascii=False),
                         1 if computed["blocked"] else 0, computed["blocked_reason"], "{}", "confirmed",
                         json.dumps(computed["result"], ensure_ascii=False)),
                    )
                created += 1
        return created

    def _playable_intervals(self, region: str, weekday: int) -> list[list[str]]:
        """返回该地区该星期几允许播出的时间区间（一天减去禁播时段）。"""
        windows = self.conn.execute(
            "SELECT start_time, end_time FROM blocked_windows WHERE region=? AND weekday=? ORDER BY start_time",
            (region, weekday),
        ).fetchall()
        free: list[list[str]] = []
        cursor = 0
        day_end = 24 * 60
        for window in windows:
            start = _minutes(window["start_time"])
            end = _minutes(window["end_time"])
            if start > cursor:
                free.append([_fmt_minutes(cursor), _fmt_minutes(start)])
            cursor = max(cursor, end)
        if cursor < day_end:
            free.append([_fmt_minutes(cursor), _fmt_minutes(day_end)])
        return free

    def _blocking_reason(self, region: str, weekday: int, slot_start: int, slot_end: int) -> str:
        windows = self.conn.execute(
            "SELECT start_time, end_time, reason FROM blocked_windows WHERE region=? AND weekday=?",
            (region, weekday),
        ).fetchall()
        for window in windows:
            start = _minutes(window["start_time"])
            end = _minutes(window["end_time"])
            if start < slot_end and slot_start < end:
                return f"与禁播时段冲突: {window['reason']}"
        return "不在可播区间"

    def _compute_review_item(self, slot: sqlite3.Row) -> dict:
        """计算一个排期条目的可播区间与阻断原因（授权窗口 + 排期校验 + 实播对照）。"""
        air_date = slot["air_date"]
        start_time = slot["start_time"]
        duration = slot["duration_minutes"]
        program_id = slot["program_id"]
        region = slot["region"]
        result: dict = {
            "slot_id": slot["id"], "air_date": air_date, "start_time": start_time,
            "duration_minutes": duration, "region": region, "program_id": program_id,
        }
        blocked = False
        reasons: list[str] = []

        program = self.conn.execute("SELECT * FROM programs WHERE id=?", (program_id,)).fetchone()
        if not program:
            return {"blocked": True, "blocked_reason": "节目不存在",
                    "playable_intervals": [], "result": result}

        if not (program["start_date"] <= air_date <= program["end_date"]):
            blocked = True
            reasons.append("播出日期超出授权窗口")
        if not self.conn.execute(
            "SELECT 1 FROM program_regions WHERE program_id=? AND region=?", (program_id, region)
        ).fetchone():
            blocked = True
            reasons.append(f"节目未授权在{region}播出")

        day = datetime.strptime(air_date, "%Y-%m-%d").date()
        weekday = day.weekday()
        intervals = self._playable_intervals(region, weekday)
        result["playable_intervals"] = intervals

        slot_start = _minutes(start_time)
        slot_end = slot_start + duration
        in_interval = any(
            slot_start >= _minutes(iv[0]) and slot_end <= _minutes(iv[1]) for iv in intervals
        )
        if not in_interval:
            blocked = True
            reasons.append(self._blocking_reason(region, weekday, slot_start, slot_end))

        for other in self.conn.execute(
            "SELECT * FROM slots WHERE air_date=? AND region=? AND status!='cancelled' AND id!=?",
            (air_date, region, slot["id"]),
        ).fetchall():
            if _overlap(start_time, duration, other["start_time"], other["duration_minutes"]):
                blocked = True
                reasons.append(f"与排期 #{other['id']} 时间重叠")
                break

        if program["cooldown_minutes"]:
            previous = self.conn.execute(
                "SELECT * FROM slots WHERE air_date=? AND region=? AND program_id=? AND status!='cancelled' "
                "AND id!=? AND start_time < ? ORDER BY start_time DESC LIMIT 1",
                (air_date, region, program_id, slot["id"], start_time),
            ).fetchone()
            if previous:
                gap = _minutes(start_time) - (_minutes(previous["start_time"]) + previous["duration_minutes"])
                if gap < program["cooldown_minutes"]:
                    blocked = True
                    reasons.append(f"与上一期节目间隔不足冷却时间 {program['cooldown_minutes']} 分钟")

        if program["sponsor"]:
            policy = self.conn.execute(
                "SELECT min_gap_minutes FROM sponsor_policies WHERE sponsor=?", (program["sponsor"],)
            ).fetchone()
            if policy:
                gap = policy["min_gap_minutes"]
                sponsored = self.conn.execute(
                    "SELECT s.* FROM slots s JOIN programs p ON p.id=s.program_id "
                    "WHERE s.air_date=? AND s.region=? AND s.status!='cancelled' AND p.sponsor=? AND s.id!=?",
                    (air_date, region, program["sponsor"], slot["id"]),
                ).fetchall()
                for other in sponsored:
                    if _overlap(start_time, duration, other["start_time"], other["duration_minutes"]):
                        blocked = True
                        reasons.append(f"与赞助商 {program['sponsor']} 的其他节目冲突")
                        break
                    distance = abs(_minutes(start_time) - (_minutes(other["start_time"]) + other["duration_minutes"]))
                    if distance < gap:
                        blocked = True
                        reasons.append(f"与赞助商 {program['sponsor']} 的节目间隔不足 {gap} 分钟")
                        break

        log = self.conn.execute(
            "SELECT * FROM playout_logs WHERE slot_id=? ORDER BY id DESC LIMIT 1", (slot["id"],)
        ).fetchone()
        if not log:
            result["playout_status"] = "missing"
        else:
            actual_program_id = log["actual_program_id"] or slot["program_id"]
            result["playout_status"] = "recorded"
            result["actual_program_id"] = actual_program_id
            result["actual_duration_minutes"] = log["actual_duration_minutes"]
            if actual_program_id != slot["program_id"]:
                reasons.append(f"实播节目 #{actual_program_id} 与计划节目 #{slot['program_id']} 不一致")
            delta = log["actual_duration_minutes"] - slot["duration_minutes"]
            if abs(delta) > 30:
                reasons.append(f"实播时长与计划相差 {delta:+d} 分钟")

        result["blocked"] = blocked
        result["blocked_reasons"] = reasons
        return {
            "blocked": blocked,
            "blocked_reason": "；".join(reasons) if reasons else "",
            "playable_intervals": intervals,
            "result": result,
        }

    def _insert_review_item(self, batch_id: int, slot: sqlite3.Row, now: str) -> None:
        computed = self._compute_review_item(slot)
        self.conn.execute(
            "INSERT INTO review_items(batch_id,slot_id,program_id,region,start_time,duration_minutes,"
            "playable_intervals,blocked,blocked_reason,diff,check_status,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, slot["id"], slot["program_id"], slot["region"], slot["start_time"],
             slot["duration_minutes"], json.dumps(computed["playable_intervals"], ensure_ascii=False),
             1 if computed["blocked"] else 0, computed["blocked_reason"], "{}", "pending",
             json.dumps(computed["result"], ensure_ascii=False)),
        )

    def initiate_review(self, air_date: str, note: str = "", created_by: str = "") -> dict:
        """发起一个复核批次：快照当日全部排期，逐条计算可播区间与阻断原因。

        同一日期若已有未决批次，先置为失效，保证同一时间只有一个活动草稿。
        """
        try:
            datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        with self.transaction():
            self._invalidate_reviews(air_date)
            now = datetime.now().isoformat()
            cur = self.conn.execute(
                "INSERT INTO review_batches(air_date,status,version,base_version,note,created_by,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (air_date, "draft", 1, 1, note, created_by, now, now),
            )
            batch_id = int(cur.lastrowid)
            slots = self.conn.execute(
                "SELECT * FROM slots WHERE air_date=? AND status!='cancelled' ORDER BY start_time",
                (air_date,),
            ).fetchall()
            for slot in slots:
                self._insert_review_item(batch_id, slot, now)
        return self.get_review(batch_id)

    def get_review(self, batch_id: int) -> dict:
        batch = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("复核批次不存在")
        items = [dict(row) for row in self.conn.execute(
            "SELECT ri.*, p.title AS program_title, p.kind AS program_kind "
            "FROM review_items ri LEFT JOIN programs p ON p.id=ri.program_id "
            "WHERE ri.batch_id=? ORDER BY ri.slot_id", (batch_id,)
        ).fetchall()]
        for item in items:
            item["playable_intervals"] = json.loads(item["playable_intervals"])
            item["diff"] = json.loads(item["diff"])
            item["result"] = json.loads(item["result"])
        out = dict(batch)
        out["items"] = items
        out["confirmed_count"] = sum(1 for i in items if i["check_status"] == "confirmed")
        out["failed_count"] = sum(1 for i in items if i["check_status"] == "failed")
        out["total_count"] = len(items)
        return out

    def list_reviews(self, air_date: str | None = None) -> list[dict]:
        if air_date:
            try:
                datetime.strptime(air_date, "%Y-%m-%d")
            except ValueError as exc:
                raise DomainError("日期必须使用 YYYY-MM-DD") from exc
            rows = self.conn.execute(
                "SELECT * FROM review_batches WHERE air_date=? ORDER BY id DESC", (air_date,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM review_batches ORDER BY id DESC LIMIT 100"
            ).fetchall()
        return [dict(row) for row in rows]

    def confirm_review(self, batch_id: int, expected_version: int, confirmed_by: str = "") -> dict:
        """确认复核结果。

        乐观锁：只有版本仍为 expected_version 的未决批次能被确认，先到先得。
        后到者不覆盖先到结果，而是把自己的计算结果与差异存成新草稿。
        逐条确认、每条独立提交；失败后重试时从最后确认项继续。
        """
        batch = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("复核批次不存在")
        if batch["status"] == "superseded":
            raise DomainError("该复核批次已失效，请重新发起复核")
        now = datetime.now().isoformat()

        if batch["status"] == "confirmed":
            if batch["version"] != expected_version:
                return self._save_loser_draft(batch_id, expected_version, confirmed_by, now)
            self._process_review_items(batch_id)
            return {"status": "confirmed", "batch": self.get_review(batch_id)}

        with self.transaction():
            cur = self.conn.execute(
                "UPDATE review_batches SET status='confirmed', version=version+1, confirmed_at=?, updated_at=? "
                "WHERE id=? AND version=? AND status IN ('draft','in_review')",
                (now, now, batch_id, expected_version),
            )
            won = cur.rowcount == 1

        if not won:
            return self._save_loser_draft(batch_id, expected_version, confirmed_by, now)

        self._process_review_items(batch_id)
        return {"status": "confirmed", "batch": self.get_review(batch_id)}

    def _save_loser_draft(self, original_batch_id: int, expected_version: int,
                           confirmed_by: str, now: str) -> dict:
        """后到者：保存一份新草稿，内容为自己的计算结果及与先到批次的差异。"""
        original = self.get_review(original_batch_id)
        winner = self.conn.execute(
            "SELECT * FROM review_batches WHERE air_date=? AND status='confirmed' ORDER BY id DESC LIMIT 1",
            (original["air_date"],),
        ).fetchone()
        winner_items: dict[int, sqlite3.Row] = {}
        if winner:
            for row in self.conn.execute(
                "SELECT * FROM review_items WHERE batch_id=?", (winner["id"],)
            ).fetchall():
                winner_items[row["slot_id"]] = row

        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO review_batches(air_date,status,version,base_version,note,created_by,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (original["air_date"], "draft", 1, expected_version,
                 f"与批次 #{winner['id'] if winner else '?'} 并发提交，保存草稿与差异",
                 confirmed_by, now, now),
            )
            draft_id = int(cur.lastrowid)
            for item in original["items"]:
                diff: dict = {}
                counterpart = winner_items.get(item["slot_id"])
                if counterpart is not None:
                    diff = self._item_diff(item, counterpart)
                self.conn.execute(
                    "INSERT INTO review_items(batch_id,slot_id,program_id,region,start_time,duration_minutes,"
                    "playable_intervals,blocked,blocked_reason,diff,check_status,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (draft_id, item["slot_id"], item["program_id"], item["region"], item["start_time"],
                     item["duration_minutes"], json.dumps(item["playable_intervals"], ensure_ascii=False),
                     1 if item["blocked"] else 0, item["blocked_reason"],
                     json.dumps(diff, ensure_ascii=False), "pending",
                     json.dumps(item["result"], ensure_ascii=False)),
                )
        return {
            "status": "superseded",
            "winner_batch_id": winner["id"] if winner else None,
            "draft_batch_id": draft_id,
            "batch": self.get_review(draft_id),
        }

    def _item_diff(self, loser: dict, winner: sqlite3.Row) -> dict:
        diff: dict = {}
        if loser["blocked"] != winner["blocked"]:
            diff["blocked"] = [winner["blocked"], loser["blocked"]]
        if loser["blocked_reason"] != winner["blocked_reason"]:
            diff["blocked_reason"] = [winner["blocked_reason"], loser["blocked_reason"]]
        winner_intervals = json.loads(winner["playable_intervals"])
        if loser["playable_intervals"] != winner_intervals:
            diff["playable_intervals"] = [winner_intervals, loser["playable_intervals"]]
        return diff

    def _process_review_items(self, batch_id: int) -> None:
        """逐条确认未决条目，每条独立提交；失败后重试时跳过已确认项继续。"""
        items = self.conn.execute(
            "SELECT * FROM review_items WHERE batch_id=? AND check_status!='confirmed' ORDER BY slot_id",
            (batch_id,),
        ).fetchall()
        for item in items:
            try:
                with self.transaction():
                    slot = self.conn.execute(
                        "SELECT * FROM slots WHERE id=?", (item["slot_id"],)
                    ).fetchone()
                    if slot and slot["status"] != "cancelled":
                        computed = self._compute_review_item(slot)
                        self.conn.execute(
                            "UPDATE review_items SET playable_intervals=?, blocked=?, blocked_reason=?, "
                            "result=?, check_status='confirmed' WHERE id=?",
                            (json.dumps(computed["playable_intervals"], ensure_ascii=False),
                             1 if computed["blocked"] else 0, computed["blocked_reason"],
                             json.dumps(computed["result"], ensure_ascii=False), item["id"]),
                        )
                    else:
                        self.conn.execute(
                            "UPDATE review_items SET check_status='confirmed', blocked=1, blocked_reason=? WHERE id=?",
                            ("排期已取消或删除", item["id"]),
                        )
            except Exception:
                try:
                    with self.transaction():
                        self.conn.execute(
                            "UPDATE review_items SET check_status='failed' WHERE id=?", (item["id"],)
                        )
                except Exception:
                    pass
                raise

    def retry_review(self, batch_id: int) -> dict:
        """重试一个已确认批次：跳过已确认项，从最后确认项继续处理未决/失败条目。"""
        batch = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("复核批次不存在")
        if batch["status"] == "superseded":
            raise DomainError("该复核批次已失效，请重新发起复核")
        self._process_review_items(batch_id)
        return {"status": self.get_review(batch_id)["status"], "batch": self.get_review(batch_id)}

    def snapshot(self) -> dict:
        programs = [dict(row) for row in self.conn.execute("SELECT * FROM programs ORDER BY id").fetchall()]
        slots = [dict(row) for row in self.conn.execute(
            "SELECT s.*, p.title, p.kind FROM slots s JOIN programs p ON p.id=s.program_id ORDER BY s.air_date,s.start_time"
        ).fetchall()]
        reviews = [dict(row) for row in self.conn.execute(
            "SELECT * FROM review_batches ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        return {"programs": programs, "slots": slots, "reviews": reviews,
                "exceptions": [dict(row) for row in self.conn.execute(
            "SELECT * FROM reconciliation_exceptions ORDER BY id DESC LIMIT 50"
        ).fetchall()]}
