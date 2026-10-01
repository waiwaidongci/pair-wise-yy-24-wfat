from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime


class DomainError(ValueError):
    """A business-rule violation that should be shown to the API caller."""


PROGRAM_KINDS = {"music", "ad", "talk", "live"}
WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

PLAN_VERSION_KEY = "plan_version"
BATCH_STATUSES = ("open", "confirmed", "draft")


def _minutes(value: str) -> int:
    parsed = datetime.strptime(value, "%H:%M")
    return parsed.hour * 60 + parsed.minute


def _hm(value: int) -> str:
    if value >= 1440:
        return "24:00"
    return f"{value // 60:02d}:{value % 60:02d}"


def _overlap(a_start: str, a_duration: int, b_start: str, b_duration: int) -> bool:
    start_a, start_b = _minutes(a_start), _minutes(b_start)
    return start_a < start_b + b_duration and start_b < start_a + a_duration


class RadioDB:
    """SQLite-backed radio scheduling service.

    授权、排期/替换、实播三处数据通过“复核批次”(review batch) 串起来：

    * 每次排期或替换都会抬升全局 plan_version，当天进行中的批次立即按新计划
      重算授权窗口与对账结果；结果指纹未变的已确认项保留，变了的回退为待确认。
    * 批次提交采用状态 CAS：先到者把 open 置为 confirmed，后到者原样保存为
      draft，并记录与胜出结果的逐项差异。
    * 逐项确认各自提交并把 last_item_seq 作为检查点；写入失败重试时只处理
      仍处于 pending 的项，从最后确认项之后继续。
    * 启动时把没有批次号的历史对账异常回填为 legacy 的初始版本批次。
    """

    def __init__(self, path: str = "radio.db") -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
        # 测试用故障注入：>0 时下一次提交被回滚并抛出 OperationalError
        self._crash_writes = 0
        # HTTP 服务多线程共享同一连接，用进程内锁把写事务串行化，
        # 避免在线程 A 的 BEGIN IMMEDIATE 内再开事务
        self._write_lock = threading.RLock()
        self._schema()
        self._migrate()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        # RLock 允许同一线程内嵌套进入（外层方法持锁调用内层 helper），
        # 不同线程在此排队，保证连接上任意时刻只有一个写事务
        with self._write_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                if self._crash_writes > 0:
                    # 模拟提交前磁盘写入失败：回滚后抛出，外层可安全重试
                    self._crash_writes -= 1
                    self.conn.rollback()
                    raise sqlite3.OperationalError("模拟写入失败，需要重试")
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def _retry_commit(self, action, attempts: int = 3):
        """运行带事务的 action；遇到临时写入失败时重试。

        action 内部必须幂等且自行从当前状态续算（例如只处理 pending 项），
        这样多次执行不会重复写入。
        """
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return action()
            except sqlite3.OperationalError as exc:
                last_error = exc
                if attempt == attempts - 1:
                    raise
                time.sleep(0.02 * (attempt + 1))
        raise last_error  # pragma: no cover

    # ---------------------------------------------------------------- schema

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
              batch_id INTEGER,
              item_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS review_batches (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_no TEXT NOT NULL UNIQUE,
              air_date TEXT NOT NULL,
              region TEXT,
              status TEXT NOT NULL CHECK(status IN ('open','confirmed','draft')),
              version_no INTEGER NOT NULL DEFAULT 1,
              plan_version INTEGER NOT NULL DEFAULT 1,
              revision INTEGER NOT NULL DEFAULT 0,
              based_on_batch_id INTEGER REFERENCES review_batches(id),
              created_by TEXT NOT NULL DEFAULT '',
              confirmed_by TEXT,
              note TEXT NOT NULL DEFAULT '',
              legacy INTEGER NOT NULL DEFAULT 0 CHECK(legacy IN (0,1)),
              last_item_seq INTEGER NOT NULL DEFAULT 0,
              recompute_count INTEGER NOT NULL DEFAULT 0,
              diff_json TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              confirmed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_batches_date ON review_batches(air_date);
            CREATE TABLE IF NOT EXISTS review_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              batch_id INTEGER NOT NULL REFERENCES review_batches(id) ON DELETE CASCADE,
              slot_id INTEGER NOT NULL REFERENCES slots(id),
              seq INTEGER NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','confirmed')),
              plan_program_id INTEGER,
              actual_program_id INTEGER,
              window_json TEXT NOT NULL,
              blockers_json TEXT NOT NULL,
              exceptions_json TEXT NOT NULL,
              result_json TEXT,
              fingerprint TEXT NOT NULL,
              UNIQUE(batch_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_items_batch_slot ON review_items(batch_id, slot_id);
            """
        )
        self.conn.commit()

    def _migrate(self) -> None:
        """给旧库补批次列、移除按日期的唯一约束，并回填无批次号的历史结果。"""
        # 旧库的 UNIQUE(air_date,slot_id,kind) 会挡住同日多批次版本，重建该表
        sql = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reconciliation_exceptions'"
        ).fetchone()
        if sql and "UNIQUE(air_date, slot_id, kind)" in sql["sql"].replace("\n", " "):
            self.conn.executescript(
                """
                ALTER TABLE reconciliation_exceptions RENAME TO reconciliation_exceptions_old;
                CREATE TABLE reconciliation_exceptions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  air_date TEXT NOT NULL,
                  slot_id INTEGER NOT NULL,
                  kind TEXT NOT NULL,
                  detail TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  batch_id INTEGER,
                  item_id INTEGER
                );
                INSERT INTO reconciliation_exceptions(id,air_date,slot_id,kind,detail,created_at)
                SELECT id,air_date,slot_id,kind,detail,created_at FROM reconciliation_exceptions_old;
                DROP TABLE reconciliation_exceptions_old;
                CREATE INDEX IF NOT EXISTS idx_exceptions_batch ON reconciliation_exceptions(batch_id);
                """
            )
            self.conn.commit()
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(reconciliation_exceptions)")}
        if "batch_id" not in cols:
            self.conn.execute("ALTER TABLE reconciliation_exceptions ADD COLUMN batch_id INTEGER")
        if "item_id" not in cols:
            self.conn.execute("ALTER TABLE reconciliation_exceptions ADD COLUMN item_id INTEGER")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exceptions_batch ON reconciliation_exceptions(batch_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_batches_date ON review_batches(air_date)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_items_batch_slot ON review_items(batch_id, slot_id)"
        )
        self.conn.commit()
        if self.conn.execute(
            "SELECT 1 FROM meta WHERE key=?", (PLAN_VERSION_KEY,)
        ).fetchone() is None:
            with self.transaction():
                self.conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?)", (PLAN_VERSION_KEY, "1")
                )
        self._backfill_legacy_batches()

    def _backfill_legacy_batches(self) -> None:
        dates = [row[0] for row in self.conn.execute(
            "SELECT DISTINCT air_date FROM reconciliation_exceptions WHERE batch_id IS NULL"
        ).fetchall()]
        for air_date in dates:
            with self.transaction():
                # 同一事务内二次确认，避免与并发回填重复
                if self.conn.execute(
                    "SELECT 1 FROM reconciliation_exceptions "
                    "WHERE air_date=? AND batch_id IS NULL LIMIT 1", (air_date,)
                ).fetchone() is None:
                    continue
                now = datetime.now().isoformat()
                batch_id = self._insert_batch(
                    air_date, None, version_no=1, plan_version=1, status="confirmed",
                    note="历史对账数据回填的初始版本", legacy=1, created_at=now,
                )
                self.conn.execute(
                    "UPDATE review_batches SET confirmed_by='系统回填', confirmed_at=? WHERE id=?",
                    (now, batch_id),
                )
                spec = self._compute_spec(air_date, None)
                legacy = {}
                for row in self.conn.execute(
                    "SELECT slot_id, kind, detail FROM reconciliation_exceptions "
                    "WHERE air_date=? AND batch_id IS NULL", (air_date,)
                ).fetchall():
                    legacy.setdefault(row["slot_id"], []).append(
                        {"kind": row["kind"], "detail": row["detail"]}
                    )
                for entry in spec:
                    entry["exceptions"] = legacy.get(entry["slot_id"], [])
                self._write_items(batch_id, spec, legacy=True)
                # 旧行已复制进批次，删掉无批次号的游离行
                self.conn.execute(
                    "DELETE FROM reconciliation_exceptions WHERE air_date=? AND batch_id IS NULL",
                    (air_date,),
                )
                self._checkpoint(batch_id)

    # ------------------------------------------------------------- programs

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
                self.conn.execute("INSERT OR IGNORE INTO program_regions(program_id,region) VALUES(?,?)", (program_id, region.strip()))
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

    # --------------------------------------------------------- validation

    def _rule_issues(self, day, start_time: str, duration: int, program, region: str,
                     ignore_slot_id: int | None = None, allow_inactive: bool = False) -> list[str]:
        """收集一个排期位置的全部阻断原因；顺序即校验优先级。"""
        issues: list[str] = []
        air_date = day.strftime("%Y-%m-%d")
        if program is None:
            return ["节目不存在或未启用"]
        if not allow_inactive and program["active"] != 1:
            return ["节目不存在或未启用"]
        if program["active"] != 1:
            issues.append("节目已停用")
        if duration <= 0:
            issues.append("排期时长必须大于0")
        if program["duration_minutes"] != duration:
            issues.append(f"排期时长必须等于节目时长 {program['duration_minutes']} 分钟")
        if not (program["start_date"] <= air_date <= program["end_date"]):
            issues.append(
                f"播出日期 {air_date} 超出授权窗口 {program['start_date']} ~ {program['end_date']}"
            )
        region_ok = self.conn.execute(
            "SELECT 1 FROM program_regions WHERE program_id=? AND region=?",
            (program["id"], region),
        ).fetchone()
        if not region_ok:
            issues.append(f"节目未授权在{region}播出")
        end_minutes = _minutes(start_time) + duration
        for window in self.conn.execute(
            "SELECT * FROM blocked_windows WHERE region=? AND weekday=?",
            (region, day.weekday()),
        ).fetchall():
            if _minutes(window["start_time"]) < end_minutes and _minutes(start_time) < _minutes(window["end_time"]):
                issues.append(f"与禁播时段冲突: {window['reason']}")
        sql = "SELECT * FROM slots WHERE air_date=? AND region=? AND status!='cancelled'"
        params: list[object] = [air_date, region]
        if ignore_slot_id is not None:
            sql += " AND id!=?"
            params.append(ignore_slot_id)
        for existing in self.conn.execute(sql, params).fetchall():
            if _overlap(start_time, duration, existing["start_time"], existing["duration_minutes"]):
                issues.append(f"与排期 #{existing['id']} 时间重叠")
        if program["cooldown_minutes"]:
            previous = self.conn.execute(
                "SELECT * FROM slots WHERE air_date=? AND region=? AND program_id=? AND status!='cancelled' AND id!=? "
                "AND start_time < ? ORDER BY start_time DESC LIMIT 1",
                (air_date, region, program["id"], ignore_slot_id or -1, start_time),
            ).fetchone()
            if previous:
                gap = _minutes(start_time) - (_minutes(previous["start_time"]) + previous["duration_minutes"])
                if gap < program["cooldown_minutes"]:
                    issues.append(f"与上一期节目间隔不足冷却时间 {program['cooldown_minutes']} 分钟")
        if program["sponsor"]:
            policy = self.conn.execute(
                "SELECT min_gap_minutes FROM sponsor_policies WHERE sponsor=?", (program["sponsor"],)
            ).fetchone()
            if policy:
                gap = policy["min_gap_minutes"]
                all_sponsored = self.conn.execute(
                    "SELECT s.*, p.sponsor FROM slots s JOIN programs p ON p.id=s.program_id "
                    "WHERE s.air_date=? AND s.region=? AND s.status!='cancelled' AND p.sponsor=? AND s.id!=?",
                    (air_date, region, program["sponsor"], ignore_slot_id or -1),
                ).fetchall()
                for other in all_sponsored:
                    if _overlap(start_time, duration, other["start_time"], other["duration_minutes"]):
                        issues.append(f"与赞助商 {program['sponsor']} 的其他节目冲突")
                        continue
                    distance = abs(_minutes(start_time) - (_minutes(other["start_time"]) + other["duration_minutes"]))
                    if distance < gap:
                        issues.append(f"与赞助商 {program['sponsor']} 的节目间隔不足 {gap} 分钟")
        return issues

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
        program = self.conn.execute(
            "SELECT * FROM programs WHERE id=? AND active=1", (program_id,)
        ).fetchone()
        if not program:
            raise DomainError("节目不存在或未启用")
        issues = self._rule_issues(day, start_time, duration, program, region, ignore_slot_id)
        if issues:
            raise DomainError(issues[0])

    def _day_intervals(self, weekday: int, region: str) -> list[tuple[int, int]]:
        """当天可播区间：00:00-24:00 减去该地区当天全部禁播时段。"""
        ranges: list[tuple[int, int]] = [(0, 1440)]
        blocks = self.conn.execute(
            "SELECT start_time,end_time FROM blocked_windows WHERE region=? AND weekday=? ORDER BY start_time",
            (region, weekday),
        ).fetchall()
        for block in blocks:
            bs, be = _minutes(block["start_time"]), _minutes(block["end_time"])
            remaining: list[tuple[int, int]] = []
            for a, b in ranges:
                if be <= a or bs >= b:
                    remaining.append((a, b))
                    continue
                if bs > a:
                    remaining.append((a, min(bs, b)))
                if be < b:
                    remaining.append((max(be, a), b))
            ranges = [(a, b) for a, b in remaining if b > a]
        return ranges

    def evaluate_slot(self, slot) -> dict:
        """返回某个排期节目的授权窗口、当天可播区间与阻断原因。"""
        air_date = slot["air_date"]
        day = datetime.strptime(air_date, "%Y-%m-%d").date()
        program = self.conn.execute("SELECT * FROM programs WHERE id=?", (slot["program_id"],)).fetchone()
        issues = self._rule_issues(
            day, slot["start_time"], slot["duration_minutes"], program, slot["region"],
            ignore_slot_id=slot["id"], allow_inactive=True,
        )
        intervals = [
            {"start": _hm(a), "end": _hm(b)}
            for a, b in self._day_intervals(day.weekday(), slot["region"])
        ]
        region_authorized = bool(self.conn.execute(
            "SELECT 1 FROM program_regions WHERE program_id=? AND region=?",
            (slot["program_id"], slot["region"]),
        ).fetchone())
        return {
            "program_id": slot["program_id"],
            "title": program["title"] if program else None,
            "license_start": program["start_date"] if program else None,
            "license_end": program["end_date"] if program else None,
            "region": slot["region"],
            "region_authorized": region_authorized,
            "weekday": day.weekday(),
            "intervals": intervals,
            "playable": not issues,
            "blockers": issues,
        }

    def program_window(self, program_id: int, air_date: str, region: str,
                       start_time: str | None = None) -> dict:
        program = self.conn.execute("SELECT * FROM programs WHERE id=?", (program_id,)).fetchone()
        if not program:
            raise DomainError("节目不存在")
        try:
            day = datetime.strptime(air_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        if not region.strip():
            raise DomainError("地区不能为空")
        intervals = [
            {"start": _hm(a), "end": _hm(b)}
            for a, b in self._day_intervals(day.weekday(), region.strip())
        ]
        blockers: list[str] = []
        if not (program["start_date"] <= air_date <= program["end_date"]):
            blockers.append(f"播出日期 {air_date} 超出授权窗口 {program['start_date']} ~ {program['end_date']}")
        if not self.conn.execute(
            "SELECT 1 FROM program_regions WHERE program_id=? AND region=?", (program_id, region.strip())
        ).fetchone():
            blockers.append(f"节目未授权在{region.strip()}播出")
        if start_time:
            end_minutes = _minutes(start_time) + program["duration_minutes"]
            for window in self.conn.execute(
                "SELECT * FROM blocked_windows WHERE region=? AND weekday=?",
                (region.strip(), day.weekday()),
            ).fetchall():
                if _minutes(window["start_time"]) < end_minutes and _minutes(start_time) < _minutes(window["end_time"]):
                    blockers.append(f"与禁播时段冲突: {window['reason']}")
        return {
            "program_id": program_id,
            "title": program["title"],
            "license_start": program["start_date"],
            "license_end": program["end_date"],
            "region": region.strip(),
            "region_authorized": not any("未授权" in b for b in blockers),
            "weekday": day.weekday(),
            "intervals": intervals,
            "playable": not blockers,
            "blockers": blockers,
        }

    # ------------------------------------------------------------- schedule

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
            slot_id = int(cur.lastrowid)
            # 计划一变：抬升版本并立即重算当天进行中的复核批次
            self._bump_plan_version()
            self._refresh_open_batches(air_date)
        return slot_id

    def replace_slot(self, slot_id: int, new_program_id: int) -> dict:
        """Replace a planned item and revalidate the resulting plan atomically."""
        with self.transaction():
            slot = self.conn.execute("SELECT * FROM slots WHERE id=? AND status='planned'", (slot_id,)).fetchone()
            if not slot:
                raise DomainError("只能替换尚未播出且状态为 planned 的排期")
            program = self.conn.execute("SELECT * FROM programs WHERE id=?", (new_program_id,)).fetchone()
            if not program:
                raise DomainError("替换节目不存在")
            self._validate_slot(slot["air_date"], slot["start_time"], int(program["duration_minutes"]),
                                new_program_id, slot["region"], slot_id)
            self.conn.execute(
                "UPDATE slots SET program_id=?, duration_minutes=?, replaced_from=?, status='replaced' WHERE id=?",
                (new_program_id, int(program["duration_minutes"]), slot["program_id"], slot_id),
            )
            # 替换同样立即作废旧窗口并重算
            self._bump_plan_version()
            self._refresh_open_batches(slot["air_date"])
        return self.get_slot(slot_id)

    def get_slot(self, slot_id: int) -> dict:
        row = self.conn.execute(
            "SELECT s.*, p.title, p.kind, p.sponsor FROM slots s JOIN programs p ON p.id=s.program_id WHERE s.id=?",
            (slot_id,),
        ).fetchone()
        if not row:
            raise DomainError("排期不存在")
        return dict(row)

    # -------------------------------------------------------------- playout

    def record_playout(self, slot_id: int, actual_start: str, actual_duration_minutes: int,
                       actual_program_id: int | None = None, note: str = "") -> int:
        slot = self.conn.execute("SELECT * FROM slots WHERE id=?", (slot_id,)).fetchone()
        if not slot:
            raise DomainError("排期不存在")
        if actual_duration_minutes < 0:
            raise DomainError("实际时长不能为负数")
        _minutes(actual_start)
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO playout_logs(slot_id,actual_start,actual_duration_minutes,actual_program_id,note,created_at) VALUES(?,?,?,?,?,?)",
                (slot_id, actual_start, actual_duration_minutes, actual_program_id, note, datetime.now().isoformat()),
            )
            # 实播变化也要把进行中的批次对账结果立即重算（授权窗口版本不变）
            self._refresh_open_batches(slot["air_date"])
        return int(cur.lastrowid)

    # ------------------------------------------------------- review batches

    def _bump_plan_version(self) -> int:
        value = self.plan_version() + 1
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (PLAN_VERSION_KEY, str(value)),
        )
        return value

    def plan_version(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (PLAN_VERSION_KEY,)).fetchone()
        return int(row["value"]) if row else 1

    def _insert_batch(self, air_date: str, region: str | None, *, version_no: int, plan_version: int,
                      status: str, note: str = "", legacy: int = 0, created_at: str | None = None,
                      created_by: str = "", based_on: int | None = None, diff: list | None = None,
                      batch_no: str | None = None) -> int:
        now = created_at or datetime.now().isoformat()
        if batch_no is None:
            suffix = f"-{region}" if region else ""
            batch_no = f"RB-{air_date}-V{version_no:02d}{suffix}"
        cur = self.conn.execute(
            "INSERT INTO review_batches(batch_no,air_date,region,status,version_no,plan_version,"
            "based_on_batch_id,created_by,note,legacy,diff_json,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (batch_no, air_date, region, status, version_no, plan_version, based_on, created_by,
             note, legacy, json.dumps(diff or [], ensure_ascii=False), now, now),
        )
        return int(cur.lastrowid)

    def _slots_for(self, air_date: str, region: str | None):
        sql = ("SELECT s.*, p.title, p.sponsor, p.kind FROM slots s JOIN programs p ON p.id=s.program_id "
               "WHERE s.air_date=? AND s.status!='cancelled'")
        params: list[object] = [air_date]
        if region:
            sql += " AND s.region=?"
            params.append(region)
        sql += " ORDER BY s.start_time, s.id"
        return self.conn.execute(sql, params).fetchall()

    def _exceptions_for(self, slot) -> list[dict]:
        air_date = slot["air_date"]
        log = self.conn.execute(
            "SELECT * FROM playout_logs WHERE slot_id=? ORDER BY id DESC LIMIT 1", (slot["id"],)
        ).fetchone()
        found: list[dict] = []
        if not log:
            return [{"kind": "missed", "detail": "没有实播记录"}]
        actual_program_id = log["actual_program_id"] or slot["program_id"]
        if actual_program_id != slot["program_id"]:
            found.append({"kind": "wrong_program",
                          "detail": f"计划节目 #{slot['program_id']}，实播节目 #{actual_program_id}"})
        delta = log["actual_duration_minutes"] - slot["duration_minutes"]
        if abs(delta) > 30:
            found.append({"kind": "overrun" if delta > 0 else "underrun",
                          "detail": f"与计划相差 {delta:+d} 分钟"})
        actual = self.conn.execute("SELECT * FROM programs WHERE id=?", (actual_program_id,)).fetchone()
        if actual:
            region_ok = self.conn.execute(
                "SELECT 1 FROM program_regions WHERE program_id=? AND region=?",
                (actual_program_id, slot["region"]),
            ).fetchone()
            if not region_ok or not (actual["start_date"] <= air_date <= actual["end_date"]):
                found.append({"kind": "out_of_license", "detail": "实播节目超出地区或日期授权"})
        return found

    def _compute_spec(self, air_date: str, region: str | None) -> list[dict]:
        spec: list[dict] = []
        for seq, slot in enumerate(self._slots_for(air_date, region), 1):
            window = self.evaluate_slot(slot)
            exceptions = self._exceptions_for(slot)
            log = self.conn.execute(
                "SELECT actual_program_id FROM playout_logs WHERE slot_id=? ORDER BY id DESC LIMIT 1",
                (slot["id"],),
            ).fetchone()
            spec.append({
                "seq": seq,
                "slot_id": slot["id"],
                "start_time": slot["start_time"],
                "title": window["title"],
                "plan_program_id": slot["program_id"],
                "actual_program_id": (log["actual_program_id"] if log else None),
                "window": window,
                "blockers": window["blockers"],
                "exceptions": exceptions,
            })
        return spec

    @staticmethod
    def _fingerprint(entry: dict) -> str:
        payload = {
            "plan": entry["plan_program_id"],
            "actual": entry["actual_program_id"],
            "window": entry["window"],
            "exceptions": entry["exceptions"],
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _write_items(self, batch_id: int, spec: list[dict], *,
                     legacy: bool = False, preserved: dict | None = None) -> dict[int, int]:
        """重写批次下的条目并同步异常表，返回 slot_id -> item_id。"""
        air_date = self._air_date_of(batch_id)
        self.conn.execute("DELETE FROM reconciliation_exceptions WHERE batch_id=?", (batch_id,))
        self.conn.execute("DELETE FROM review_items WHERE batch_id=?", (batch_id,))
        id_map: dict[int, int] = {}
        now = datetime.now().isoformat()
        for entry in spec:
            fp = self._fingerprint(entry)
            old = (preserved or {}).get(entry["slot_id"])
            if legacy:
                status = "confirmed"
                result_json = json.dumps({"verdict": "legacy", "note": "历史回填"}, ensure_ascii=False)
            elif old is not None and old["status"] == "confirmed" and old["fingerprint"] == fp:
                # 指纹未变：保留人工确认与复核意见
                status = "confirmed"
                result_json = old["result_json"]
            else:
                status = "pending"
                result_json = None
            cur = self.conn.execute(
                "INSERT INTO review_items(batch_id,slot_id,seq,status,plan_program_id,actual_program_id,"
                "window_json,blockers_json,exceptions_json,result_json,fingerprint) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (batch_id, entry["slot_id"], entry["seq"], status, entry["plan_program_id"],
                 entry["actual_program_id"], json.dumps(entry["window"], ensure_ascii=False),
                 json.dumps(entry["blockers"], ensure_ascii=False),
                 json.dumps(entry["exceptions"], ensure_ascii=False), result_json, fp),
            )
            item_id = int(cur.lastrowid)
            id_map[entry["slot_id"]] = item_id
            for exc in entry["exceptions"]:
                self.conn.execute(
                    "INSERT INTO reconciliation_exceptions(air_date,slot_id,kind,detail,created_at,batch_id,item_id) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (air_date, entry["slot_id"], exc["kind"], exc["detail"], now, batch_id, item_id),
                )
        return id_map

    def _air_date_of(self, batch_id: int) -> str:
        return self.conn.execute("SELECT air_date FROM review_batches WHERE id=?", (batch_id,)).fetchone()[0]

    def _checkpoint(self, batch_id: int) -> int:
        """last_item_seq = 从头开始连续已确认的最大序号。"""
        seqs = [row[0] for row in self.conn.execute(
            "SELECT seq FROM review_items WHERE batch_id=? AND status='confirmed' ORDER BY seq", (batch_id,)
        ).fetchall()]
        checkpoint = 0
        for seq in seqs:
            if seq == checkpoint + 1:
                checkpoint = seq
            else:
                break
        self.conn.execute(
            "UPDATE review_batches SET last_item_seq=?, updated_at=? WHERE id=?",
            (checkpoint, datetime.now().isoformat(), batch_id),
        )
        return checkpoint

    def _refresh_open_batches(self, air_date: str) -> None:
        """排期/替换/实播变化后，立即重算当天所有进行中的批次（调用方持事务）。"""
        batches = self.conn.execute(
            "SELECT id FROM review_batches WHERE air_date=? AND status='open'", (air_date,)
        ).fetchall()
        for row in batches:
            self._repopulate(row["id"])

    def _repopulate(self, batch_id: int, *, count_recompute: bool = True) -> None:
        batch = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        preserved = {
            row["slot_id"]: {"status": row["status"], "fingerprint": row["fingerprint"],
                             "result_json": row["result_json"]}
            for row in self.conn.execute(
                "SELECT slot_id,status,fingerprint,result_json FROM review_items WHERE batch_id=?", (batch_id,)
            ).fetchall()
        }
        spec = self._compute_spec(batch["air_date"], batch["region"])
        self._write_items(batch_id, spec, preserved=preserved)
        self._checkpoint(batch_id)
        if count_recompute and preserved:
            self.conn.execute(
                "UPDATE review_batches SET plan_version=?, recompute_count=recompute_count+1, updated_at=? WHERE id=?",
                (self.plan_version(), datetime.now().isoformat(), batch_id),
            )
        else:
            self.conn.execute(
                "UPDATE review_batches SET plan_version=?, updated_at=? WHERE id=?",
                (self.plan_version(), datetime.now().isoformat(), batch_id),
            )

    def start_review(self, air_date: str, region: str | None = None, created_by: str = "") -> dict:
        """发起复核：同一天/地区已有进行中批次则幂等返回（断点续审）。"""
        try:
            datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        region = (region or "").strip() or None

        def action():
            with self.transaction():
                existing = self.conn.execute(
                    "SELECT id FROM review_batches WHERE air_date=? AND status='open' AND region IS ?",
                    (air_date, region),
                ).fetchone()
                if existing:
                    self._repopulate(existing["id"], count_recompute=False)
                    return existing["id"]
                row = self.conn.execute(
                    "SELECT COALESCE(MAX(version_no),0) FROM review_batches "
                    "WHERE air_date=? AND region IS ? AND status!='draft'",
                    (air_date, region),
                ).fetchone()
                batch_id = self._insert_batch(
                    air_date, region, version_no=int(row[0]) + 1,
                    plan_version=self.plan_version(), status="open", created_by=created_by.strip(),
                )
                spec = self._compute_spec(air_date, region)
                self._write_items(batch_id, spec)
                self._checkpoint(batch_id)
                return batch_id

        batch_id = self._retry_commit(action)
        return self.get_batch(batch_id)

    def confirm_item(self, batch_id: int, seq: int | None = None, attempts: int = 3) -> dict:
        """确认一个条目（默认检查点之后的第一个 pending），独立事务即检查点。

        seq=None 时在拿到写锁后再挑选下一个待确认项，因此多线程同时提交不会
        选中同一项；所有 pending 已确认完时返回 None。
        """

        def action():
            with self.transaction():
                batch = self.conn.execute(
                    "SELECT * FROM review_batches WHERE id=?", (batch_id,)
                ).fetchone()
                if not batch:
                    raise DomainError("复核批次不存在")
                if batch["status"] != "open":
                    raise DomainError(f"批次状态为 {batch['status']}，不能继续确认")
                if seq is None:
                    row = self.conn.execute(
                        "SELECT * FROM review_items WHERE batch_id=? AND status='pending' ORDER BY seq LIMIT 1",
                        (batch_id,),
                    ).fetchone()
                    if row is None:
                        return None
                else:
                    row = self.conn.execute(
                        "SELECT * FROM review_items WHERE batch_id=? AND seq=?", (batch_id, seq)
                    ).fetchone()
                    if row is None:
                        raise DomainError(f"第 {seq} 项不存在")
                    if row["status"] != "pending":
                        raise DomainError(f"第 {row['seq']} 项已经确认过")
                self.conn.execute(
                    "UPDATE review_items SET status='confirmed' WHERE id=? AND status='pending'",
                    (row["id"],),
                )
                self._checkpoint(batch_id)
                self.conn.execute(
                    "UPDATE review_batches SET revision=revision+1, updated_at=? WHERE id=?",
                    (datetime.now().isoformat(), batch_id),
                )
            return self.get_batch(batch_id)

        return self._retry_commit(action, attempts=attempts)

    @staticmethod
    def _normalize_results(results: dict | None, items) -> dict[int, dict]:
        """把 {seq 或 slot_id: {verdict,note}} 归一为 seq -> 结果。"""
        normalized: dict[int, dict] = {}
        if not results:
            return normalized
        seq_by_slot = {item["slot_id"]: item["seq"] for item in items}
        for raw_key, value in results.items():
            try:
                key = int(str(raw_key))
            except (TypeError, ValueError):
                continue
            if not isinstance(value, dict):
                continue
            seq = key if any(item["seq"] == key for item in items) else seq_by_slot.get(key)
            if seq is None:
                continue
            normalized[seq] = {
                "verdict": str(value.get("verdict", "ok"))[:40],
                "note": str(value.get("note", ""))[:200],
            }
        return normalized

    def submit_batch(self, batch_id: int, submitted_by: str = "", results: dict | None = None) -> dict:
        """终确认提交：先到者成为 confirmed，后到者保存草稿和差异。"""
        batch = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        if not batch:
            raise DomainError("复核批次不存在")
        if batch["status"] == "draft":
            raise DomainError("草稿不能直接提交，请在正式批次上修改后再提交")

        # 1) 从检查点继续逐项确认；每项独立提交，失败重试不重复。
        #    并发时先到者会把批次置为 confirmed，后到者在此处自然停下进入草稿分支。
        while True:
            try:
                advanced = self.confirm_item(batch_id, None)
            except DomainError:
                break
            if advanced is None:
                break

        # 2) CAS 终确认，失败方落草稿
        def finalize():
            with self.transaction():
                current = self.conn.execute(
                    "SELECT * FROM review_batches WHERE id=?", (batch_id,)
                ).fetchone()
                items = self._item_rows(batch_id)
                mine = self._normalize_results(results, items)
                if current["status"] == "open":
                    changed = self.conn.execute(
                        "UPDATE review_batches SET status='confirmed', confirmed_by=?, confirmed_at=?, "
                        "revision=revision+1, updated_at=? WHERE id=? AND status='open'",
                        (submitted_by.strip() or "匿名", datetime.now().isoformat(),
                         datetime.now().isoformat(), batch_id),
                    )
                    if changed.rowcount == 1:
                        for seq, value in mine.items():
                            self.conn.execute(
                                "UPDATE review_items SET result_json=? WHERE batch_id=? AND seq=?",
                                (json.dumps(value, ensure_ascii=False), batch_id, seq),
                            )
                        return {"outcome": "confirmed", "batch": self.get_batch(batch_id)}
                # 后到者：胜出者已经确认
                winner = current
                winner_items = self._item_rows(batch_id)
                diff = self._diff_results(winner_items, mine)
                draft_count = self.conn.execute(
                    "SELECT COUNT(*) FROM review_batches WHERE based_on_batch_id=?", (batch_id,)
                ).fetchone()[0]
                draft_no = f"{winner['batch_no']}-D{draft_count + 1}"
                draft_id = self._insert_batch(
                    winner["air_date"], winner["region"], version_no=winner["version_no"],
                    plan_version=winner["plan_version"], status="draft", based_on=batch_id,
                    created_by=submitted_by.strip() or "匿名", note="并发提交的后到者草稿",
                    diff=diff, batch_no=draft_no,
                )
                self._copy_items_as_draft(draft_id, winner_items, mine)
                return {
                    "outcome": "draft",
                    "batch": self.get_batch(batch_id),
                    "draft": self.get_batch(draft_id),
                    "diff": diff,
                    "message": f"批次已由 {winner['confirmed_by'] or '他人'} 先确认，你的结果已存为草稿 {draft_no}",
                }

        return self._retry_commit(finalize)

    def _diff_results(self, winner_items, mine: dict[int, dict]) -> list[dict]:
        diff: list[dict] = []
        for item in winner_items:
            theirs = json.loads(item["result_json"]) if item["result_json"] else {}
            ours = mine.get(item["seq"], {})
            for field in ("verdict", "note"):
                if theirs.get(field, "") != ours.get(field, ""):
                    diff.append({
                        "seq": item["seq"],
                        "slot_id": item["slot_id"],
                        "field": field,
                        "winner": theirs.get(field, ""),
                        "yours": ours.get(field, ""),
                    })
        return diff

    def _copy_items_as_draft(self, draft_id: int, winner_items, mine: dict[int, dict]) -> None:
        for item in winner_items:
            self.conn.execute(
                "INSERT INTO review_items(batch_id,slot_id,seq,status,plan_program_id,actual_program_id,"
                "window_json,blockers_json,exceptions_json,result_json,fingerprint) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (draft_id, item["slot_id"], item["seq"], "pending", item["plan_program_id"],
                 item["actual_program_id"], item["window_json"], item["blockers_json"],
                 item["exceptions_json"],
                 json.dumps(mine.get(item["seq"], {}), ensure_ascii=False) if mine else None,
                 item["fingerprint"]),
            )

    def _item_rows(self, batch_id: int):
        return self.conn.execute(
            "SELECT * FROM review_items WHERE batch_id=? ORDER BY seq", (batch_id,)
        ).fetchall()

    def get_batch(self, batch_id: int) -> dict:
        row = self.conn.execute("SELECT * FROM review_batches WHERE id=?", (batch_id,)).fetchone()
        if not row:
            raise DomainError("复核批次不存在")
        batch = dict(row)
        items = []
        for item in self._item_rows(batch_id):
            data = dict(item)
            for key in ("window_json", "blockers_json", "exceptions_json", "result_json"):
                parsed = json.loads(data.pop(key)) if data[key] else None
                data[key[:-5] if key.endswith("_json") else key] = parsed
            items.append(data)
        batch["items"] = items
        batch["diff"] = json.loads(batch.pop("diff_json") or "[]")
        return batch

    def list_batches(self, air_date: str | None = None) -> list[dict]:
        sql = "SELECT id FROM review_batches"
        params: list[object] = []
        if air_date:
            sql += " WHERE air_date=?"
            params.append(air_date)
        sql += " ORDER BY id DESC"
        return [self.get_batch(row["id"]) for row in self.conn.execute(sql, params).fetchall()]

    # ----------------------------------------------------------- reconcile

    def reconcile_date(self, air_date: str) -> list[dict]:
        """对账即刷新/创建当天复核批次，返回最新批次的异常（旧批次保留为历史版本）。"""
        try:
            datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError as exc:
            raise DomainError("日期必须使用 YYYY-MM-DD") from exc
        batch = self.start_review(air_date)
        return self.get_exceptions(air_date)

    def get_exceptions(self, air_date: str) -> list[dict]:
        # 只返回当天最新版本批次（排除草稿）的异常，历史批次不再被当成有效结果
        latest = self.conn.execute(
            "SELECT id FROM review_batches WHERE air_date=? AND status!='draft' ORDER BY id DESC LIMIT 1",
            (air_date,),
        ).fetchone()
        if not latest:
            return []
        return [dict(row) for row in self.conn.execute(
            "SELECT * FROM reconciliation_exceptions WHERE batch_id=? ORDER BY slot_id, kind",
            (latest["id"],),
        ).fetchall()]

    # ------------------------------------------------------------- snapshot

    def snapshot(self) -> dict:
        programs = []
        for row in self.conn.execute("SELECT * FROM programs ORDER BY id").fetchall():
            item = dict(row)
            item["regions"] = [r["region"] for r in self.conn.execute(
                "SELECT region FROM program_regions WHERE program_id=? ORDER BY region", (row["id"],)
            ).fetchall()]
            programs.append(item)
        slots = []
        for row in self.conn.execute(
            "SELECT s.*, p.title, p.kind FROM slots s JOIN programs p ON p.id=s.program_id "
            "ORDER BY s.air_date,s.start_time"
        ).fetchall():
            slot = dict(row)
            slot["window"] = self.evaluate_slot(row)
            slots.append(slot)
        return {
            "plan_version": self.plan_version(),
            "programs": programs,
            "slots": slots,
            "batches": self.list_batches(),
            "exceptions": [dict(row) for row in self.conn.execute(
                "SELECT * FROM reconciliation_exceptions ORDER BY id DESC LIMIT 50"
            ).fetchall()],
        }

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
