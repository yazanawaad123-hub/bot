import asyncio
import difflib
import hashlib
import copy
import json
import logging
import os
import re
import sqlite3
import time
import platform
import sys
import unicodedata
import threading
import tempfile
import urllib.request, urllib.error
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands
from flask import Flask, jsonify, request


# =============================================================================
# إعدادات المكتبة — عبّئ الـ IDs المطلوبة فقط
# =============================================================================
MATERIALS_FILE = "materials.json"
DATABASE_FILE = "library.db"

# قناة أخذ رتب التخصصات. إذا بقيت 0 فلن يظهر زر قناة الرتب.
ROLES_CHANNEL_ID = 0

# قناة المساعدة لحل مشكلة وجود أكثر من رتبة تخصص.
HELP_CHANNEL_ID = 0

# فحص تلقائي لتغيّر materials.json وتحديث الجلسات المفتوحة.
LIVE_SYNC_INTERVAL_SECONDS = 2.0

# إعدادات الأداء والتحديث الحي. القيم محافظة لتناسب Render المجاني.
HEALTH_CHECK_INTERVAL_SECONDS = 60.0
LIVE_REFRESH_DEBOUNCE_SECONDS = 1.25
LIVE_REFRESH_CONCURRENCY = 20
MAX_ACTIVE_SESSIONS = 3000
SEARCH_CACHE_TTL_SECONDS = 45.0
SEARCH_CACHE_MAX_ITEMS = 500
BACKGROUND_FLUSH_INTERVAL_SECONDS = 15.0

# خدمة بوت الرفع المنفصلة التي تملك النسخة الأساسية من materials.json.
UPLOAD_REMOTE_URL = os.getenv("UPLOAD_REMOTE_URL", "").strip().rstrip("/")
LIBRARY_SYNC_TOKEN = os.getenv("LIBRARY_SYNC_TOKEN", "").strip()
REMOTE_SYNC_POLL_SECONDS = max(1.0, float(os.getenv("REMOTE_SYNC_POLL_SECONDS", "2")))
REMOTE_SYNC_TIMEOUT_SECONDS = max(2.0, float(os.getenv("REMOTE_SYNC_TIMEOUT_SECONDS", "8")))
LIBRARY_WEB_HOST = os.getenv("HOST", "0.0.0.0")
LIBRARY_WEB_PORT = int(os.getenv("PORT", "10000"))

# قناة لوحة الإدارة الخاصة بالمكتبة. 0 = السماح بنشرها في أي قناة بواسطة الإدارة.
LIBRARY_ADMIN_CHANNEL_ID = 0

# قناة بلاغات الملفات التالفة/الخاطئة. 0 = تعطيل إرسال البلاغات للقناة مع بقائها محفوظة بقاعدة البيانات.
LIBRARY_REPORTS_CHANNEL_ID = 0

# قناة أخطاء البوت الإدارية. 0 = تسجيل الأخطاء في Console فقط.
LIBRARY_LOG_CHANNEL_ID = 0

# عدد النتائج في صفحة البحث، وعدد العناصر في صفحة التصفح.
SEARCH_RESULTS_PER_PAGE = 8
BROWSE_ITEMS_PER_PAGE = 20
RELATED_FILES_LIMIT = 3

# حد بسيط يمنع ضغط المستخدم نفسه بسرعة، ولا يعيق الاستخدام الطبيعي.
USER_ACTIONS_PER_10_SECONDS = 20

# حسابات إدارة المكتبة فقط. أضف Discord User IDs هنا.
# لا تعتمد أوامر الإدارة على رتبة Administrator. مالك السيرفر مسموح له احتياطياً.
LIBRARY_ADMIN_USER_IDS: set[int] = {
    # 123456789012345678,
}

UI_STYLE_MAP = {
    "ازرق": discord.ButtonStyle.primary,
    "رمادي": discord.ButtonStyle.secondary,
    "اخضر": discord.ButtonStyle.success,
    "احمر": discord.ButtonStyle.danger,
}
UI_STYLE_NAMES = {1: "ازرق", 2: "رمادي", 3: "اخضر", 4: "احمر"}

SPECIALIZATION_ROLES = {
    "طالب علمي": "علمي", "طالبة علمي": "علمي",
    "طالب أدبي": "أدبي", "طالبة أدبي": "أدبي",
    "طالب صناعي": "صناعي", "طالبة صناعي": "صناعي",
    "طالب تجاري": "تجاري", "طالبة تجاري": "تجاري",
    "طالب زراعي": "زراعي", "طالبة زراعي": "زراعي",
    "طالب فندقي": "فندقي", "طالبة فندقي": "فندقي",
    "طالب اقتصاد منزلي": "اقتصاد منزلي", "طالبة اقتصاد منزلي": "اقتصاد منزلي",
    "طالب شرعي": "شرعي", "طالبة شرعي": "شرعي",
}

# كلمات شائعة يكتبها الطالب بطريقة مختلفة. أضف عليها متى شئت.
SEARCH_ALIASES = {
    "فيزيا": "فيزياء",
    "فزيا": "فيزياء",
    "فيز": "فيزياء",
    "فيزياءه": "فيزياء",
    "رياضه": "رياضيات",
    "رياضياته": "رياضيات",
    "عربي": "لغة عربية",
    "العربي": "لغة عربية",
    "انجليزي": "لغة انجليزية",
    "انقليزي": "لغة انجليزية",
    "إنجليزي": "لغة انجليزية",
    "دين": "تربية اسلامية",
    "اسلامية": "تربية اسلامية",
    "اسلاميه": "تربية اسلامية",
    "تجريبي": "امتحانات تجريبي",
    "تجريبية": "امتحانات تجريبي",
    "اجابات": "اجابة",
    "حلول": "اجابة",
    "دوسيه": "دوسية",
    "دوسيات": "دوسية",
    "ملخصات": "ملخص",
    "كتب": "كتاب",
}

logger = logging.getLogger("library_bot")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_arabic(text: Any) -> str:
    """توحيد الكتابة العربية لتسهيل البحث المرن."""
    value = unicodedata.normalize("NFKC", str(text or "")).lower().strip()
    value = re.sub(r"[\u064b-\u065f\u0670\u06d6-\u06ed]", "", value)
    value = value.translate(str.maketrans({
        "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
        "ى": "ي", "ؤ": "و", "ئ": "ي", "ة": "ه",
        "ـ": " ",
    }))
    value = re.sub(r"[^\w\u0600-\u06ff]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


NORMALIZED_ALIASES = {normalize_arabic(k): normalize_arabic(v) for k, v in SEARCH_ALIASES.items()}


def expand_query_tokens(query: str) -> list[str]:
    tokens: list[str] = []
    for token in normalize_arabic(query).split():
        mapped = NORMALIZED_ALIASES.get(token, token)
        tokens.extend(mapped.split())
    return [t for t in tokens if t]


def is_url(value: str) -> bool:
    return bool(re.match(r"^https?://", value.strip(), flags=re.I))


def safe_label(text: str, limit: int = 80) -> str:
    text = str(text).strip() or "بدون اسم"
    return text if len(text) <= limit else text[: limit - 1] + "…"


def member_is_admin(member: discord.Member) -> bool:
    return is_library_admin(member)


def is_library_admin(member: discord.Member) -> bool:
    if member.id in LIBRARY_ADMIN_USER_IDS:
        return True
    return bool(member.guild and member.guild.owner_id == member.id)


async def require_library_admin(interaction: discord.Interaction) -> bool:
    if not isinstance(interaction.user, discord.Member) or not is_library_admin(interaction.user):
        await safe_interaction_error(interaction, "هذا الأمر مخصص لحسابات إدارة المكتبة فقط.")
        return False
    if LIBRARY_ADMIN_CHANNEL_ID and interaction.channel_id != LIBRARY_ADMIN_CHANNEL_ID:
        await safe_interaction_error(interaction, f"استخدم أوامر الإدارة في <#{LIBRARY_ADMIN_CHANNEL_ID}> فقط.")
        return False
    return True


def parse_internal_path(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"[>/\\]", value or "") if part.strip()]


def stable_node_id(parent_path: list[str], child_key: str) -> str:
    raw = json.dumps(parent_path + [child_key], ensure_ascii=False, separators=(",", ":"))
    return "NODE-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16].upper()


class EventBus:
    """ناقل أحداث داخلي خفيف يربط الرفع والمزامنة والبحث والجلسات المفتوحة."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Any]] = defaultdict(list)

    def subscribe(self, event_name: str, callback: Any) -> None:
        if callback not in self._subscribers[event_name]:
            self._subscribers[event_name].append(callback)

    async def publish(self, event_name: str, payload: Optional[dict[str, Any]] = None) -> None:
        payload = payload or {}
        callbacks = list(self._subscribers.get(event_name, []))
        if not callbacks:
            return
        results = await asyncio.gather(*(cb(payload) for cb in callbacks), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.exception("فشل مستمع حدث %s", event_name, exc_info=result)


class LibraryStore:
    """SQLite + فهرس ذاكرة للبحث السريع."""

    def __init__(self, db_path: str, materials_path: str):
        self.db_path = db_path
        self.materials_path = materials_path
        self._write_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self.files_cache: list[dict[str, Any]] = []
        self.tree_cache: dict[str, Any] = {}
        self.materials_mtime: float = -1
        self._search_cache: dict[tuple[str, str, int], tuple[float, list[dict[str, Any]]]] = {}
        self._search_inflight: dict[tuple[str, str, int], asyncio.Task] = {}
        self._search_inflight_lock = asyncio.Lock()
        self._ui_children_cache: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = {}
        self._files_by_path: dict[str, dict[str, Any]] = {}
        self._browse_keys_index: dict[tuple[str, ...], list[str]] = {}
        self._favorites_cache: set[tuple[int, int]] = set()
        self._pending_views: dict[int, int] = defaultdict(int)
        self._pending_search_logs: deque[tuple[int, str, str, str, int, str]] = deque()
        self._pending_lock = asyncio.Lock()
        self.metrics: dict[str, float] = defaultdict(float)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)
        await self.sync_from_materials(force=True)
        self._favorites_cache = await asyncio.to_thread(self._load_favorites_sync)

    def _load_favorites_sync(self) -> set[tuple[int, int]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT user_id,file_id FROM favorites").fetchall()
        return {(int(r["user_id"]), int(r["file_id"])) for r in rows}

    def _initialize_sync(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unique_key TEXT NOT NULL UNIQUE,
                    specialization TEXT NOT NULL,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    path_json TEXT NOT NULL,
                    path_text TEXT NOT NULL,
                    normalized_text TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    added_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    views INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_files_spec_active ON files(specialization, active);
                CREATE INDEX IF NOT EXISTS idx_files_views ON files(views DESC);
                CREATE INDEX IF NOT EXISTS idx_files_added ON files(added_at DESC);

                CREATE TABLE IF NOT EXISTS favorites (
                    user_id INTEGER NOT NULL,
                    file_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, file_id),
                    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER,
                    file_id INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    details TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status, created_at DESC);

                CREATE TABLE IF NOT EXISTS searches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    specialization TEXT NOT NULL,
                    query TEXT NOT NULL,
                    normalized_query TEXT NOT NULL,
                    results_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_searches_created ON searches(created_at DESC);

                CREATE TABLE IF NOT EXISTS user_search_state (
                    user_id INTEGER NOT NULL,
                    specialization TEXT NOT NULL,
                    query TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, specialization)
                );

                CREATE TABLE IF NOT EXISTS missing_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    specialization TEXT NOT NULL,
                    query TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_missing_requests_query ON missing_requests(specialization, query, created_at DESC);

                CREATE TABLE IF NOT EXISTS panels (
                    guild_id INTEGER PRIMARY KEY,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );


                CREATE TABLE IF NOT EXISTS ui_nodes (
                    node_id TEXT PRIMARY KEY,
                    parent_path_json TEXT NOT NULL,
                    child_key TEXT NOT NULL,
                    display_name TEXT,
                    emoji TEXT,
                    style_name TEXT NOT NULL DEFAULT 'رمادي',
                    position INTEGER,
                    hidden INTEGER NOT NULL DEFAULT 0,
                    render_mode TEXT NOT NULL DEFAULT 'button',
                    locked INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    UNIQUE(parent_path_json, child_key)
                );
                CREATE INDEX IF NOT EXISTS idx_ui_nodes_parent ON ui_nodes(parent_path_json, position);

                CREATE TABLE IF NOT EXISTS ui_templates (
                    name TEXT PRIMARY KEY,
                    template_json TEXT NOT NULL,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ui_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    node_id TEXT,
                    before_json TEXT NOT NULL DEFAULT '{}',
                    after_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ui_history_created ON ui_history(created_at DESC);

                CREATE TABLE IF NOT EXISTS library_settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def _load_materials_sync(self) -> dict[str, Any]:
        if not os.path.exists(self.materials_path):
            logger.warning("لم يتم العثور على %s — المكتبة ستبدأ فارغة.", self.materials_path)
            return {}
        with open(self.materials_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("يجب أن يكون جذر materials.json كائناً JSON من نوع object.")
        return data

    @staticmethod
    def _extract_files(node: Any, path: Optional[list[str]] = None) -> list[dict[str, Any]]:
        path = path or []
        output: list[dict[str, Any]] = []
        if isinstance(node, dict):
            # دعم صيغة ملف مطورة بجانب الصيغة القديمة: {name,url,...}
            if isinstance(node.get("url"), str) and is_url(node["url"]):
                name = str(node.get("name") or (path[-1] if path else "ملف"))
                actual_path = path[:-1] + [name] if path else [name]
                meta = {k: v for k, v in node.items() if k not in {"url", "name"}}
                output.append({"name": name, "url": node["url"], "path": actual_path, "metadata": meta})
                return output
            for key, value in node.items():
                output.extend(LibraryStore._extract_files(value, path + [str(key)]))
        elif isinstance(node, str) and is_url(node):
            output.append({
                "name": path[-1] if path else "ملف",
                "url": node,
                "path": path,
                "metadata": {},
            })
        return output

    async def sync_from_materials(self, force: bool = False) -> bool:
        """مزامنة JSON إلى SQLite فقط عند تغيّر الملف."""
        try:
            mtime = os.path.getmtime(self.materials_path) if os.path.exists(self.materials_path) else -1
            if not force and mtime == self.materials_mtime:
                return False
            tree = await asyncio.to_thread(self._load_materials_sync)
            extracted = self._extract_files(tree)
            await asyncio.to_thread(self._sync_files_sync, extracted)
            async with self._cache_lock:
                self.tree_cache = tree
                self.materials_mtime = mtime
            await self.reload_cache()
            logger.info("تمت مزامنة المكتبة: %s ملف.", len(extracted))
            return True
        except Exception:
            logger.exception("فشلت مزامنة materials.json")
            raise

    def _sync_files_sync(self, extracted: list[dict[str, Any]]) -> None:
        now = utc_now_iso()
        active_keys: set[str] = set()
        with self._connect() as conn:
            for item in extracted:
                path = [str(p) for p in item["path"]]
                if len(path) < 2:
                    continue
                spec = path[0]
                name = str(item["name"])
                url = str(item["url"])
                unique_key = normalize_arabic(" | ".join(path)) + "|" + url.strip()
                active_keys.add(unique_key)
                path_text = " > ".join(path)
                normalized_text = normalize_arabic(" ".join(path) + " " + name)
                conn.execute(
                    """
                    INSERT INTO files(
                        unique_key, specialization, name, url, path_json, path_text,
                        normalized_text, metadata_json, added_at, updated_at, active
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,1)
                    ON CONFLICT(unique_key) DO UPDATE SET
                        specialization=excluded.specialization,
                        name=excluded.name,
                        url=excluded.url,
                        path_json=excluded.path_json,
                        path_text=excluded.path_text,
                        normalized_text=excluded.normalized_text,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at,
                        active=1
                    """,
                    (
                        unique_key, spec, name, url, json.dumps(path, ensure_ascii=False),
                        path_text, normalized_text,
                        json.dumps(item.get("metadata") or {}, ensure_ascii=False), now, now,
                    ),
                )
            if active_keys:
                placeholders = ",".join("?" for _ in active_keys)
                conn.execute(f"UPDATE files SET active=0 WHERE unique_key NOT IN ({placeholders})", tuple(active_keys))
            else:
                conn.execute("UPDATE files SET active=0")

    async def reload_cache(self) -> None:
        rows = await asyncio.to_thread(self._all_files_sync)
        files_by_path = {item["path_text"]: item for item in rows}
        browse_index = self._build_browse_index(self.tree_cache)
        async with self._cache_lock:
            self.files_cache = rows
            self._files_by_path = files_by_path
            self._browse_keys_index = browse_index
            self._search_cache.clear()
            self._ui_children_cache.clear()

    @classmethod
    def _build_browse_index(cls, tree: dict[str, Any]) -> dict[tuple[str, ...], list[str]]:
        index: dict[tuple[str, ...], list[str]] = {}

        def walk(node: Any, path: list[str]) -> bool:
            if isinstance(node, str):
                return is_url(node)
            if not isinstance(node, dict):
                return False
            if isinstance(node.get("url"), str) and is_url(node["url"]):
                return True
            keys: list[str] = []
            has_any = False
            for key, value in node.items():
                if key in {"url", "name", "description", "year", "size", "added_at"}:
                    continue
                if walk(value, path + [str(key)]):
                    keys.append(str(key))
                    has_any = True
            index[tuple(path)] = keys
            return has_any

        walk(tree, [])
        return index

    async def get_browse_keys(self, path: list[str]) -> list[str]:
        async with self._cache_lock:
            return list(self._browse_keys_index.get(tuple(path), []))

    def _all_files_sync(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM files WHERE active=1").fetchall()
        return [self._row_to_file(row) for row in rows]

    @staticmethod
    def _row_to_file(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["path"] = json.loads(item.pop("path_json"))
        item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
        return item

    async def get_tree_node(self, path: list[str]) -> Any:
        # التصفح يقرأ من الذاكرة فقط؛ المراقب/ناقل الأحداث يتولى المزامنة.
        async with self._cache_lock:
            node: Any = self.tree_cache
            for key in path:
                if isinstance(node, dict) and key in node:
                    node = node[key]
                else:
                    return None
            return node

    async def get_file_by_path(self, path: list[str]) -> Optional[dict[str, Any]]:
        path_text = " > ".join(path)
        async with self._cache_lock:
            item = self._files_by_path.get(path_text)
            return dict(item) if item else None

    async def get_file(self, file_id: int) -> Optional[dict[str, Any]]:
        def work() -> Optional[dict[str, Any]]:
            with self._connect() as conn:
                row = conn.execute("SELECT * FROM files WHERE id=? AND active=1", (file_id,)).fetchone()
            return self._row_to_file(row) if row else None
        return await asyncio.to_thread(work)

    async def search(self, specialization: str, query: str, limit: int = 100) -> list[dict[str, Any]]:
        normalized_key = normalize_arabic(query)
        cache_key = (specialization, normalized_key, limit)
        now_mono = time.monotonic()
        async with self._cache_lock:
            cached = self._search_cache.get(cache_key)
            if cached and now_mono - cached[0] <= SEARCH_CACHE_TTL_SECONDS:
                self.metrics["search_cache_hits"] += 1
                return [dict(x) for x in cached[1]]

        # جميع الطلبات المتطابقة تشترك في عملية بحث واحدة فقط.
        async with self._search_inflight_lock:
            task = self._search_inflight.get(cache_key)
            if task is None or task.done():
                task = asyncio.create_task(
                    self._search_uncached(specialization, query, limit, cache_key),
                    name=f"library-search:{specialization}:{normalized_key[:30]}",
                )
                self._search_inflight[cache_key] = task
                task.add_done_callback(lambda _t, key=cache_key: self._search_inflight.pop(key, None))
            else:
                self.metrics["search_coalesced"] += 1
        result = await asyncio.shield(task)
        return [dict(x) for x in result]

    async def _search_uncached(
        self, specialization: str, query: str, limit: int, cache_key: tuple[str, str, int]
    ) -> list[dict[str, Any]]:
        started = time.perf_counter()
        tokens = expand_query_tokens(query)
        if not tokens:
            return []
        async with self._cache_lock:
            candidates = [x for x in self.files_cache if x["specialization"] == specialization]

        scored: list[tuple[float, dict[str, Any]]] = []
        normalized_query = " ".join(tokens)
        for item in candidates:
            text = item["normalized_text"]
            words = text.split()
            score = 0.0
            matched_all = True
            for token in tokens:
                if token in text:
                    score += 12.0
                    if token in words:
                        score += 5.0
                    continue
                best = max((difflib.SequenceMatcher(None, token, word).ratio() for word in words), default=0.0)
                if best >= 0.78:
                    score += best * 7.0
                else:
                    matched_all = False
                    break
            if not matched_all:
                continue
            if normalized_query in text:
                score += 20.0
            score += min(item.get("views", 0), 1000) / 1000
            scored.append((score, item))

        scored.sort(key=lambda x: (x[0], x[1].get("views", 0)), reverse=True)
        result = [dict(item) for _, item in scored[:limit]]
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        self.metrics["search_count"] += 1
        self.metrics["search_total_ms"] += elapsed_ms
        self.metrics["search_max_ms"] = max(self.metrics.get("search_max_ms", 0.0), elapsed_ms)
        async with self._cache_lock:
            if len(self._search_cache) >= SEARCH_CACHE_MAX_ITEMS:
                oldest_key = min(self._search_cache, key=lambda k: self._search_cache[k][0])
                self._search_cache.pop(oldest_key, None)
            self._search_cache[cache_key] = (time.monotonic(), result)
        return result

    async def log_search(self, user_id: int, specialization: str, query: str, count: int) -> None:
        async with self._pending_lock:
            self._pending_search_logs.append(
                (user_id, specialization, query, normalize_arabic(query), count, utc_now_iso())
            )

    async def increment_view(self, file_id: int) -> None:
        # تحديث فوري في الذاكرة، والكتابة إلى SQLite تتم لاحقاً كدفعة واحدة.
        async with self._pending_lock:
            self._pending_views[file_id] += 1
        async with self._cache_lock:
            for item in self.files_cache:
                if item["id"] == file_id:
                    item["views"] += 1
                    break

    async def flush_background_writes(self) -> dict[str, int]:
        async with self._pending_lock:
            views = dict(self._pending_views)
            searches = list(self._pending_search_logs)
            self._pending_views.clear()
            self._pending_search_logs.clear()
        if not views and not searches:
            return {"views": 0, "searches": 0}

        def work() -> None:
            with self._connect() as conn:
                if views:
                    conn.executemany(
                        "UPDATE files SET views=views+? WHERE id=?",
                        [(count, file_id) for file_id, count in views.items()],
                    )
                if searches:
                    conn.executemany(
                        "INSERT INTO searches(user_id,specialization,query,normalized_query,results_count,created_at) VALUES(?,?,?,?,?,?)",
                        searches,
                    )

        try:
            await asyncio.to_thread(work)
            return {"views": sum(views.values()), "searches": len(searches)}
        except Exception:
            # لا نضيّع البيانات إذا كانت قاعدة البيانات مشغولة مؤقتاً.
            async with self._pending_lock:
                for file_id, count in views.items():
                    self._pending_views[file_id] += count
                self._pending_search_logs.extendleft(reversed(searches))
            raise

    async def toggle_favorite(self, user_id: int, file_id: int) -> bool:
        key = (user_id, file_id)
        async with self._write_lock:
            exists = key in self._favorites_cache

            def work() -> None:
                with self._connect() as conn:
                    if exists:
                        conn.execute("DELETE FROM favorites WHERE user_id=? AND file_id=?", key)
                    else:
                        conn.execute(
                            "INSERT OR IGNORE INTO favorites(user_id,file_id,created_at) VALUES(?,?,?)",
                            (user_id, file_id, utc_now_iso()),
                        )

            await asyncio.to_thread(work)
            if exists:
                self._favorites_cache.discard(key)
                return False
            self._favorites_cache.add(key)
            return True

    async def is_favorite(self, user_id: int, file_id: int) -> bool:
        return (user_id, file_id) in self._favorites_cache

    async def favorites(self, user_id: int, specialization: str) -> list[dict[str, Any]]:
        def work() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT f.* FROM favorites fav
                    JOIN files f ON f.id=fav.file_id
                    WHERE fav.user_id=? AND f.specialization=? AND f.active=1
                    ORDER BY fav.created_at DESC
                    """, (user_id, specialization)
                ).fetchall()
            return [self._row_to_file(r) for r in rows]
        return await asyncio.to_thread(work)

    async def related(self, file: dict[str, Any], limit: int = RELATED_FILES_LIMIT) -> list[dict[str, Any]]:
        path = file["path"]
        # نعتمد غالباً على الفرع + المادة (أول مستويين) مع تجنب الملف نفسه.
        subject = normalize_arabic(path[1]) if len(path) > 1 else ""
        async with self._cache_lock:
            matches = [dict(x) for x in self.files_cache if x["id"] != file["id"] and x["specialization"] == file["specialization"]]
        ranked = []
        for item in matches:
            score = 0
            if len(item["path"]) > 1 and normalize_arabic(item["path"][1]) == subject:
                score += 20
            shared = len(set(normalize_arabic(" ".join(path)).split()) & set(item["normalized_text"].split()))
            score += shared
            if score >= 20:
                ranked.append((score, item.get("views", 0), item))
        ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return [x[2] for x in ranked[:limit]]

    async def create_report(self, user_id: int, guild_id: Optional[int], file_id: int, reason: str, details: str) -> int:
        async with self._write_lock:
            def work() -> int:
                with self._connect() as conn:
                    cur = conn.execute(
                        "INSERT INTO reports(user_id,guild_id,file_id,reason,details,created_at) VALUES(?,?,?,?,?,?)",
                        (user_id, guild_id, file_id, reason, details[:1000], utc_now_iso()),
                    )
                    return int(cur.lastrowid)
            return await asyncio.to_thread(work)

    async def save_last_search(self, user_id: int, specialization: str, query: str) -> None:
        def work() -> None:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO user_search_state(user_id,specialization,query,updated_at) VALUES(?,?,?,?) "
                    "ON CONFLICT(user_id,specialization) DO UPDATE SET query=excluded.query,updated_at=excluded.updated_at",
                    (user_id, specialization, query, utc_now_iso()),
                )
        await asyncio.to_thread(work)

    async def get_last_search(self, user_id: int, specialization: str) -> Optional[str]:
        def work() -> Optional[str]:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT query FROM user_search_state WHERE user_id=? AND specialization=?",
                    (user_id, specialization),
                ).fetchone()
            return str(row["query"]) if row else None
        return await asyncio.to_thread(work)

    async def add_missing_request(self, user_id: int, specialization: str, query: str) -> None:
        def work() -> None:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO missing_requests(user_id,specialization,query,created_at) VALUES(?,?,?,?)",
                    (user_id, specialization, query, utc_now_iso()),
                )
        await asyncio.to_thread(work)

    async def missing_requests_summary(self, limit: int = 25) -> list[dict[str, Any]]:
        def work() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT specialization,query,COUNT(*) c,MAX(created_at) last_at FROM missing_requests "
                    "GROUP BY specialization,query ORDER BY c DESC,last_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        return await asyncio.to_thread(work)

    async def suggest_queries(self, specialization: str, query: str, limit: int = 5) -> list[str]:
        tokens = expand_query_tokens(query)
        async with self._cache_lock:
            files = [dict(x) for x in self.files_cache if x["specialization"] == specialization and x.get("active", 1)]
        candidates: dict[str, float] = {}
        for file in files:
            parts = list(file.get("path", [])) + [file.get("name", "")]
            for part in parts:
                norm = normalize_arabic(part)
                if not norm:
                    continue
                score = max((difflib.SequenceMatcher(None, t, norm).ratio() for t in tokens), default=0.0)
                if score >= 0.45:
                    candidates[str(part)] = max(candidates.get(str(part), 0.0), score)
        return [x[0] for x in sorted(candidates.items(), key=lambda kv: (-kv[1], len(kv[0])))[:limit]]

    async def health_snapshot(self) -> dict[str, Any]:
        def db_check() -> dict[str, Any]:
            with self._connect() as conn:
                quick = conn.execute("PRAGMA quick_check").fetchone()[0]
                active = conn.execute("SELECT COUNT(*) FROM files WHERE active=1").fetchone()[0]
                db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            return {"db_check": quick, "active_files": active, "db_size": db_size}
        data = await asyncio.to_thread(db_check)
        count = self.metrics.get("search_count", 0.0)
        data.update({
            "cache_files": len(self.files_cache),
            "search_cache_items": len(self._search_cache),
            "search_count": int(count),
            "search_avg_ms": (self.metrics.get("search_total_ms", 0.0) / count) if count else 0.0,
            "search_max_ms": self.metrics.get("search_max_ms", 0.0),
            "search_cache_hits": int(self.metrics.get("search_cache_hits", 0.0)),
        })
        return data

    async def stats(self) -> dict[str, Any]:
        def work() -> dict[str, Any]:
            with self._connect() as conn:
                total = conn.execute("SELECT COUNT(*) FROM files WHERE active=1").fetchone()[0]
                views = conn.execute("SELECT COALESCE(SUM(views),0) FROM files WHERE active=1").fetchone()[0]
                favs = conn.execute("SELECT COUNT(*) FROM favorites").fetchone()[0]
                reports = conn.execute("SELECT COUNT(*) FROM reports WHERE status='open'").fetchone()[0]
                searches = conn.execute("SELECT COUNT(*) FROM searches").fetchone()[0]
                by_spec = conn.execute(
                    "SELECT specialization,COUNT(*) c FROM files WHERE active=1 GROUP BY specialization ORDER BY c DESC"
                ).fetchall()
                top_files = conn.execute(
                    "SELECT id,name,path_text,views FROM files WHERE active=1 ORDER BY views DESC LIMIT 8"
                ).fetchall()
                no_results = conn.execute(
                    """
                    SELECT query,COUNT(*) c FROM searches WHERE results_count=0
                    GROUP BY normalized_query ORDER BY c DESC LIMIT 8
                    """
                ).fetchall()
            return {
                "total": total, "views": views, "favorites": favs, "reports": reports,
                "searches": searches, "by_spec": [dict(x) for x in by_spec],
                "top_files": [dict(x) for x in top_files], "no_results": [dict(x) for x in no_results],
            }
        return await asyncio.to_thread(work)

    async def latest(self, limit: int = 20) -> list[dict[str, Any]]:
        def work() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM files WHERE active=1 ORDER BY added_at DESC,id DESC LIMIT ?", (limit,)
                ).fetchall()
            return [self._row_to_file(r) for r in rows]
        return await asyncio.to_thread(work)

    async def save_panel(self, guild_id: int, channel_id: int, message_id: int) -> None:
        def work() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO panels(guild_id,channel_id,message_id,updated_at) VALUES(?,?,?,?)
                    ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id,message_id=excluded.message_id,updated_at=excluded.updated_at
                    """, (guild_id, channel_id, message_id, utc_now_iso())
                )
        await asyncio.to_thread(work)

    async def get_panel(self, guild_id: int) -> Optional[dict[str, int]]:
        def work() -> Optional[dict[str, int]]:
            with self._connect() as conn:
                row = conn.execute("SELECT channel_id,message_id FROM panels WHERE guild_id=?", (guild_id,)).fetchone()
            return dict(row) if row else None
        return await asyncio.to_thread(work)

    async def delete_panel_record(self, guild_id: int) -> None:
        def work() -> None:
            with self._connect() as conn:
                conn.execute("DELETE FROM panels WHERE guild_id=?", (guild_id,))
        await asyncio.to_thread(work)


    async def get_ui_children(self, parent_path: list[str], keys: list[str]) -> list[dict[str, Any]]:
        parent_json = json.dumps(parent_path, ensure_ascii=False, separators=(",", ":"))
        cache_key = (parent_json, tuple(keys))
        async with self._cache_lock:
            cached = self._ui_children_cache.get(cache_key)
            if cached is not None:
                self.metrics["ui_cache_hits"] += 1
                return [dict(x) for x in cached]
        def work() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute("SELECT * FROM ui_nodes WHERE parent_path_json=?", (parent_json,)).fetchall()
            overrides = {str(r["child_key"]): dict(r) for r in rows}
            result=[]
            for original_index, key in enumerate(keys):
                row=overrides.get(key, {})
                result.append({
                    "node_id": row.get("node_id") or stable_node_id(parent_path, key),
                    "key": key,
                    "display_name": row.get("display_name") or key,
                    "emoji": row.get("emoji") or "",
                    "style_name": row.get("style_name") or "رمادي",
                    "position": row.get("position") if row.get("position") is not None else 100000 + original_index,
                    "hidden": bool(row.get("hidden", 0)),
                    "render_mode": row.get("render_mode") or "button",
                    "locked": bool(row.get("locked", 0)),
                })
            result.sort(key=lambda x: (int(x["position"]), normalize_arabic(x["display_name"])))
            return result
        result = await asyncio.to_thread(work)
        async with self._cache_lock:
            self._ui_children_cache[cache_key] = [dict(x) for x in result]
        return result

    async def invalidate_ui_cache(self, parent_path: Optional[list[str]] = None) -> None:
        async with self._cache_lock:
            if parent_path is None:
                self._ui_children_cache.clear()
                return
            parent_json = json.dumps(parent_path, ensure_ascii=False, separators=(",", ":"))
            for key in [k for k in self._ui_children_cache if k[0] == parent_json]:
                self._ui_children_cache.pop(key, None)

    def _ui_row_sync(self, parent_path: list[str], child_key: str) -> dict[str, Any]:
        parent_json=json.dumps(parent_path, ensure_ascii=False, separators=(",", ":"))
        node_id=stable_node_id(parent_path, child_key)
        with self._connect() as conn:
            row=conn.execute("SELECT * FROM ui_nodes WHERE parent_path_json=? AND child_key=?", (parent_json, child_key)).fetchone()
        if row:
            return dict(row)
        return {"node_id":node_id,"parent_path_json":parent_json,"child_key":child_key,"display_name":None,"emoji":None,"style_name":"رمادي","position":None,"hidden":0,"render_mode":"button","locked":0,"updated_at":utc_now_iso()}

    async def set_ui_override(self, user_id: int, parent_path: list[str], child_key: str, **changes: Any) -> dict[str, Any]:
        allowed={"display_name","emoji","style_name","position","hidden","render_mode","locked"}
        changes={k:v for k,v in changes.items() if k in allowed}
        async with self._write_lock:
            def work() -> dict[str, Any]:
                before=self._ui_row_sync(parent_path, child_key)
                after=dict(before); after.update(changes); after["updated_at"]=utc_now_iso()
                cols=["node_id","parent_path_json","child_key","display_name","emoji","style_name","position","hidden","render_mode","locked","updated_at"]
                with self._connect() as conn:
                    conn.execute(f"INSERT INTO ui_nodes({','.join(cols)}) VALUES({','.join('?' for _ in cols)}) ON CONFLICT(parent_path_json,child_key) DO UPDATE SET display_name=excluded.display_name,emoji=excluded.emoji,style_name=excluded.style_name,position=excluded.position,hidden=excluded.hidden,render_mode=excluded.render_mode,locked=excluded.locked,updated_at=excluded.updated_at", tuple(after.get(c) for c in cols))
                    conn.execute("INSERT INTO ui_history(user_id,action,node_id,before_json,after_json,created_at) VALUES(?,?,?,?,?,?)", (user_id,"update",after["node_id"],json.dumps(before,ensure_ascii=False),json.dumps(after,ensure_ascii=False),utc_now_iso()))
                return after
            result = await asyncio.to_thread(work)
            await self.invalidate_ui_cache(parent_path)
            return result

    async def move_ui_node(self, user_id: int, parent_path: list[str], child_key: str, direction: str, absolute_position: Optional[int]=None) -> dict[str, Any]:
        keys_node=await self.get_tree_node(parent_path)
        if not isinstance(keys_node, dict) or child_key not in keys_node:
            raise ValueError("العنصر غير موجود في المسار الداخلي.")
        keys=[str(k) for k in keys_node.keys() if k not in {"url","name","description","year","size","added_at"}]
        items=await self.get_ui_children(parent_path, keys)
        visible=[x for x in items if not x["hidden"]]
        idx=next((i for i,x in enumerate(visible) if x["key"]==child_key),None)
        if idx is None: raise ValueError("العنصر مخفي أو غير موجود.")
        target=idx
        if absolute_position is not None: target=max(0,min(len(visible)-1,absolute_position))
        elif direction=="اعلى": target=max(0,idx-5)
        elif direction=="اسفل": target=min(len(visible)-1,idx+5)
        elif direction=="يمين": target=min(len(visible)-1,idx+1)
        elif direction=="يسار": target=max(0,idx-1)
        elif direction=="اول": target=0
        elif direction=="اخر": target=len(visible)-1
        moved=visible.pop(idx); visible.insert(target,moved)
        for position,item in enumerate(visible):
            await self.set_ui_override(user_id,parent_path,item["key"],position=position)
        return {"from":idx,"to":target,"item":moved}

    async def reset_ui_node(self, user_id:int,parent_path:list[str],child_key:str)->None:
        def work():
            parent_json=json.dumps(parent_path,ensure_ascii=False,separators=(",",":"))
            with self._connect() as conn:
                row=conn.execute("SELECT * FROM ui_nodes WHERE parent_path_json=? AND child_key=?",(parent_json,child_key)).fetchone()
                if row:
                    conn.execute("INSERT INTO ui_history(user_id,action,node_id,before_json,after_json,created_at) VALUES(?,?,?,?,?,?)",(user_id,"reset",row["node_id"],json.dumps(dict(row),ensure_ascii=False),"{}",utc_now_iso()))
                    conn.execute("DELETE FROM ui_nodes WHERE parent_path_json=? AND child_key=?",(parent_json,child_key))
        await asyncio.to_thread(work)
        await self.invalidate_ui_cache(parent_path)

    async def auto_sort_ui(self,user_id:int,parent_path:list[str],mode:str)->int:
        node=await self.get_tree_node(parent_path)
        if not isinstance(node,dict): raise ValueError("المسار ليس مجلداً.")
        keys=[str(k) for k in node.keys() if k not in {"url","name","description","year","size","added_at"}]
        items=await self.get_ui_children(parent_path,keys)
        if mode=="ابجدي": items.sort(key=lambda x:normalize_arabic(x["display_name"]))
        elif mode=="عكسي": items.sort(key=lambda x:normalize_arabic(x["display_name"]),reverse=True)
        elif mode=="سنوات_تنازلي": items.sort(key=lambda x:int(re.search(r"(?:19|20)\\d{2}",x["display_name"]).group()) if re.search(r"(?:19|20)\\d{2}",x["display_name"]) else -1,reverse=True)
        elif mode=="سنوات_تصاعدي": items.sort(key=lambda x:int(re.search(r"(?:19|20)\\d{2}",x["display_name"]).group()) if re.search(r"(?:19|20)\\d{2}",x["display_name"]) else 9999)
        else: raise ValueError("نوع الترتيب غير معروف.")
        for pos,item in enumerate(items): await self.set_ui_override(user_id,parent_path,item["key"],position=pos)
        return len(items)

    async def copy_ui_format(self,user_id:int,source_parent:list[str],target_parent:list[str])->int:
        source_node=await self.get_tree_node(source_parent); target_node=await self.get_tree_node(target_parent)
        if not isinstance(source_node,dict) or not isinstance(target_node,dict): raise ValueError("أحد المسارين ليس مجلداً.")
        source_keys=[str(k) for k in source_node.keys() if k in target_node]
        source_items=await self.get_ui_children(source_parent,source_keys)
        for item in source_items:
            await self.set_ui_override(user_id,target_parent,item["key"],display_name=item["display_name"] if item["display_name"]!=item["key"] else None,emoji=item["emoji"] or None,style_name=item["style_name"],position=item["position"],hidden=int(item["hidden"]),render_mode=item["render_mode"],locked=int(item["locked"]))
        return len(source_items)

    async def save_ui_template(self,user_id:int,name:str,parent_path:list[str])->int:
        node=await self.get_tree_node(parent_path)
        if not isinstance(node,dict): raise ValueError("المسار ليس مجلداً.")
        keys=[str(k) for k in node.keys() if k not in {"url","name","description","year","size","added_at"}]
        items=await self.get_ui_children(parent_path,keys)
        payload=[{k:x[k] for k in ("key","display_name","emoji","style_name","position","hidden","render_mode","locked")} for x in items]
        def work():
            with self._connect() as conn: conn.execute("INSERT INTO ui_templates(name,template_json,created_by,created_at) VALUES(?,?,?,?) ON CONFLICT(name) DO UPDATE SET template_json=excluded.template_json,created_by=excluded.created_by,created_at=excluded.created_at",(name,json.dumps(payload,ensure_ascii=False),user_id,utc_now_iso()))
        await asyncio.to_thread(work); return len(payload)

    async def apply_ui_template(self,user_id:int,name:str,target_parent:list[str])->int:
        def load():
            with self._connect() as conn: row=conn.execute("SELECT template_json FROM ui_templates WHERE name=?",(name,)).fetchone()
            return json.loads(row[0]) if row else None
        payload=await asyncio.to_thread(load)
        if payload is None: raise ValueError("القالب غير موجود.")
        target=await self.get_tree_node(target_parent)
        if not isinstance(target,dict): raise ValueError("المسار الهدف ليس مجلداً.")
        count=0
        for item in payload:
            if item["key"] in target:
                await self.set_ui_override(user_id,target_parent,item["key"],**{k:item.get(k) for k in ("display_name","emoji","style_name","position","hidden","render_mode","locked")}); count+=1
        return count

    async def ui_history(self,limit:int=20)->list[dict[str,Any]]:
        def work():
            with self._connect() as conn: return [dict(r) for r in conn.execute("SELECT * FROM ui_history ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]
        return await asyncio.to_thread(work)

    async def undo_last_ui(self,user_id:int)->Optional[dict[str,Any]]:
        def work():
            with self._connect() as conn:
                row=conn.execute("SELECT * FROM ui_history WHERE action IN ('update','reset') ORDER BY id DESC LIMIT 1").fetchone()
                if not row:return None
                before=json.loads(row["before_json"] or "{}"); after=json.loads(row["after_json"] or "{}")
                ref=before or after
                if before:
                    cols=["node_id","parent_path_json","child_key","display_name","emoji","style_name","position","hidden","render_mode","locked","updated_at"]
                    before["updated_at"]=utc_now_iso()
                    conn.execute(f"INSERT INTO ui_nodes({','.join(cols)}) VALUES({','.join('?' for _ in cols)}) ON CONFLICT(parent_path_json,child_key) DO UPDATE SET display_name=excluded.display_name,emoji=excluded.emoji,style_name=excluded.style_name,position=excluded.position,hidden=excluded.hidden,render_mode=excluded.render_mode,locked=excluded.locked,updated_at=excluded.updated_at",tuple(before.get(c) for c in cols))
                elif ref:
                    conn.execute("DELETE FROM ui_nodes WHERE node_id=?",(ref.get("node_id"),))
                conn.execute("INSERT INTO ui_history(user_id,action,node_id,before_json,after_json,created_at) VALUES(?,?,?,?,?,?)",(user_id,"undo",ref.get("node_id"),json.dumps(after,ensure_ascii=False),json.dumps(before,ensure_ascii=False),utc_now_iso()))
                return dict(row)
        return await asyncio.to_thread(work)

    async def ui_diagnostics(self)->dict[str,Any]:
        def work():
            with self._connect() as conn:
                duplicate_positions=[dict(r) for r in conn.execute("SELECT parent_path_json,position,COUNT(*) c FROM ui_nodes WHERE position IS NOT NULL AND hidden=0 GROUP BY parent_path_json,position HAVING c>1").fetchall()]
                long_names=[dict(r) for r in conn.execute("SELECT node_id,display_name FROM ui_nodes WHERE LENGTH(display_name)>80").fetchall()]
                hidden=[dict(r) for r in conn.execute("SELECT node_id,child_key,parent_path_json FROM ui_nodes WHERE hidden=1").fetchall()]
                total=conn.execute("SELECT COUNT(*) FROM ui_nodes").fetchone()[0]
            return {"total":total,"duplicate_positions":duplicate_positions,"long_names":long_names,"hidden":hidden}
        return await asyncio.to_thread(work)


class ActionLimiter:
    def __init__(self, limit: int, window: float):
        self.limit = limit
        self.window = window
        self.events: dict[int, deque[float]] = defaultdict(deque)

    def allow(self, user_id: int) -> bool:
        now = time.monotonic()
        q = self.events[user_id]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            return False
        q.append(now)
        return True


class LibraryBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!lib_", intents=intents)
        self.store = LibraryStore(DATABASE_FILE, MATERIALS_FILE)
        self.limiter = ActionLimiter(USER_ACTIONS_PER_10_SECONDS, 10.0)
        self.events_bus = EventBus()
        self._synced_once = False
        self.active_sessions: dict[int, dict[str, Any]] = {}
        self._live_sync_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._background_flush_task: Optional[asyncio.Task] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._refresh_pending = False
        self._remote_sync_task: Optional[asyncio.Task] = None
        self._remote_etag: str = ""
        self._refresh_semaphore = asyncio.Semaphore(LIVE_REFRESH_CONCURRENCY)
        self.started_monotonic = time.monotonic()
        self.health_state: dict[str, Any] = {}
        self.events_bus.subscribe("library.changed", self._on_library_changed)

    def register_session(self, user_id: int, message: Any, state: dict[str, Any]) -> None:
        if len(self.active_sessions) >= MAX_ACTIVE_SESSIONS and user_id not in self.active_sessions:
            oldest = min(self.active_sessions, key=lambda uid: self.active_sessions[uid]["updated"])
            self.active_sessions.pop(oldest, None)
        self.active_sessions[user_id] = {"message": message, "state": state, "updated": time.monotonic()}

    async def _refresh_active_sessions(self) -> None:
        stale: list[int] = []

        async def refresh_one(user_id: int, session: dict[str, Any]) -> None:
            if time.monotonic() - session["updated"] > 900:
                stale.append(user_id)
                return
            message = session["message"]
            state = session["state"]
            async with self._refresh_semaphore:
                try:
                    if state["kind"] == "browse":
                        node = await self.store.get_tree_node(state["path"])
                        if not isinstance(node, dict):
                            return
                        keys = await self.store.get_browse_keys(state["path"])
                        items = await self.store.get_ui_children(state["path"], keys)
                        embed, view = await build_browse_payload(self, state["specialization"], state["path"], items, state.get("page",0))
                        await message.edit(embed=embed, view=view)
                    elif state["kind"] == "search":
                        results = await self.store.search(state["specialization"], state["query"])
                        filtered = filter_search_results(results, state.get("filter", "الكل"))
                        page = min(state.get("page",0), max(0,(len(filtered)-1)//SEARCH_RESULTS_PER_PAGE))
                        await message.edit(embed=search_results_embed(state["query"], filtered, page), view=SearchResultsView(self,state["specialization"],state["query"],results,page,state.get("filter","الكل")))
                except (discord.NotFound, discord.Forbidden):
                    stale.append(user_id)
                except discord.HTTPException as exc:
                    logger.warning("تعذر تحديث جلسة %s بسبب Discord HTTP: %s", user_id, exc)
                except Exception:
                    logger.exception("فشل تحديث جلسة المكتبة للمستخدم %s", user_id)

        await asyncio.gather(*(refresh_one(uid, sess) for uid, sess in list(self.active_sessions.items())))
        for user_id in stale:
            self.active_sessions.pop(user_id, None)

    async def _on_library_changed(self, payload: dict[str, Any]) -> None:
        # دمج عدة إضافات متتابعة في تحديث واحد لتجنب مئات تعديلات Discord.
        self._refresh_pending = True
        if self._refresh_task and not self._refresh_task.done():
            return

        async def runner() -> None:
            while self._refresh_pending and not self.is_closed():
                self._refresh_pending = False
                await asyncio.sleep(LIVE_REFRESH_DEBOUNCE_SECONDS)
                await self._refresh_active_sessions()

        self._refresh_task = asyncio.create_task(runner(), name="library-session-refresh")

    async def notify_library_changed(self) -> bool:
        """يستدعيها بوت رفع الملفات فور قبول/حذف/تعديل ملف.

        تعيد مزامنة JSON وSQLite والذاكرة ثم تحدث جلسات الطلاب المفتوحة.
        """
        changed = await self.store.sync_from_materials(force=True)
        await self.events_bus.publish("library.changed", {"source": "direct"})
        return changed

    def _fetch_remote_materials_sync(self, force: bool = False) -> tuple[bool, bytes | None, str]:
        if not UPLOAD_REMOTE_URL or not LIBRARY_SYNC_TOKEN:
            return False, None, self._remote_etag
        headers = {
            "Authorization": f"Bearer {LIBRARY_SYNC_TOKEN}",
            "User-Agent": "TawjihiLibrary/remote-sync",
        }
        if self._remote_etag and not force:
            headers["If-None-Match"] = f'"{self._remote_etag}"'
        req = urllib.request.Request(f"{UPLOAD_REMOTE_URL}/api/library/snapshot", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=REMOTE_SYNC_TIMEOUT_SECONDS) as response:
                raw = response.read()
                etag = response.headers.get("ETag", "").strip('"')
                json.loads(raw.decode("utf-8"))
                return True, raw, etag
        except urllib.error.HTTPError as exc:
            if exc.code == 304:
                return False, None, self._remote_etag
            raise

    async def sync_from_remote(self, force: bool = False) -> bool:
        changed, raw, etag = await asyncio.to_thread(self._fetch_remote_materials_sync, force)
        if not changed or raw is None:
            return False
        target = Path(MATERIALS_FILE)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix="materials-remote-", suffix=".tmp", dir=str(target.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, target)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        self._remote_etag = etag or hashlib.sha256(raw).hexdigest()
        await self.notify_library_changed()
        return True

    async def _remote_sync_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.sync_from_remote(force=False)
            except Exception:
                logger.exception("فشلت مزامنة المكتبة من خدمة الرفع")
            await asyncio.sleep(REMOTE_SYNC_POLL_SECONDS)

    async def _live_sync_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                changed = await self.store.sync_from_materials()
                if changed:
                    await self.events_bus.publish("library.changed", {"source": "watcher"})
            except Exception:
                logger.exception("فشل التحديث الحي للمكتبة")
            await asyncio.sleep(LIVE_SYNC_INTERVAL_SECONDS)

    async def _background_flush_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.store.flush_background_writes()
            except Exception:
                logger.exception("فشل حفظ دفعة العدادات وسجلات البحث")
            await asyncio.sleep(BACKGROUND_FLUSH_INTERVAL_SECONDS)

    async def _health_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                snapshot = await self.store.health_snapshot()
                snapshot.update({
                    "uptime_seconds": int(time.monotonic() - self.started_monotonic),
                    "active_sessions": len(self.active_sessions),
                    "guilds": len(self.guilds),
                    "latency_ms": round(self.latency * 1000, 1),
                    "python": sys.version.split()[0],
                    "platform": platform.system(),
                })
                try:
                    import psutil  # اختياري
                    proc = psutil.Process(os.getpid())
                    snapshot["rss_mb"] = round(proc.memory_info().rss / 1024 / 1024, 1)
                    snapshot["cpu_percent"] = proc.cpu_percent(interval=None)
                except Exception:
                    snapshot["rss_mb"] = None
                    snapshot["cpu_percent"] = None
                self.health_state = snapshot
                if snapshot.get("db_check") != "ok":
                    await send_log(self, f"فحص SQLite غير سليم: {snapshot.get('db_check')}")
                if snapshot.get("latency_ms", 0) > 1500:
                    await send_log(self, f"زمن اتصال Discord مرتفع: {snapshot['latency_ms']}ms")
            except Exception:
                logger.exception("فشل فحص صحة المكتبة")
            await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)

    async def internal_load_test(self, operations: int = 1000, concurrency: int = 100) -> dict[str, Any]:
        operations = max(1, min(operations, 5000))
        concurrency = max(1, min(concurrency, 500))
        specs = list(dict.fromkeys(SPECIALIZATION_ROLES.values()))
        sem = asyncio.Semaphore(concurrency)
        latencies: list[float] = []

        async def one(i: int) -> None:
            async with sem:
                started = time.perf_counter()
                await self.store.search(specs[i % len(specs)], "فيزيا وزاري 2025", limit=20)
                latencies.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(operations)))
        total = time.perf_counter() - started
        ordered = sorted(latencies)
        return {
            "operations": operations,
            "concurrency": concurrency,
            "total_seconds": total,
            "ops_per_second": operations / total if total else 0.0,
            "avg_ms": sum(latencies) / len(latencies),
            "p95_ms": ordered[min(len(ordered)-1, int(len(ordered)*0.95))],
            "max_ms": max(latencies),
        }

    async def setup_hook(self) -> None:
        await self.store.initialize()
        if UPLOAD_REMOTE_URL and LIBRARY_SYNC_TOKEN:
            try:
                await self.sync_from_remote(force=True)
            except Exception:
                logger.exception("تعذر سحب النسخة الأولية من بوت الرفع؛ سيتم الاعتماد على النسخة المحلية مؤقتاً")
        self.add_view(OpenLibraryView(self))
        self._remote_sync_task = asyncio.create_task(self._remote_sync_loop(), name="library-remote-sync")
        self._live_sync_task = asyncio.create_task(self._live_sync_loop(), name="library-live-sync")
        self._health_task = asyncio.create_task(self._health_loop(), name="library-health-monitor")
        self._background_flush_task = asyncio.create_task(
            self._background_flush_loop(), name="library-background-flush"
        )
        if not self._synced_once:
            await self.tree.sync()
            self._synced_once = True

    async def on_ready(self) -> None:
        logger.info("البوت جاهز: %s (%s)", self.user, self.user.id if self.user else "?")

    async def on_error(self, event_method: str, *args: Any, **kwargs: Any) -> None:
        logger.exception("خطأ غير معالج في الحدث %s", event_method)
        await send_log(self, f"خطأ غير معالج في `{event_method}`")


async def send_log(bot: LibraryBot, text: str) -> None:
    if not LIBRARY_LOG_CHANNEL_ID:
        return
    try:
        channel = bot.get_channel(LIBRARY_LOG_CHANNEL_ID) or await bot.fetch_channel(LIBRARY_LOG_CHANNEL_ID)
        if isinstance(channel, discord.abc.Messageable):
            await channel.send(f"⚠️ {text[:1800]}")
    except Exception:
        logger.exception("تعذر إرسال سجل الخطأ لقناة Logs")


async def safe_interaction_error(interaction: discord.Interaction, text: str = "صار خطأ بسيط. جرّب مرة ثانية.") -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception:
        logger.exception("تعذر إرسال رسالة الخطأ للمستخدم")


def get_user_specializations(member: discord.Member) -> list[str]:
    role_names = {r.name for r in member.roles}
    found: list[str] = []
    for role_name, spec in SPECIALIZATION_ROLES.items():
        if role_name in role_names and spec not in found:
            found.append(spec)
    return found


def channel_link_view(channel_id: int, guild_id: Optional[int], label: str, emoji: str = "🆘") -> Optional[discord.ui.View]:
    if not channel_id:
        return None
    view = discord.ui.View(timeout=180)
    url = f"https://discord.com/channels/{guild_id}/{channel_id}" if guild_id else f"https://discord.com/channels/@me/{channel_id}"
    view.add_item(discord.ui.Button(label=label, style=discord.ButtonStyle.link, url=url, emoji=emoji))
    return view


def help_channel_view(guild_id: Optional[int] = None) -> Optional[discord.ui.View]:
    return channel_link_view(HELP_CHANNEL_ID, guild_id, "الذهاب إلى قناة المساعدة")


def node_has_files(node: Any) -> bool:
    if isinstance(node, str):
        return is_url(node)
    if isinstance(node, dict):
        if isinstance(node.get("url"), str) and is_url(node["url"]):
            return True
        return any(node_has_files(v) for k, v in node.items() if k not in {"url", "name", "description", "year", "size", "added_at"})
    return False


def role_channel_view(guild_id: Optional[int] = None) -> Optional[discord.ui.View]:
    if not ROLES_CHANNEL_ID:
        return None
    view = discord.ui.View(timeout=120)
    view.add_item(discord.ui.Button(
        label="اذهب إلى قناة الرتب",
        style=discord.ButtonStyle.link,
        url=(f"https://discord.com/channels/{guild_id}/{ROLES_CHANNEL_ID}" if guild_id else f"https://discord.com/channels/@me/{ROLES_CHANNEL_ID}"),
    ))
    return view


class SearchModal(discord.ui.Modal, title="البحث في المكتبة"):
    query = discord.ui.TextInput(
        label="اكتب ما تبحث عنه",
        placeholder="مثال: فيزيا وزاري أو فيزياء 2025",
        min_length=2,
        max_length=100,
    )

    def __init__(self, bot: LibraryBot, specialization: str):
        super().__init__(timeout=180)
        self.bot = bot
        self.specialization = specialization

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not self.bot.limiter.allow(interaction.user.id):
            await interaction.response.send_message("خفف الضغط شوي وجرب بعد ثوانٍ.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            query=str(self.query)
            results = await self.bot.store.search(self.specialization, query)
            await self.bot.store.log_search(interaction.user.id, self.specialization, query, len(results))
            await self.bot.store.save_last_search(interaction.user.id, self.specialization, query)
            if not results:
                suggestions=await self.bot.store.suggest_queries(self.specialization,query)
                msg=f"ما لقيت نتائج لـ **{query}** داخل مكتبة {self.specialization}."
                if suggestions: msg += "\n\n**اقتراحات قريبة:** " + "، ".join(suggestions)
                message=await interaction.followup.send(msg,view=MissingResultView(self.bot,self.specialization,query),ephemeral=True,wait=True)
                return
            message=await interaction.followup.send(embed=search_results_embed(query,results,0),view=SearchResultsView(self.bot,self.specialization,query,results,0),ephemeral=True,wait=True)
            self.bot.register_session(interaction.user.id,message,{"kind":"search","specialization":self.specialization,"query":query,"page":0,"filter":"الكل"})
        except Exception:
            logger.exception("فشل البحث")
            await interaction.followup.send("تعذر تنفيذ البحث الآن. جرّب مرة ثانية.", ephemeral=True)


class MissingResultView(discord.ui.View):
    def __init__(self,bot:LibraryBot,specialization:str,query:str):
        super().__init__(timeout=300); self.bot=bot; self.specialization=specialization; self.query=query
    @discord.ui.button(label="طلب توفير الملف",style=discord.ButtonStyle.success,emoji="📥")
    async def request_file(self,interaction:discord.Interaction,button:discord.ui.Button)->None:
        await self.bot.store.add_missing_request(interaction.user.id,self.specialization,self.query)
        await interaction.response.send_message("تم تسجيل طلبك للإدارة.",ephemeral=True)
    @discord.ui.button(label="بحث جديد",style=discord.ButtonStyle.primary,emoji="🔍")
    async def new_search(self,interaction:discord.Interaction,button:discord.ui.Button)->None:
        await interaction.response.send_modal(SearchModal(self.bot,self.specialization))


class ReportDetailsModal(discord.ui.Modal, title="تفاصيل البلاغ"):
    details = discord.ui.TextInput(
        label="اشرح المشكلة باختصار",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500,
        placeholder="مثال: الرابط يفتح ملفاً مختلفاً",
    )

    def __init__(self, bot: LibraryBot, file_id: int, reason: str):
        super().__init__(timeout=180)
        self.bot = bot
        self.file_id = file_id
        self.reason = reason

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        file = await self.bot.store.get_file(self.file_id)
        if not file:
            await interaction.followup.send("هذا الملف لم يعد موجوداً.", ephemeral=True)
            return
        report_id = await self.bot.store.create_report(
            interaction.user.id,
            interaction.guild.id if interaction.guild else None,
            self.file_id,
            self.reason,
            str(self.details),
        )
        await send_report_to_channel(self.bot, interaction, file, report_id, self.reason, str(self.details))
        await interaction.followup.send("تم إرسال البلاغ للإدارة، شكراً إلك.", ephemeral=True)


class ReportReasonSelect(discord.ui.Select):
    def __init__(self, bot: LibraryBot, file_id: int):
        options = [
            discord.SelectOption(label="الرابط لا يعمل", value="الرابط لا يعمل", emoji="🔗"),
            discord.SelectOption(label="ملف خاطئ", value="ملف خاطئ", emoji="📄"),
            discord.SelectOption(label="المسار غير صحيح", value="المسار غير صحيح", emoji="📂"),
            discord.SelectOption(label="الملف ناقص أو جودته سيئة", value="الملف ناقص أو جودته سيئة", emoji="⚠️"),
            discord.SelectOption(label="مشكلة أخرى", value="مشكلة أخرى", emoji="✍️"),
        ]
        super().__init__(placeholder="اختر نوع المشكلة", min_values=1, max_values=1, options=options)
        self.bot = bot
        self.file_id = file_id

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(ReportDetailsModal(self.bot, self.file_id, self.values[0]))


class ReportReasonView(discord.ui.View):
    def __init__(self, bot: LibraryBot, file_id: int):
        super().__init__(timeout=180)
        self.add_item(ReportReasonSelect(bot, file_id))


async def send_report_to_channel(
    bot: LibraryBot,
    interaction: discord.Interaction,
    file: dict[str, Any],
    report_id: int,
    reason: str,
    details: str,
) -> None:
    if not LIBRARY_REPORTS_CHANNEL_ID:
        return
    try:
        channel = bot.get_channel(LIBRARY_REPORTS_CHANNEL_ID) or await bot.fetch_channel(LIBRARY_REPORTS_CHANNEL_ID)
        embed = discord.Embed(title=f"بلاغ مكتبة #{report_id}", color=discord.Color.orange(), timestamp=discord.utils.utcnow())
        embed.add_field(name="السبب", value=reason, inline=False)
        embed.add_field(name="الملف", value=safe_label(file["name"], 250), inline=False)
        embed.add_field(name="المسار", value=file["path_text"][:1024], inline=False)
        embed.add_field(name="المستخدم", value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
        if details.strip():
            embed.add_field(name="التفاصيل", value=details[:1024], inline=False)
        view = discord.ui.View(timeout=None)
        view.add_item(discord.ui.Button(label="فتح الملف", style=discord.ButtonStyle.link, url=file["url"]))
        await channel.send(embed=embed, view=view)
    except Exception:
        logger.exception("تعذر إرسال البلاغ لقناة الإدارة")


def search_results_embed(query: str, results: list[dict[str, Any]], page: int) -> discord.Embed:
    total_pages = max(1, (len(results) + SEARCH_RESULTS_PER_PAGE - 1) // SEARCH_RESULTS_PER_PAGE)
    start = page * SEARCH_RESULTS_PER_PAGE
    subset = results[start:start + SEARCH_RESULTS_PER_PAGE]
    embed = discord.Embed(
        title=f"نتائج البحث: {query}",
        description=f"وجدت **{len(results)}** نتيجة — صفحة {page + 1}/{total_pages}",
        color=discord.Color.blurple(),
    )
    for index, item in enumerate(subset, start=start + 1):
        embed.add_field(
            name=f"{index}. {safe_label(item['name'], 200)}",
            value=f"`{item['path_text'][:850]}`",
            inline=False,
        )
    return embed


class FileSelect(discord.ui.Select):
    def __init__(self, bot: LibraryBot, files: list[dict[str, Any]], placeholder: str = "اختر الملف", search_context: Optional[dict[str, Any]] = None):
        options = []
        for item in files[:25]:
            description = item["path_text"]
            options.append(discord.SelectOption(
                label=safe_label(item["name"], 100),
                value=str(item["id"]),
                description=safe_label(description, 100),
                emoji="📎",
            ))
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options)
        self.bot = bot
        self.search_context = search_context

    async def callback(self, interaction: discord.Interaction) -> None:
        file = await self.bot.store.get_file(int(self.values[0]))
        if not file:
            await interaction.response.send_message("الملف لم يعد موجوداً.", ephemeral=True)
            return
        await show_file(interaction, self.bot, file, self.search_context)


FILTER_KEYWORDS = {
    "الكل": (),
    "وزاري": ("وزاري",),
    "تجريبي": ("تجريبي", "تجريبيه"),
    "ملخصات": ("ملخص",),
    "دوسيات": ("دوسيه",),
    "كتب": ("كتاب",),
    "إجابات": ("اجابه", "حل"),
}

def filter_search_results(results: list[dict[str, Any]], selected: str) -> list[dict[str, Any]]:
    words = FILTER_KEYWORDS.get(selected, ())
    if not words:
        return results
    return [r for r in results if any(w in normalize_arabic(r.get("path_text","") + " " + r.get("name","")) for w in words)]

class SearchFilterSelect(discord.ui.Select):
    def __init__(self, parent: "SearchResultsView"):
        options=[discord.SelectOption(label=k,value=k,default=k==parent.selected_filter) for k in FILTER_KEYWORDS]
        super().__init__(placeholder="فلترة النتائج",options=options,row=1)
        self.parent_view=parent
    async def callback(self,interaction:discord.Interaction)->None:
        selected=self.values[0]
        filtered=filter_search_results(self.parent_view.all_results,selected)
        await interaction.response.edit_message(embed=search_results_embed(self.parent_view.query,filtered,0),view=SearchResultsView(self.parent_view.bot,self.parent_view.specialization,self.parent_view.query,self.parent_view.all_results,0,selected))
        try:
            msg=await interaction.original_response(); self.parent_view.bot.register_session(interaction.user.id,msg,{"kind":"search","specialization":self.parent_view.specialization,"query":self.parent_view.query,"page":0,"filter":selected})
        except Exception: pass

class SearchResultsView(discord.ui.View):
    def __init__(self, bot: LibraryBot, specialization: str, query: str, results: list[dict[str, Any]], page: int, selected_filter: str = "الكل"):
        super().__init__(timeout=600)
        self.bot=bot; self.specialization=specialization; self.query=query; self.all_results=results; self.selected_filter=selected_filter
        self.results=filter_search_results(results,selected_filter); self.page=page
        start=page*SEARCH_RESULTS_PER_PAGE; subset=self.results[start:start+SEARCH_RESULTS_PER_PAGE]
        if subset: self.add_item(FileSelect(bot,subset,"اختر نتيجة لفتحها",search_context={"specialization":specialization,"query":query,"results":results,"page":page,"filter":selected_filter}))
        self.add_item(SearchFilterSelect(self))
        total_pages=max(1,(len(self.results)+SEARCH_RESULTS_PER_PAGE-1)//SEARCH_RESULTS_PER_PAGE)
        prev=discord.ui.Button(label="السابق",style=discord.ButtonStyle.secondary,disabled=page<=0,row=2)
        nxt=discord.ui.Button(label="التالي",style=discord.ButtonStyle.secondary,disabled=page>=total_pages-1,row=2)
        page_btn=discord.ui.Button(label=f"{page+1}/{total_pages}",style=discord.ButtonStyle.secondary,disabled=True,row=2)
        home=discord.ui.Button(label="القائمة الرئيسية",style=discord.ButtonStyle.primary,row=2)
        async def go(interaction,new_page):
            await interaction.response.edit_message(embed=search_results_embed(self.query,self.results,new_page),view=SearchResultsView(self.bot,self.specialization,self.query,self.all_results,new_page,self.selected_filter))
            try:
                msg=await interaction.original_response(); self.bot.register_session(interaction.user.id,msg,{"kind":"search","specialization":self.specialization,"query":self.query,"page":new_page,"filter":self.selected_filter})
            except Exception: pass
        prev.callback=lambda i: go(i,max(0,self.page-1)); nxt.callback=lambda i: go(i,min(total_pages-1,self.page+1))
        async def home_cb(interaction): await show_home(interaction,self.bot,self.specialization,edit=True)
        home.callback=home_cb
        for b in (prev,page_btn,nxt,home): self.add_item(b)


class NodeSelect(discord.ui.Select):
    def __init__(self, bot: LibraryBot, specialization: str, parent_path: list[str], items: list[dict[str, Any]]):
        options=[]
        for i,item in enumerate(items[:25]):
            label=safe_label(item["display_name"],100)
            options.append(discord.SelectOption(label=label,value=str(i),emoji=item["emoji"] or "📁"))
        super().__init__(placeholder="اختر من القائمة",options=options,min_values=1,max_values=1)
        self.bot=bot; self.specialization=specialization; self.parent_path=parent_path; self.items=items
    async def callback(self,interaction:discord.Interaction)->None:
        item=self.items[int(self.values[0])]
        await open_path(interaction,self.bot,self.specialization,self.parent_path+[item["key"]])


class BrowseView(discord.ui.View):
    def __init__(self,bot:LibraryBot,specialization:str,path:list[str],items:list[dict[str,Any]],page:int=0):
        super().__init__(timeout=600)
        self.bot=bot; self.specialization=specialization; self.path=path; self.items=items; self.page=page
        visible=[x for x in items if not x["hidden"]]
        total_pages=max(1,(len(visible)+BROWSE_ITEMS_PER_PAGE-1)//BROWSE_ITEMS_PER_PAGE)
        start=page*BROWSE_ITEMS_PER_PAGE; page_items=visible[start:start+BROWSE_ITEMS_PER_PAGE]
        use_select=any(x["render_mode"]=="select" for x in page_items)
        if use_select:
            self.add_item(NodeSelect(bot,specialization,path,page_items))
        else:
            for index,item in enumerate(page_items):
                label=(f'{item["emoji"]} ' if item["emoji"] else '')+item["display_name"]
                button=discord.ui.Button(label=safe_label(label,80),style=UI_STYLE_MAP.get(item["style_name"],discord.ButtonStyle.secondary),row=index//5)
                async def callback(interaction:discord.Interaction,selected:str=item["key"])->None:
                    await open_path(interaction,self.bot,self.specialization,self.path+[selected])
                button.callback=callback; self.add_item(button)
        nav_row=4
        prev=discord.ui.Button(label="السابق",style=discord.ButtonStyle.secondary,disabled=page<=0,row=nav_row)
        page_btn=discord.ui.Button(label=f"{page+1}/{total_pages}",style=discord.ButtonStyle.secondary,disabled=True,row=nav_row)
        nxt=discord.ui.Button(label="التالي",style=discord.ButtonStyle.secondary,disabled=page>=total_pages-1,row=nav_row)
        back=discord.ui.Button(label="رجوع",style=discord.ButtonStyle.danger,disabled=len(path)<=1,row=nav_row)
        home=discord.ui.Button(label="القائمة الرئيسية",style=discord.ButtonStyle.primary,row=nav_row)
        async def prev_cb(interaction): await render_browse(interaction,self.bot,self.specialization,self.path,self.items,max(0,page-1))
        async def next_cb(interaction): await render_browse(interaction,self.bot,self.specialization,self.path,self.items,min(total_pages-1,page+1))
        async def back_cb(interaction): await open_path(interaction,self.bot,self.specialization,self.path[:-1])
        async def home_cb(interaction): await show_home(interaction,self.bot,self.specialization,edit=True)
        prev.callback=prev_cb; nxt.callback=next_cb; back.callback=back_cb; home.callback=home_cb
        for b in (prev,page_btn,nxt,back,home): self.add_item(b)


async def build_browse_payload(bot:LibraryBot,specialization:str,path:list[str],items:list[dict[str,Any]],page:int):
    visible=[x for x in items if not x["hidden"]]
    total_pages=max(1,(len(visible)+BROWSE_ITEMS_PER_PAGE-1)//BROWSE_ITEMS_PER_PAGE)
    page=min(page,total_pages-1)
    display_path=[]
    for i,key in enumerate(path):
        if i==0: display_path.append(key); continue
        parent=path[:i]; resolved=await bot.store.get_ui_children(parent,[key])
        display_path.append(resolved[0]["display_name"] if resolved else key)
    embed=discord.Embed(title=f"مكتبة {specialization}",description=f"📂 **{' > '.join(display_path)}**\nصفحة {page+1}/{total_pages}",color=discord.Color.blurple())
    return embed,BrowseView(bot,specialization,path,items,page)

async def render_browse(interaction:discord.Interaction,bot:LibraryBot,specialization:str,path:list[str],items:list[dict[str,Any]],page:int)->None:
    embed,view=await build_browse_payload(bot,specialization,path,items,page)
    await interaction.response.edit_message(embed=embed,view=view)
    try:
        msg=await interaction.original_response(); bot.register_session(interaction.user.id,msg,{"kind":"browse","specialization":specialization,"path":path,"page":page})
    except Exception: pass


async def open_path(interaction: discord.Interaction, bot: LibraryBot, specialization: str, path: list[str]) -> None:
    if not bot.limiter.allow(interaction.user.id):
        await safe_interaction_error(interaction, "خفف الضغط شوي وجرب بعد ثوانٍ.")
        return
    try:
        node = await bot.store.get_tree_node(path)
        if isinstance(node, str) and is_url(node):
            file = await bot.store.get_file_by_path(path)
            if file:
                await show_file(interaction, bot, file)
            else:
                await interaction.response.send_message(f"📎 {path[-1]}\n{node}", ephemeral=True)
            return
        if isinstance(node, dict) and isinstance(node.get("url"), str):
            file = await bot.store.get_file_by_path(path[:-1] + [str(node.get("name") or path[-1])])
            if file:
                await show_file(interaction, bot, file)
                return
        if not isinstance(node, dict) or not node:
            await safe_interaction_error(interaction, "لا يوجد محتوى هنا حالياً.")
            return
        keys = await bot.store.get_browse_keys(path)
        items = await bot.store.get_ui_children(path, keys)
        await render_browse(interaction, bot, specialization, path, items, 0)
    except Exception:
        logger.exception("فشل فتح المسار %s", path)
        await safe_interaction_error(interaction)


class FileView(discord.ui.View):
    def __init__(self, bot: LibraryBot, file: dict[str, Any], is_favorite: bool, related: list[dict[str, Any]], search_context: Optional[dict[str,Any]]=None):
        super().__init__(timeout=600)
        self.bot = bot
        self.file = file
        self.related_files = related
        self.search_context = search_context
        self.add_item(discord.ui.Button(label="فتح الملف", style=discord.ButtonStyle.link, url=file["url"], row=0))

        fav = discord.ui.Button(
            label="إزالة من المفضلة" if is_favorite else "إضافة للمفضلة",
            style=discord.ButtonStyle.success,
            emoji="⭐",
            row=0,
        )
        report = discord.ui.Button(label="الإبلاغ عن مشكلة", style=discord.ButtonStyle.danger, emoji="⚠️", row=0)
        home = discord.ui.Button(label="القائمة الرئيسية", style=discord.ButtonStyle.primary, row=0)

        async def fav_cb(interaction: discord.Interaction) -> None:
            added = await self.bot.store.toggle_favorite(interaction.user.id, self.file["id"])
            await interaction.response.send_message(
                "تمت إضافة الملف للمفضلة." if added else "تمت إزالة الملف من المفضلة.", ephemeral=True
            )

        async def report_cb(interaction: discord.Interaction) -> None:
            await interaction.response.send_message(
                "اختر المشكلة:", view=ReportReasonView(self.bot, self.file["id"]), ephemeral=True
            )

        async def home_cb(interaction: discord.Interaction) -> None:
            await show_home(interaction, self.bot, self.file["specialization"], edit=True)

        fav.callback = fav_cb
        report.callback = report_cb
        home.callback = home_cb
        self.add_item(fav)
        self.add_item(report)
        copy_btn=discord.ui.Button(label="اسم الملف",style=discord.ButtonStyle.secondary,emoji="📋",row=1)
        async def copy_cb(interaction:discord.Interaction)->None:
            await interaction.response.send_message(f"```{self.file['name']}```",ephemeral=True)
        copy_btn.callback=copy_cb; self.add_item(copy_btn)
        if self.search_context:
            back=discord.ui.Button(label="العودة للنتائج",style=discord.ButtonStyle.secondary,emoji="↩️",row=1)
            async def back_cb(interaction:discord.Interaction)->None:
                c=self.search_context; filtered=filter_search_results(c["results"],c.get("filter","الكل"))
                await interaction.response.edit_message(embed=search_results_embed(c["query"],filtered,c.get("page",0)),view=SearchResultsView(self.bot,c["specialization"],c["query"],c["results"],c.get("page",0),c.get("filter","الكل")))
            back.callback=back_cb; self.add_item(back)
        self.add_item(home)

        if related:
            self.add_item(FileSelect(bot, related, "ملفات مرتبطة قد تفيدك"))


async def show_file(interaction: discord.Interaction, bot: LibraryBot, file: dict[str, Any], search_context: Optional[dict[str,Any]]=None) -> None:
    await bot.store.increment_view(file["id"])
    favorite = await bot.store.is_favorite(interaction.user.id, file["id"])
    related = await bot.store.related(file)
    embed = discord.Embed(
        title=f"📎 {safe_label(file['name'], 240)}",
        description=f"**المسار الكامل**\n`{file['path_text'][:3500]}`",
        color=discord.Color.green(),
    )
    if related:
        embed.set_footer(text="يوجد بالأسفل ملفات مرتبطة اختيارية")
    view = FileView(bot, file, favorite, related, search_context)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    else:
        # فتح الملف من قائمة النتائج يكون أوضح برسالة مستقلة قصيرة.
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class SpecializationSelect(discord.ui.Select):
    def __init__(self, bot: LibraryBot, specs: list[str]):
        super().__init__(
            placeholder="اختر التخصص",
            options=[discord.SelectOption(label=spec, value=spec, emoji="📚") for spec in specs],
        )
        self.bot = bot

    async def callback(self, interaction: discord.Interaction) -> None:
        await show_home(interaction, self.bot, self.values[0], edit=True)


class SpecializationSelectView(discord.ui.View):
    def __init__(self, bot: LibraryBot, specs: list[str]):
        super().__init__(timeout=180)
        self.add_item(SpecializationSelect(bot, specs))


class HomeView(discord.ui.View):
    def __init__(self, bot: LibraryBot, specialization: str):
        super().__init__(timeout=600)
        self.bot = bot
        self.specialization = specialization
        last=discord.ui.Button(label="آخر بحث",style=discord.ButtonStyle.secondary,emoji="🔁",row=1)
        async def last_cb(interaction:discord.Interaction)->None:
            query=await self.bot.store.get_last_search(interaction.user.id,self.specialization)
            if not query:
                await interaction.response.send_message("ما عندك بحث سابق بعد.",ephemeral=True); return
            await interaction.response.defer(ephemeral=True,thinking=True)
            results=await self.bot.store.search(self.specialization,query)
            if not results:
                await interaction.followup.send(f"لم تعد توجد نتائج لـ **{query}**.",ephemeral=True); return
            msg=await interaction.followup.send(embed=search_results_embed(query,results,0),view=SearchResultsView(self.bot,self.specialization,query,results,0),ephemeral=True,wait=True)
            self.bot.register_session(interaction.user.id,msg,{"kind":"search","specialization":self.specialization,"query":query,"page":0,"filter":"الكل"})
        last.callback=last_cb; self.add_item(last)

    @discord.ui.button(label="تصفح المكتبة", style=discord.ButtonStyle.secondary, emoji="📚", row=0)
    async def browse(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await open_path(interaction, self.bot, self.specialization, [self.specialization])

    @discord.ui.button(label="بحث", style=discord.ButtonStyle.primary, emoji="🔍", row=0)
    async def search(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(SearchModal(self.bot, self.specialization))

    @discord.ui.button(label="مفضلتي", style=discord.ButtonStyle.success, emoji="⭐", row=0)
    async def favorites(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        files = await self.bot.store.favorites(interaction.user.id, self.specialization)
        if not files:
            await interaction.response.send_message("قائمة المفضلة فارغة.", ephemeral=True)
            return
        await interaction.response.send_message(
            embed=discord.Embed(
                title="مفضلتي",
                description=f"لديك **{len(files)}** ملف محفوظ.",
                color=discord.Color.gold(),
            ),
            view=SearchResultsView(self.bot, self.specialization, "المفضلة", files, 0),
            ephemeral=True,
        )


async def show_home(interaction: discord.Interaction, bot: LibraryBot, specialization: str, edit: bool = False) -> None:
    embed = discord.Embed(
        title=f"مكتبة {specialization}",
        description="ابحث مباشرة أو تصفح المواد بسرعة.",
        color=discord.Color.blurple(),
    )
    if edit:
        await interaction.response.edit_message(embed=embed, view=HomeView(bot, specialization), content=None)
    else:
        await interaction.response.send_message(embed=embed, view=HomeView(bot, specialization), ephemeral=True)


class OpenLibraryView(discord.ui.View):
    def __init__(self, bot: LibraryBot):
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="افتح المكتبة",
        style=discord.ButtonStyle.success,
        emoji="📚",
        custom_id="academic_library:open:v2",
    )
    async def open_library(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("هذا الزر يعمل داخل السيرفر فقط.", ephemeral=True)
            return
        specs = get_user_specializations(interaction.user)
        if not specs and member_is_admin(interaction.user):
            specs = list(dict.fromkeys(SPECIALIZATION_ROLES.values()))
        if not specs:
            view = role_channel_view(interaction.guild.id if interaction.guild else None)
            await interaction.response.send_message(
                "تأكد أنك أخذت رتبة تخصصك أولاً.", view=view, ephemeral=True
            )
            return
        if len(specs) > 1:
            await interaction.response.send_message(
                "عندك أكثر من رتبة تخصص. توجه إلى قناة المساعدة واطلب من الإدارة إزالة الرتبة الزائدة، ثم افتح المكتبة من جديد.",
                view=help_channel_view(interaction.guild.id if interaction.guild else None),
                ephemeral=True,
            )
            return
        await show_home(interaction, self.bot, specs[0])



async def _admin_children(bot: "LibraryBot", path: list[str]) -> list[dict[str, Any]]:
    node = await bot.store.get_tree_node(path)
    if not isinstance(node, dict):
        return []
    ignored = {"url", "name", "description", "year", "size", "added_at"}
    keys = [str(k) for k in node.keys() if str(k) not in ignored]
    return await bot.store.get_ui_children(path, keys)


async def _admin_folder_payload(bot: "LibraryBot", path: list[str]) -> tuple[str, discord.ui.View]:
    items = await _admin_children(bot, path)
    title = " > ".join(path) if path else "جذر المكتبة"
    text = (
        f"🗂️ **إدارة المجلدات والأزرار**\n"
        f"المكان الحالي: `{title}`\n\n"
        "اختر عنصراً من القائمة، وبعدها تظهر لك كل الخيارات الخاصة به.\n"
        "لا تحتاج لكتابة أي مسار."
    )
    return text, AdminFolderBrowserView(bot, path, items)


class AdminBranchPickerView(discord.ui.View):
    def __init__(self, bot: "LibraryBot"):
        super().__init__(timeout=900)
        self.bot = bot
        branches = list(dict.fromkeys(SPECIALIZATION_ROLES.values()))
        options = [discord.SelectOption(label=b, value=b, emoji="✅") for b in branches]
        select = discord.ui.Select(
            placeholder="اضغط واختر الفرع الذي تريد إدارته",
            options=options[:25],
            min_values=1,
            max_values=1,
        )
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction: discord.Interaction) -> None:
        branch = self.children[0].values[0]
        text, view = await _admin_folder_payload(self.bot, [branch])
        await interaction.response.edit_message(content=text, embed=None, view=view)


class AdminFolderBrowserView(discord.ui.View):
    def __init__(self, bot: "LibraryBot", path: list[str], items: list[dict[str, Any]], page: int = 0):
        super().__init__(timeout=900)
        self.bot = bot
        self.path = list(path)
        self.items = list(items)
        self.page = max(0, int(page))

        per_page = 20
        start = self.page * per_page
        shown = self.items[start:start + per_page]

        if shown:
            options = []
            for item in shown:
                label = safe_label(str(item.get("display_name") or item.get("key") or "عنصر"), 95)
                flags = []
                if item.get("hidden"):
                    flags.append("مخفي")
                if int(item.get("position") or 0) < 0:
                    flags.append("مثبت")
                description = " • ".join(flags) or "اضغط لإدارة هذا العنصر"
                options.append(
                    discord.SelectOption(
                        label=label,
                        value=str(item["key"]),
                        description=description[:100],
                        emoji="📁",
                    )
                )
            select = discord.ui.Select(
                placeholder="اختر مجلداً أو عنصراً لإدارته",
                options=options,
                min_values=1,
                max_values=1,
                row=0,
            )
            select.callback = self._select_item
            self.add_item(select)

        if self.page > 0:
            prev = discord.ui.Button(label="السابق", emoji="◀️", style=discord.ButtonStyle.secondary, row=1)
            prev.callback = self._previous_page
            self.add_item(prev)
        if start + per_page < len(self.items):
            nxt = discord.ui.Button(label="التالي", emoji="▶️", style=discord.ButtonStyle.secondary, row=1)
            nxt.callback = self._next_page
            self.add_item(nxt)

        if len(self.path) > 1:
            back = discord.ui.Button(label="رجوع", emoji="↩️", style=discord.ButtonStyle.secondary, row=2)
            back.callback = self._back
            self.add_item(back)

        home = discord.ui.Button(label="اختيار فرع آخر", emoji="🏠", style=discord.ButtonStyle.secondary, row=2)
        home.callback = self._home
        self.add_item(home)

        sort_btn = discord.ui.Button(label="ترتيب تلقائي", emoji="🔤", style=discord.ButtonStyle.primary, row=3)
        sort_btn.callback = self._sort
        self.add_item(sort_btn)

        preview = discord.ui.Button(label="معاينة", emoji="👁️", style=discord.ButtonStyle.primary, row=3)
        preview.callback = self._preview
        self.add_item(preview)

    async def _select_item(self, interaction: discord.Interaction) -> None:
        select = next(c for c in self.children if isinstance(c, discord.ui.Select))
        key = select.values[0]
        item = next((x for x in self.items if str(x.get("key")) == key), {"key": key, "display_name": key})
        node = await self.bot.store.get_tree_node(self.path + [key])
        is_folder = isinstance(node, dict) and not await self.bot.store.get_file_by_path(self.path + [key])
        await interaction.response.edit_message(
            content=(
                f"⚙️ **إدارة العنصر**\n"
                f"المجلد: `{' > '.join(self.path)}`\n"
                f"العنصر: **{item.get('display_name') or key}**\n\n"
                "اختر العملية التي تريدها:"
            ),
            view=AdminNodeActionsView(self.bot, self.path, item, is_folder),
        )

    async def _previous_page(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(view=AdminFolderBrowserView(self.bot, self.path, self.items, self.page - 1))

    async def _next_page(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(view=AdminFolderBrowserView(self.bot, self.path, self.items, self.page + 1))

    async def _back(self, interaction: discord.Interaction) -> None:
        text, view = await _admin_folder_payload(self.bot, self.path[:-1])
        await interaction.response.edit_message(content=text, view=view)

    async def _home(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            content="🗂️ **إدارة المجلدات والأزرار**\nاختر الفرع:",
            embed=None,
            view=AdminBranchPickerView(self.bot),
        )

    async def _sort(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            content=f"🔤 اختر نوع الترتيب للمجلد:\n`{' > '.join(self.path)}`",
            view=AdminSortView(self.bot, self.path),
        )

    async def _preview(self, interaction: discord.Interaction) -> None:
        node = await self.bot.store.get_tree_node(self.path)
        if not isinstance(node, dict):
            return await interaction.response.send_message("هذا المكان ليس مجلداً.", ephemeral=True)
        keys = [str(k) for k in node.keys() if str(k) not in {"url","name","description","year","size","added_at"}]
        items = await self.bot.store.get_ui_children(self.path, keys)
        spec = self.path[0] if self.path else "الإدارة"
        embed = discord.Embed(
            title="معاينة واجهة المجلد",
            description=f"`{' > '.join(self.path)}`",
            color=discord.Color.blurple(),
        )
        await interaction.response.send_message(embed=embed, view=BrowseView(self.bot, spec, self.path, items, 0), ephemeral=True)


class AdminNodeActionsView(discord.ui.View):
    def __init__(self, bot: "LibraryBot", parent_path: list[str], item: dict[str, Any], is_folder: bool):
        super().__init__(timeout=900)
        self.bot = bot
        self.parent_path = list(parent_path)
        self.item = dict(item)
        self.key = str(item.get("key"))
        self.is_folder = bool(is_folder)

        if self.is_folder:
            enter = discord.ui.Button(label="فتح المجلد", emoji="📂", style=discord.ButtonStyle.success, row=0)
            enter.callback = self._enter
            self.add_item(enter)

        rename = discord.ui.Button(label="تعديل الاسم", emoji="✏️", style=discord.ButtonStyle.primary, row=0)
        rename.callback = self._rename
        self.add_item(rename)

        color = discord.ui.Button(label="لون الزر", emoji="🎨", style=discord.ButtonStyle.primary, row=0)
        color.callback = self._color
        self.add_item(color)

        hidden = bool(self.item.get("hidden"))
        vis = discord.ui.Button(
            label=("إظهار" if hidden else "إخفاء"),
            emoji=("👁️" if hidden else "🙈"),
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        vis.callback = self._toggle_hidden
        self.add_item(vis)

        pin = discord.ui.Button(label="تثبيت بالأعلى", emoji="📌", style=discord.ButtonStyle.secondary, row=1)
        pin.callback = self._pin
        self.add_item(pin)

        for label, emoji, direction in [
            ("أعلى", "⬆️", "اعلى"),
            ("أسفل", "⬇️", "اسفل"),
            ("أول", "⏮️", "اول"),
            ("آخر", "⏭️", "اخر"),
        ]:
            b = discord.ui.Button(label=label, emoji=emoji, style=discord.ButtonStyle.secondary, row=2)
            b.callback = self._move(direction)
            self.add_item(b)

        reset = discord.ui.Button(label="إعادة افتراضي", emoji="↪️", style=discord.ButtonStyle.danger, row=3)
        reset.callback = self._reset
        self.add_item(reset)

        back = discord.ui.Button(label="رجوع للمجلد", emoji="↩️", style=discord.ButtonStyle.secondary, row=3)
        back.callback = self._back
        self.add_item(back)

    async def _refresh_parent(self, interaction: discord.Interaction, note: str = "") -> None:
        text, view = await _admin_folder_payload(self.bot, self.parent_path)
        if note:
            text = f"{note}\n\n{text}"
        if interaction.response.is_done():
            await interaction.edit_original_response(content=text, view=view, embed=None)
        else:
            await interaction.response.edit_message(content=text, view=view, embed=None)

    async def _enter(self, interaction: discord.Interaction) -> None:
        text, view = await _admin_folder_payload(self.bot, self.parent_path + [self.key])
        await interaction.response.edit_message(content=text, view=view, embed=None)

    async def _rename(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(AdminRenameModal(self.bot, self.parent_path, self.key))

    async def _color(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            content=f"🎨 اختر لون **{self.item.get('display_name') or self.key}**:",
            view=AdminColorView(self.bot, self.parent_path, self.key),
        )

    async def _toggle_hidden(self, interaction: discord.Interaction) -> None:
        new_value = 0 if bool(self.item.get("hidden")) else 1
        await self.bot.store.set_ui_override(interaction.user.id, self.parent_path, self.key, hidden=new_value)
        await self._refresh_parent(interaction, "✅ تم تحديث حالة الظهور.")

    async def _pin(self, interaction: discord.Interaction) -> None:
        await self.bot.store.set_ui_override(interaction.user.id, self.parent_path, self.key, position=-100000)
        await self._refresh_parent(interaction, "✅ تم تثبيت العنصر في الأعلى.")

    def _move(self, direction: str):
        async def callback(interaction: discord.Interaction) -> None:
            await self.bot.store.move_ui_node(interaction.user.id, self.parent_path, self.key, direction)
            await self._refresh_parent(interaction, f"✅ تم تحريك العنصر: **{direction}**.")
        return callback

    async def _reset(self, interaction: discord.Interaction) -> None:
        await self.bot.store.reset_ui_node(interaction.user.id, self.parent_path, self.key)
        await self._refresh_parent(interaction, "✅ رجع العنصر للوضع الافتراضي.")

    async def _back(self, interaction: discord.Interaction) -> None:
        await self._refresh_parent(interaction)


class AdminRenameModal(discord.ui.Modal, title="تعديل الاسم الظاهر"):
    new_name = discord.ui.TextInput(label="الاسم الجديد", max_length=80)

    def __init__(self, bot: "LibraryBot", parent_path: list[str], key: str):
        super().__init__()
        self.bot = bot
        self.parent_path = list(parent_path)
        self.key = key

    async def on_submit(self, interaction: discord.Interaction) -> None:
        name = str(self.new_name.value).strip()
        if not name:
            return await interaction.response.send_message("الاسم لا يمكن أن يكون فارغاً.", ephemeral=True)
        await self.bot.store.set_ui_override(
            interaction.user.id, self.parent_path, self.key, display_name=name
        )
        text, view = await _admin_folder_payload(self.bot, self.parent_path)
        await interaction.response.edit_message(content=f"✅ تم تغيير الاسم إلى **{name}**.\n\n{text}", view=view)


class AdminColorView(discord.ui.View):
    COLORS = {
        "أزرق": "ازرق",
        "رمادي": "رمادي",
        "أخضر": "اخضر",
        "أحمر": "احمر",
    }

    def __init__(self, bot: "LibraryBot", parent_path: list[str], key: str):
        super().__init__(timeout=600)
        self.bot = bot
        self.parent_path = list(parent_path)
        self.key = key
        styles = {
            "أزرق": discord.ButtonStyle.primary,
            "رمادي": discord.ButtonStyle.secondary,
            "أخضر": discord.ButtonStyle.success,
            "أحمر": discord.ButtonStyle.danger,
        }
        for label, value in self.COLORS.items():
            b = discord.ui.Button(label=label, style=styles[label])
            b.callback = self._set_color(value)
            self.add_item(b)
        back = discord.ui.Button(label="رجوع", emoji="↩️", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._back
        self.add_item(back)

    def _set_color(self, value: str):
        async def callback(interaction: discord.Interaction) -> None:
            await self.bot.store.set_ui_override(
                interaction.user.id, self.parent_path, self.key, style_name=value
            )
            text, view = await _admin_folder_payload(self.bot, self.parent_path)
            await interaction.response.edit_message(content=f"✅ تم تغيير اللون.\n\n{text}", view=view)
        return callback

    async def _back(self, interaction: discord.Interaction) -> None:
        text, view = await _admin_folder_payload(self.bot, self.parent_path)
        await interaction.response.edit_message(content=text, view=view)


class AdminSortView(discord.ui.View):
    def __init__(self, bot: "LibraryBot", path: list[str]):
        super().__init__(timeout=600)
        self.bot = bot
        self.path = list(path)
        for label, value in [
            ("أبجدي", "ابجدي"),
            ("أبجدي عكسي", "عكسي"),
            ("سنوات تنازلي", "سنوات_تنازلي"),
            ("سنوات تصاعدي", "سنوات_تصاعدي"),
        ]:
            b = discord.ui.Button(label=label, style=discord.ButtonStyle.primary)
            b.callback = self._sort(value)
            self.add_item(b)
        back = discord.ui.Button(label="رجوع", emoji="↩️", style=discord.ButtonStyle.secondary, row=1)
        back.callback = self._back
        self.add_item(back)

    def _sort(self, value: str):
        async def callback(interaction: discord.Interaction) -> None:
            count = await self.bot.store.auto_sort_ui(interaction.user.id, self.path, value)
            text, view = await _admin_folder_payload(self.bot, self.path)
            await interaction.response.edit_message(content=f"✅ تم ترتيب **{count}** عنصراً.\n\n{text}", view=view)
        return callback

    async def _back(self, interaction: discord.Interaction) -> None:
        text, view = await _admin_folder_payload(self.bot, self.path)
        await interaction.response.edit_message(content=text, view=view)


class AdminPanelActionsView(discord.ui.View):
    def __init__(self, bot: "LibraryBot"):
        super().__init__(timeout=600)
        self.bot = bot

    @discord.ui.button(label="نشر اللوحة هنا", emoji="📌", style=discord.ButtonStyle.success)
    async def publish(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild or not isinstance(interaction.channel, discord.abc.Messageable):
            return await interaction.response.send_message("استخدمها داخل السيرفر.", ephemeral=True)
        old = await self.bot.store.get_panel(interaction.guild.id)
        if old:
            return await interaction.response.send_message("توجد لوحة محفوظة بالفعل. حدّثها أو احذفها أولاً.", ephemeral=True)
        embed = discord.Embed(
            title="مكتبة الفهرس الأكاديمي",
            description="اضغط الزر لفتح مكتبة تخصصك.",
            color=discord.Color.green(),
        )
        message = await interaction.channel.send(embed=embed, view=OpenLibraryView(self.bot))
        await self.bot.store.save_panel(interaction.guild.id, interaction.channel_id, message.id)
        await interaction.response.send_message(f"✅ تم نشر اللوحة: {message.jump_url}", ephemeral=True)

    @discord.ui.button(label="تحديث نفس اللوحة", emoji="🔄", style=discord.ButtonStyle.primary)
    async def update(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild:
            return await interaction.response.send_message("استخدمها داخل السيرفر.", ephemeral=True)
        panel = await self.bot.store.get_panel(interaction.guild.id)
        if not panel:
            return await interaction.response.send_message("لا توجد لوحة محفوظة.", ephemeral=True)
        try:
            channel = self.bot.get_channel(panel["channel_id"]) or await self.bot.fetch_channel(panel["channel_id"])
            message = await channel.fetch_message(panel["message_id"])
            embed = discord.Embed(
                title="مكتبة الفهرس الأكاديمي",
                description="اضغط الزر لفتح مكتبة تخصصك.",
                color=discord.Color.green(),
            )
            await message.edit(embed=embed, view=OpenLibraryView(self.bot))
            await self.bot.store.sync_from_materials(force=True)
            await interaction.response.send_message("✅ تم تحديث **نفس الرسالة**.", ephemeral=True)
        except Exception as exc:
            await interaction.response.send_message(f"❌ تعذر تحديث اللوحة: {exc}", ephemeral=True)

    @discord.ui.button(label="حذف اللوحة", emoji="🗑️", style=discord.ButtonStyle.danger)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not interaction.guild:
            return await interaction.response.send_message("استخدمها داخل السيرفر.", ephemeral=True)
        panel = await self.bot.store.get_panel(interaction.guild.id)
        if not panel:
            return await interaction.response.send_message("لا توجد لوحة محفوظة.", ephemeral=True)
        try:
            channel = self.bot.get_channel(panel["channel_id"]) or await self.bot.fetch_channel(panel["channel_id"])
            message = await channel.fetch_message(panel["message_id"])
            await message.delete()
        except Exception:
            pass
        await self.bot.store.delete_panel_record(interaction.guild.id)
        await interaction.response.send_message("✅ تم حذف اللوحة.", ephemeral=True)


class AdminDashboardView(discord.ui.View):
    def __init__(self, bot: "LibraryBot"):
        super().__init__(timeout=900)
        self.bot = bot

    async def _admin_check(self, interaction: discord.Interaction) -> bool:
        if not isinstance(interaction.user, discord.Member) or not member_is_admin(interaction.user):
            await interaction.response.send_message("هذا الزر للإدارة فقط.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="إدارة المجلدات والأزرار", style=discord.ButtonStyle.success, emoji="🗂️", row=0)
    async def folders(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        await interaction.response.edit_message(
            content="🗂️ **إدارة المجلدات والأزرار**\nاختر الفرع:",
            embed=None,
            view=AdminBranchPickerView(self.bot),
        )

    @discord.ui.button(label="لوحة المكتبة", style=discord.ButtonStyle.primary, emoji="📚", row=0)
    async def panel(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        await interaction.response.edit_message(
            content="📚 اختر ما تريد عمله بلوحة المكتبة المنشورة:",
            embed=None,
            view=AdminPanelActionsView(self.bot),
        )

    @discord.ui.button(label="الإحصائيات", style=discord.ButtonStyle.secondary, emoji="📊", row=1)
    async def stats(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        stats = await self.bot.store.stats()
        await interaction.response.send_message(embed=admin_stats_embed(stats), ephemeral=True)

    @discord.ui.button(label="أحدث الملفات", style=discord.ButtonStyle.secondary, emoji="🆕", row=1)
    async def latest(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        latest = await self.bot.store.latest(20)
        lines = [f"• **{safe_label(x['name'], 100)}**\n`{x['path_text'][:180]}`" for x in latest]
        embed = discord.Embed(title="أحدث الملفات", description="\n".join(lines)[:4000] or "لا يوجد ملفات.", color=discord.Color.teal())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="طلبات الملفات الناقصة", style=discord.ButtonStyle.secondary, emoji="📥", row=1)
    async def missing(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        rows = await self.bot.store.missing_requests_summary(25)
        text = "\n".join(f"• **{r['query']}** — {r['specialization']} — {r['c']} طلب" for r in rows) or "لا توجد طلبات."
        await interaction.response.send_message(
            embed=discord.Embed(title="طلبات الملفات الناقصة", description=text[:4000], color=discord.Color.orange()),
            ephemeral=True,
        )

    @discord.ui.button(label="فحص الواجهة", style=discord.ButtonStyle.secondary, emoji="🧪", row=2)
    async def diagnostics(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        d = await self.bot.store.ui_diagnostics()
        embed = discord.Embed(title="فحص واجهة المكتبة", color=discord.Color.orange())
        embed.add_field(name="تعديلات العرض", value=str(d["total"]))
        embed.add_field(name="تعارض مواقع", value=str(len(d["duplicate_positions"])))
        embed.add_field(name="أسماء طويلة", value=str(len(d["long_names"])))
        embed.add_field(name="عناصر مخفية", value=str(len(d["hidden"])))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="سجل التعديلات", style=discord.ButtonStyle.secondary, emoji="🧾", row=2)
    async def history(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        rows = await self.bot.store.ui_history(15)
        text = "\n".join(
            f"`#{r['id']}` <@{r['user_id']}> — {r['action']} — `{r['node_id'] or '-'}`"
            for r in rows
        ) or "لا يوجد سجل."
        await interaction.response.send_message(text[:1900], ephemeral=True)

    @discord.ui.button(label="تراجع عن آخر تعديل", style=discord.ButtonStyle.secondary, emoji="↩️", row=2)
    async def undo(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        row = await self.bot.store.undo_last_ui(interaction.user.id)
        await interaction.response.send_message(
            "✅ تم التراجع عن آخر تعديل." if row else "لا يوجد تعديل يمكن التراجع عنه.",
            ephemeral=True,
        )

    @discord.ui.button(label="حالة النظام", style=discord.ButtonStyle.secondary, emoji="🩺", row=3)
    async def health(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        data = self.bot.health_state or await self.bot.store.health_snapshot()
        text = (
            f"**SQLite:** {data.get('db_check', 'غير معروف')}\n"
            f"**الملفات:** {data.get('active_files', 0):,}\n"
            f"**الجلسات المفتوحة:** {len(self.bot.active_sessions):,}\n"
            f"**Discord latency:** {data.get('latency_ms', self.bot.latency*1000):.1f} ms\n"
            f"**متوسط البحث:** {data.get('search_avg_ms', 0):.2f} ms\n"
            f"**RAM:** {data.get('rss_mb') if data.get('rss_mb') is not None else 'غير متاح'} MB"
        )
        await interaction.response.send_message(
            embed=discord.Embed(title="حالة نظام المكتبة", description=text, color=discord.Color.green()),
            ephemeral=True,
        )

    @discord.ui.button(label="اختبار ضغط سريع", style=discord.ButtonStyle.secondary, emoji="⚡", row=3)
    async def load_test(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not await self._admin_check(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.bot.internal_load_test(1000, 100)
        text = (
            f"العمليات: **{result['operations']:,}**\n"
            f"التزامن: **{result['concurrency']:,}**\n"
            f"المدة: **{result['total_seconds']:.2f}s**\n"
            f"العمليات/ثانية: **{result['ops_per_second']:.1f}**\n"
            f"P95: **{result['p95_ms']:.2f}ms**"
        )
        await interaction.edit_original_response(
            embed=discord.Embed(title="اختبار ضغط المكتبة", description=text, color=discord.Color.blurple())
        )

def admin_stats_embed(stats: dict[str, Any]) -> discord.Embed:
    embed = discord.Embed(title="إحصائيات المكتبة", color=discord.Color.blurple(), timestamp=discord.utils.utcnow())
    embed.description = (
        f"**الملفات:** {stats['total']:,}\n"
        f"**مرات الفتح:** {stats['views']:,}\n"
        f"**المفضلة:** {stats['favorites']:,}\n"
        f"**عمليات البحث:** {stats['searches']:,}\n"
        f"**البلاغات المفتوحة:** {stats['reports']:,}"
    )
    by_spec = "\n".join(f"• {x['specialization']}: {x['c']}" for x in stats["by_spec"]) or "لا يوجد"
    embed.add_field(name="الملفات حسب الفرع", value=by_spec[:1024], inline=False)
    top = "\n".join(f"• {x['name']} — {x['views']} فتح" for x in stats["top_files"]) or "لا يوجد"
    embed.add_field(name="الأكثر فتحاً", value=top[:1024], inline=False)
    missing = "\n".join(f"• {x['query']} — {x['c']} مرة" for x in stats["no_results"]) or "لا يوجد"
    embed.add_field(name="بحث بلا نتائج", value=missing[:1024], inline=False)
    return embed


# =============================================================================
# خادم المزامنة البعيدة للمكتبة
# =============================================================================
_library_web_app = Flask("tawjihi-library-sync")
_library_runtime_bot: Optional[LibraryBot] = None
_library_runtime_loop: Optional[asyncio.AbstractEventLoop] = None

@_library_web_app.get("/")
def library_health():
    bot = _library_runtime_bot
    return jsonify(
        ok=True,
        bot_ready=bool(bot and bot.is_ready()),
        files=(len(bot.store.files_cache) if bot else 0),
        remote_configured=bool(UPLOAD_REMOTE_URL and LIBRARY_SYNC_TOKEN),
    )

@_library_web_app.post("/api/library/sync")
def library_sync_webhook():
    auth = request.headers.get("Authorization", "")
    if not LIBRARY_SYNC_TOKEN or auth != f"Bearer {LIBRARY_SYNC_TOKEN}":
        return jsonify(ok=False, error="unauthorized"), 401
    bot, loop = _library_runtime_bot, _library_runtime_loop
    if not bot or not loop or loop.is_closed():
        return jsonify(ok=False, error="bot_not_ready"), 503
    future = asyncio.run_coroutine_threadsafe(bot.sync_from_remote(force=True), loop)
    try:
        changed = future.result(timeout=max(3.0, REMOTE_SYNC_TIMEOUT_SECONDS + 2.0))
        return jsonify(ok=True, changed=bool(changed))
    except Exception as exc:
        logger.exception("فشل webhook مزامنة المكتبة")
        return jsonify(ok=False, error=str(exc)[:300]), 500

def run_library_web() -> None:
    _library_web_app.run(host=LIBRARY_WEB_HOST, port=LIBRARY_WEB_PORT, threaded=True, use_reloader=False)


def build_library_bot() -> LibraryBot:
    bot = LibraryBot()
    original_setup_hook = bot.setup_hook

    async def _registered_setup_hook() -> None:
        global _library_runtime_bot, _library_runtime_loop
        _library_runtime_bot = bot
        _library_runtime_loop = asyncio.get_running_loop()
        await original_setup_hook()

    bot.setup_hook = _registered_setup_hook

    @bot.tree.command(name="فتح_المكتبة", description="نشر لوحة فتح مكتبة المواد")
    async def publish_library(interaction: discord.Interaction) -> None:
        if not await require_library_admin(interaction):
            return
        if not interaction.guild or not isinstance(interaction.channel, discord.abc.Messageable):
            await interaction.response.send_message("استخدم الأمر داخل السيرفر.", ephemeral=True)
            return
        if LIBRARY_ADMIN_CHANNEL_ID and interaction.channel_id != LIBRARY_ADMIN_CHANNEL_ID:
            await interaction.response.send_message(
                f"استخدم الأمر في قناة الإدارة المحددة: <#{LIBRARY_ADMIN_CHANNEL_ID}>", ephemeral=True
            )
            return

        old = await bot.store.get_panel(interaction.guild.id)
        if old:
            try:
                old_channel = bot.get_channel(old["channel_id"]) or await bot.fetch_channel(old["channel_id"])
                old_message = await old_channel.fetch_message(old["message_id"])
                await interaction.response.send_message(
                    f"لوحة المكتبة منشورة بالفعل: {old_message.jump_url}", ephemeral=True
                )
                return
            except Exception:
                await bot.store.delete_panel_record(interaction.guild.id)

        await interaction.response.send_message("جاري نشر اللوحة…", ephemeral=True)
        embed = discord.Embed(
            title="مكتبة الفهرس الأكاديمي",
            description="اضغط الزر لفتح مكتبة تخصصك.",
            color=discord.Color.green(),
        )
        message = await interaction.channel.send(embed=embed, view=OpenLibraryView(bot))
        await bot.store.save_panel(interaction.guild.id, interaction.channel_id, message.id)
        await interaction.edit_original_response(content=f"تم نشر لوحة المكتبة: {message.jump_url}")

    @bot.tree.command(name="تحديث_لوحة_المكتبة", description="تحديث رسالة لوحة المكتبة المنشورة")
    async def update_library_panel(interaction: discord.Interaction) -> None:
        if not await require_library_admin(interaction):
            return
        if not interaction.guild:
            await interaction.response.send_message("استخدم الأمر داخل السيرفر.", ephemeral=True)
            return
        panel = await bot.store.get_panel(interaction.guild.id)
        if not panel:
            await interaction.response.send_message("لا توجد لوحة محفوظة. استخدم /فتح_المكتبة.", ephemeral=True)
            return
        try:
            channel = bot.get_channel(panel["channel_id"]) or await bot.fetch_channel(panel["channel_id"])
            message = await channel.fetch_message(panel["message_id"])
            embed = discord.Embed(
                title="مكتبة الفهرس الأكاديمي",
                description="اضغط الزر لفتح مكتبة تخصصك.",
                color=discord.Color.green(),
            )
            await message.edit(embed=embed, view=OpenLibraryView(bot))
            await bot.store.sync_from_materials(force=True)
            await interaction.response.send_message("تم تحديث اللوحة والمكتبة.", ephemeral=True)
        except Exception:
            logger.exception("تعذر تحديث لوحة المكتبة")
            await interaction.response.send_message("تعذر العثور على اللوحة أو تعديلها.", ephemeral=True)

    @bot.tree.command(name="حذف_لوحة_المكتبة", description="حذف لوحة المكتبة المنشورة")
    async def delete_library_panel(interaction: discord.Interaction) -> None:
        if not await require_library_admin(interaction):
            return
        if not interaction.guild:
            await interaction.response.send_message("استخدم الأمر داخل السيرفر.", ephemeral=True)
            return
        panel = await bot.store.get_panel(interaction.guild.id)
        if not panel:
            await interaction.response.send_message("لا توجد لوحة محفوظة.", ephemeral=True)
            return
        try:
            channel = bot.get_channel(panel["channel_id"]) or await bot.fetch_channel(panel["channel_id"])
            message = await channel.fetch_message(panel["message_id"])
            await message.delete()
        except Exception:
            pass
        await bot.store.delete_panel_record(interaction.guild.id)
        await interaction.response.send_message("تم حذف لوحة المكتبة.", ephemeral=True)

    @bot.tree.command(name="إدارة_المكتبة", description="فتح لوحة إدارة المكتبة")
    async def admin_library(interaction: discord.Interaction) -> None:
        if not await require_library_admin(interaction):
            return
        if LIBRARY_ADMIN_CHANNEL_ID and interaction.channel_id != LIBRARY_ADMIN_CHANNEL_ID:
            await interaction.response.send_message(
                f"لوحة الإدارة متاحة في <#{LIBRARY_ADMIN_CHANNEL_ID}> فقط.", ephemeral=True
            )
            return
        stats = await bot.store.stats()
        await interaction.response.send_message(
            embed=admin_stats_embed(stats), view=AdminDashboardView(bot), ephemeral=True
        )

    move_choices=[app_commands.Choice(name="أعلى",value="اعلى"),app_commands.Choice(name="أسفل",value="اسفل"),app_commands.Choice(name="يمين",value="يمين"),app_commands.Choice(name="يسار",value="يسار"),app_commands.Choice(name="أول القائمة",value="اول"),app_commands.Choice(name="آخر القائمة",value="اخر")]
    style_choices=[app_commands.Choice(name="أزرق",value="ازرق"),app_commands.Choice(name="رمادي",value="رمادي"),app_commands.Choice(name="أخضر",value="اخضر"),app_commands.Choice(name="أحمر",value="احمر")]

    @bot.tree.command(name="تعديل_مكان_زر",description="تحريك زر داخل المجلد بدون تغيير مساره الحقيقي")
    @app_commands.describe(المجلد="المسار الداخلي مثل علمي/فيزياء",العنصر="الاسم الداخلي للعنصر",الحركة="اتجاه الحركة")
    @app_commands.choices(الحركة=move_choices)
    async def move_button(interaction:discord.Interaction,المجلد:str,العنصر:str,الحركة:app_commands.Choice[str])->None:
        if not await require_library_admin(interaction): return
        await interaction.response.defer(ephemeral=True,thinking=True)
        try:
            result=await bot.store.move_ui_node(interaction.user.id,parse_internal_path(المجلد),العنصر,الحركة.value)
            await interaction.followup.send(f"تم تحريك **{العنصر}** من الموقع {result['from']+1} إلى {result['to']+1}.",ephemeral=True)
        except Exception as e: await interaction.followup.send(f"تعذر التحريك: {e}",ephemeral=True)

    @bot.tree.command(name="تحديد_مكان_زر",description="وضع زر في رقم محدد من 1 إلى 20 داخل الصفحة")
    async def set_button_position(interaction:discord.Interaction,المجلد:str,العنصر:str,الموقع:app_commands.Range[int,1,20])->None:
        if not await require_library_admin(interaction): return
        await interaction.response.defer(ephemeral=True,thinking=True)
        try:
            result=await bot.store.move_ui_node(interaction.user.id,parse_internal_path(المجلد),العنصر,"",الموقع-1)
            await interaction.followup.send(f"تم وضع **{العنصر}** في الموقع {result['to']+1}.",ephemeral=True)
        except Exception as e: await interaction.followup.send(f"تعذر التعديل: {e}",ephemeral=True)

    @bot.tree.command(name="تعديل_اسم_العرض",description="تغيير الاسم الظاهر فقط دون التأثير على المزامنة أو المسار")
    async def rename_display(interaction:discord.Interaction,المجلد:str,العنصر:str,الاسم_الجديد:app_commands.Range[str,1,80])->None:
        if not await require_library_admin(interaction): return
        await bot.store.set_ui_override(interaction.user.id,parse_internal_path(المجلد),العنصر,display_name=الاسم_الجديد)
        await interaction.response.send_message(f"تم تغيير الاسم الظاهر إلى **{الاسم_الجديد}**، والمسار الداخلي بقي كما هو.",ephemeral=True)

    @bot.tree.command(name="تعديل_شكل_زر",description="تغيير لون وإيموجي وطريقة عرض زر")
    @app_commands.choices(اللون=style_choices)
    async def style_button(interaction:discord.Interaction,المجلد:str,العنصر:str,اللون:app_commands.Choice[str],الايموجي:Optional[str]=None,قائمة_منسدلة:bool=False)->None:
        if not await require_library_admin(interaction): return
        await bot.store.set_ui_override(interaction.user.id,parse_internal_path(المجلد),العنصر,style_name=اللون.value,emoji=(الايموجي or None),render_mode=("select" if قائمة_منسدلة else "button"))
        await interaction.response.send_message("تم تعديل شكل العنصر.",ephemeral=True)

    @bot.tree.command(name="إخفاء_عنصر_مكتبة",description="إخفاء عنصر من الطلاب دون حذفه أو تغيير مساره")
    async def hide_node(interaction:discord.Interaction,المجلد:str,العنصر:str,مخفي:bool=True)->None:
        if not await require_library_admin(interaction): return
        await bot.store.set_ui_override(interaction.user.id,parse_internal_path(المجلد),العنصر,hidden=int(مخفي))
        await interaction.response.send_message("تم إخفاء العنصر." if مخفي else "تم إظهار العنصر.",ephemeral=True)

    @bot.tree.command(name="إعادة_ضبط_شكل_عنصر",description="إعادة الاسم والترتيب واللون للوضع الأصلي")
    async def reset_node_ui(interaction:discord.Interaction,المجلد:str,العنصر:str)->None:
        if not await require_library_admin(interaction): return
        await bot.store.reset_ui_node(interaction.user.id,parse_internal_path(المجلد),العنصر)
        await interaction.response.send_message("تمت إعادة العنصر للوضع الأصلي القادم من المزامنة.",ephemeral=True)

    sort_choices=[app_commands.Choice(name="أبجدي",value="ابجدي"),app_commands.Choice(name="أبجدي عكسي",value="عكسي"),app_commands.Choice(name="السنوات تنازلياً",value="سنوات_تنازلي"),app_commands.Choice(name="السنوات تصاعدياً",value="سنوات_تصاعدي")]
    @bot.tree.command(name="ترتيب_تلقائي_للمجلد",description="ترتيب عناصر مجلد تلقائياً")
    @app_commands.choices(النوع=sort_choices)
    async def auto_sort_folder(interaction:discord.Interaction,المجلد:str,النوع:app_commands.Choice[str])->None:
        if not await require_library_admin(interaction): return
        await interaction.response.defer(ephemeral=True,thinking=True)
        try:
            count=await bot.store.auto_sort_ui(interaction.user.id,parse_internal_path(المجلد),النوع.value)
            await interaction.followup.send(f"تم ترتيب {count} عنصراً.",ephemeral=True)
        except Exception as e: await interaction.followup.send(f"فشل الترتيب: {e}",ephemeral=True)

    @bot.tree.command(name="نسخ_تنسيق_مجلد",description="نسخ ترتيب وألوان وأسماء العرض بين مجلدين متشابهين")
    async def copy_folder_format(interaction:discord.Interaction,المصدر:str,الهدف:str)->None:
        if not await require_library_admin(interaction): return
        await interaction.response.defer(ephemeral=True,thinking=True)
        try:
            count=await bot.store.copy_ui_format(interaction.user.id,parse_internal_path(المصدر),parse_internal_path(الهدف))
            await interaction.followup.send(f"تم نسخ تنسيق {count} عنصراً مشتركاً.",ephemeral=True)
        except Exception as e: await interaction.followup.send(f"فشل النسخ: {e}",ephemeral=True)

    @bot.tree.command(name="حفظ_قالب_واجهة",description="حفظ تنسيق مجلد كقالب قابل لإعادة الاستخدام")
    async def save_ui_template_cmd(interaction:discord.Interaction,اسم_القالب:str,المجلد:str)->None:
        if not await require_library_admin(interaction): return
        count=await bot.store.save_ui_template(interaction.user.id,اسم_القالب,parse_internal_path(المجلد))
        await interaction.response.send_message(f"تم حفظ القالب **{اسم_القالب}** وفيه {count} عنصراً.",ephemeral=True)

    @bot.tree.command(name="تطبيق_قالب_واجهة",description="تطبيق قالب محفوظ على مجلد")
    async def apply_ui_template_cmd(interaction:discord.Interaction,اسم_القالب:str,المجلد:str)->None:
        if not await require_library_admin(interaction): return
        count=await bot.store.apply_ui_template(interaction.user.id,اسم_القالب,parse_internal_path(المجلد))
        await interaction.response.send_message(f"تم تطبيق القالب على {count} عنصراً متطابقاً.",ephemeral=True)

    @bot.tree.command(name="معاينة_ترتيب_مجلد",description="عرض المجلد كما سيظهر للطلاب دون نشر شيء")
    async def preview_folder(interaction:discord.Interaction,المجلد:str)->None:
        if not await require_library_admin(interaction): return
        path=parse_internal_path(المجلد); node=await bot.store.get_tree_node(path)
        if not isinstance(node,dict): await interaction.response.send_message("المسار ليس مجلداً صالحاً.",ephemeral=True); return
        keys=[str(k) for k in node.keys() if k not in {"url","name","description","year","size","added_at"}]
        items=await bot.store.get_ui_children(path,keys)
        spec=path[0] if path else "الإدارة"
        embed=discord.Embed(title="معاينة واجهة المجلد",description=f"المسار الداخلي: `{' > '.join(path)}`",color=discord.Color.blurple())
        await interaction.response.send_message(embed=embed,view=BrowseView(bot,spec,path,items,0),ephemeral=True)

    @bot.tree.command(name="سجل_تعديلات_الواجهة",description="عرض آخر تغييرات أسماء وترتيب وأشكال المكتبة")
    async def ui_history_cmd(interaction:discord.Interaction)->None:
        if not await require_library_admin(interaction): return
        rows=await bot.store.ui_history(15)
        text="\
".join(f"`#{r['id']}` <@{r['user_id']}> — {r['action']} — `{r['node_id'] or '-'}`" for r in rows) or "لا يوجد سجل بعد."
        await interaction.response.send_message(text[:1900],ephemeral=True)

    @bot.tree.command(name="تراجع_عن_آخر_تعديل_واجهة",description="التراجع عن آخر تعديل واجهة")
    async def undo_ui_cmd(interaction:discord.Interaction)->None:
        if not await require_library_admin(interaction): return
        row=await bot.store.undo_last_ui(interaction.user.id)
        await interaction.response.send_message("تم التراجع عن آخر تعديل." if row else "لا يوجد تعديل يمكن التراجع عنه.",ephemeral=True)

    @bot.tree.command(name="تثبيت_عنصر_مكتبة",description="تثبيت عنصر في أعلى المجلد دون تغيير مساره الحقيقي")
    async def pin_library_node(interaction:discord.Interaction,المجلد:str,العنصر:str,مثبت:bool=True)->None:
        if not await require_library_admin(interaction): return
        parent=parse_internal_path(المجلد)
        await bot.store.set_ui_override(interaction.user.id,parent,العنصر,position=(-100000 if مثبت else None))
        await interaction.response.send_message("تم تثبيت العنصر." if مثبت else "تم إلغاء تثبيت العنصر.",ephemeral=True)

    @bot.tree.command(name="طلبات_الملفات_الناقصة",description="عرض أكثر الملفات التي طلبها الطلاب ولم يجدوها")
    async def missing_file_requests(interaction:discord.Interaction)->None:
        if not await require_library_admin(interaction): return
        rows=await bot.store.missing_requests_summary(25)
        text="\n".join(f"• **{r['query']}** — {r['specialization']} — {r['c']} طلب" for r in rows) or "لا توجد طلبات حتى الآن."
        await interaction.response.send_message(embed=discord.Embed(title="طلبات الملفات الناقصة",description=text[:4000],color=discord.Color.orange()),ephemeral=True)

    @bot.tree.command(name="فحص_واجهة_المكتبة",description="كشف تعارض المواقع والأسماء الطويلة والعناصر المخفية")
    async def diagnose_ui_cmd(interaction:discord.Interaction)->None:
        if not await require_library_admin(interaction): return
        d=await bot.store.ui_diagnostics()
        embed=discord.Embed(title="فحص واجهة المكتبة",color=discord.Color.orange())
        embed.add_field(name="تعديلات العرض",value=str(d['total']))
        embed.add_field(name="تعارض مواقع",value=str(len(d['duplicate_positions'])))
        embed.add_field(name="أسماء طويلة",value=str(len(d['long_names'])))
        embed.add_field(name="عناصر مخفية",value=str(len(d['hidden'])))
        await interaction.response.send_message(embed=embed,ephemeral=True)

    @bot.tree.error
    async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        logger.exception("خطأ أمر Slash: %s", error)
        await safe_interaction_error(interaction)
        await send_log(bot, f"خطأ أمر Slash: `{str(error)[:1500]}`")


    @bot.tree.command(name="حالة_نظام_المكتبة", description="عرض صحة وأداء المكتبة")
    async def library_health(interaction: discord.Interaction) -> None:
        if not await require_library_admin(interaction):
            return
        data = bot.health_state or await bot.store.health_snapshot()
        embed = discord.Embed(title="حالة نظام المكتبة", color=discord.Color.green(), timestamp=discord.utils.utcnow())
        embed.description = (
            f"**SQLite:** {data.get('db_check', 'غير معروف')}\n"
            f"**الملفات النشطة:** {data.get('active_files', 0):,}\n"
            f"**الجلسات المفتوحة:** {len(bot.active_sessions):,}\n"
            f"**زمن Discord:** {data.get('latency_ms', bot.latency*1000):.1f} ms\n"
            f"**متوسط البحث:** {data.get('search_avg_ms', 0):.2f} ms\n"
            f"**أبطأ بحث:** {data.get('search_max_ms', 0):.2f} ms\n"
            f"**إصابات Cache:** {data.get('search_cache_hits', 0):,}\n"
            f"**حجم قاعدة البيانات:** {data.get('db_size', 0)/1024/1024:.2f} MB\n"
            f"**RAM:** {data.get('rss_mb') if data.get('rss_mb') is not None else 'غير متاح'} MB"
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @bot.tree.command(name="اختبار_ضغط_المكتبة", description="اختبار داخلي لأداء البحث بدون إزعاج Discord")
    @app_commands.describe(العمليات="عدد العمليات من 1 إلى 5000", التزامن="عدد العمليات المتزامنة من 1 إلى 500")
    async def library_load_test(interaction: discord.Interaction, العمليات: app_commands.Range[int, 1, 5000] = 1000, التزامن: app_commands.Range[int, 1, 500] = 100) -> None:
        if not await require_library_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await bot.internal_load_test(int(العمليات), int(التزامن))
        text = (
            f"**العمليات:** {result['operations']:,}\n"
            f"**التزامن:** {result['concurrency']:,}\n"
            f"**المدة:** {result['total_seconds']:.2f} ثانية\n"
            f"**العمليات/ثانية:** {result['ops_per_second']:.1f}\n"
            f"**المتوسط:** {result['avg_ms']:.2f} ms\n"
            f"**P95:** {result['p95_ms']:.2f} ms\n"
            f"**الأبطأ:** {result['max_ms']:.2f} ms"
        )
        await interaction.edit_original_response(embed=discord.Embed(title="نتيجة اختبار ضغط المكتبة", description=text, color=discord.Color.blurple()))


    # إبقاء أمر إدارة واحد فقط للإدارة. الوظائف القديمة ما زالت داخل الكود
    # لكن لا تظهر كأوامر Slash، وتتم من اللوحة بالأزرار.
    _legacy_admin_commands = (
        "فتح_المكتبة",
        "تحديث_لوحة_المكتبة",
        "حذف_لوحة_المكتبة",
        "تعديل_مكان_زر",
        "تحديد_مكان_زر",
        "تعديل_اسم_العرض",
        "تعديل_شكل_زر",
        "إخفاء_عنصر_مكتبة",
        "إعادة_ضبط_شكل_عنصر",
        "ترتيب_تلقائي_للمجلد",
        "نسخ_تنسيق_مجلد",
        "حفظ_قالب_واجهة",
        "تطبيق_قالب_واجهة",
        "معاينة_ترتيب_مجلد",
        "سجل_تعديلات_الواجهة",
        "تراجع_عن_آخر_تعديل_واجهة",
        "تثبيت_عنصر_مكتبة",
        "طلبات_الملفات_الناقصة",
        "فحص_واجهة_المكتبة",
        "حالة_نظام_المكتبة",
        "اختبار_ضغط_المكتبة",
    )
    for _name in _legacy_admin_commands:
        try:
            bot.tree.remove_command(_name)
        except Exception:
            pass

    return bot


# =========================================================================
# يتم استدعاء build_library_bot() من main.py مثل الملف القديم.
# =========================================================================
