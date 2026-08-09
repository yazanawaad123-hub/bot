# -*- coding: utf-8 -*-
"""
بوت استيراد Google Drive لمكتبة الفهرس الأكاديمي
==================================================

ملف واحد مستقل للبوت الجديد، ويعيد استخدام البنية الحقيقية لبوت رفع الملفات:
- نفس SQLite
- نفس Shared Groups
- نفس مدير حسابات Google Drive
- نفس Folder IDs
- نفس سجل الملفات والإحصائيات

طريقة الربط:
1) ضع هذا الملف بجانب main_tawjihi_drive_manager.py
2) ضع توكن البوت الجديد في متغير البيئة DRIVE_IMPORT_BOT_TOKEN
3) شغله من main.py باستدعاء run_import_bot() أو شغله منفردًا.

يمكن تغيير اسم ملف البوت الأساسي عبر:
TAWJIHI_CORE_MODULE=main_tawjihi_drive_manager
"""
from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import logging
import mimetypes
import os
import re
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import discord
from discord import app_commands
from discord.ext import commands, tasks
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# -----------------------------------------------------------------------------
# الإعدادات
# -----------------------------------------------------------------------------
CORE_MODULE_NAME = os.getenv("TAWJIHI_CORE_MODULE", "main_tawjihi_drive_manager").strip()
IMPORT_BOT_TOKEN = os.getenv("DRIVE_IMPORT_BOT_TOKEN", "").strip()
IMPORT_GUILD_ID = int(os.getenv("DRIVE_IMPORT_GUILD_ID", "0") or 0)
IMPORT_ADMIN_CHANNEL_ID = int(os.getenv("DRIVE_IMPORT_ADMIN_CHANNEL_ID", "0") or 0)
IMPORT_PROGRESS_EDIT_SECONDS = max(3, int(os.getenv("IMPORT_PROGRESS_EDIT_SECONDS", "8") or 8))
IMPORT_DOWNLOAD_CHUNK = 8 * 1024 * 1024
IMPORT_UPLOAD_CHUNK = 8 * 1024 * 1024
IMPORT_MAX_RETRIES = max(1, int(os.getenv("IMPORT_MAX_RETRIES", "4") or 4))
IMPORT_CONFIDENCE_THRESHOLD = float(os.getenv("IMPORT_CONFIDENCE_THRESHOLD", "0.82") or 0.82)

log = logging.getLogger("drive-import")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

try:
    core = importlib.import_module(CORE_MODULE_NAME)
except Exception as exc:
    raise RuntimeError(
        f"تعذر استيراد ملف البوت الأساسي ({CORE_MODULE_NAME}). "
        "ضع drive_import_bot.py بجانبه أو عدّل TAWJIHI_CORE_MODULE."
    ) from exc

DB_PATH: Path = Path(core.DB_PATH)
BRANCHES: list[str] = list(core.BRANCHES)
MATERIALS: dict[str, list[str]] = dict(core.MATERIALS)
CATEGORIES = ["امتحانات تجريبي", "وزاري", "ملخصات", "كتاب المادة", "تأسيس", "دوسيات", "مواد"]
YEAR_RE = re.compile(r"^(20(?:1[5-9]|2[0-9]))$")
DRIVE_ID_RE = re.compile(r"[-\w]{20,}")
FOLDER_MIME = "application/vnd.google-apps.folder"
GOOGLE_MIME_PREFIX = "application/vnd.google-apps."

if not IMPORT_GUILD_ID:
    IMPORT_GUILD_ID = int(getattr(core, "GUILD_ID", 0) or 0)

# -----------------------------------------------------------------------------
# قاعدة البيانات الإضافية داخل نفس SQLite
# -----------------------------------------------------------------------------
def db() -> sqlite3.Connection:
    return core.db()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_import_schema() -> None:
    core.init_db()
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS drive_import_jobs(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              actor_id INTEGER NOT NULL,
              guild_id INTEGER,
              channel_id INTEGER,
              progress_message_id INTEGER,
              source_account TEXT,
              source_refs TEXT NOT NULL,
              default_branch TEXT,
              default_subject TEXT,
              default_category TEXT,
              default_year TEXT,
              status TEXT NOT NULL DEFAULT 'queued',
              total_files INTEGER NOT NULL DEFAULT 0,
              scanned_files INTEGER NOT NULL DEFAULT 0,
              imported_files INTEGER NOT NULL DEFAULT 0,
              duplicate_files INTEGER NOT NULL DEFAULT 0,
              skipped_files INTEGER NOT NULL DEFAULT 0,
              error_files INTEGER NOT NULL DEFAULT 0,
              bytes_total INTEGER NOT NULL DEFAULT 0,
              bytes_done INTEGER NOT NULL DEFAULT 0,
              current_name TEXT,
              last_error TEXT,
              started_at TEXT,
              finished_at TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_import_jobs_status
              ON drive_import_jobs(status,updated_at);

            CREATE TABLE IF NOT EXISTS drive_import_items(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id INTEGER NOT NULL,
              source_account TEXT NOT NULL,
              source_file_id TEXT NOT NULL,
              source_parent_id TEXT,
              source_name TEXT NOT NULL,
              source_mime TEXT,
              source_size INTEGER NOT NULL DEFAULT 0,
              source_modified_time TEXT,
              source_path TEXT,
              branch TEXT,
              subject TEXT,
              category TEXT,
              year TEXT,
              session TEXT,
              logical_path TEXT,
              storage_path TEXT,
              shared_group_id TEXT,
              destination_account TEXT,
              destination_file_id TEXT,
              destination_url TEXT,
              sha256 TEXT,
              status TEXT NOT NULL DEFAULT 'pending',
              attempts INTEGER NOT NULL DEFAULT 0,
              decision_source TEXT,
              confidence REAL NOT NULL DEFAULT 0,
              error TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(job_id,source_account,source_file_id)
            );
            CREATE INDEX IF NOT EXISTS idx_import_items_job_status
              ON drive_import_items(job_id,status,id);

            CREATE TABLE IF NOT EXISTS drive_import_rules(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pattern TEXT NOT NULL,
              pattern_type TEXT NOT NULL DEFAULT 'folder_path',
              branch TEXT NOT NULL,
              subject TEXT NOT NULL,
              category TEXT NOT NULL,
              year TEXT,
              session TEXT,
              priority INTEGER NOT NULL DEFAULT 100,
              uses INTEGER NOT NULL DEFAULT 0,
              created_by INTEGER,
              is_active INTEGER NOT NULL DEFAULT 1,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(pattern,pattern_type)
            );

            CREATE TABLE IF NOT EXISTS drive_import_audit(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id INTEGER,
              item_id INTEGER,
              actor_id INTEGER,
              action TEXT NOT NULL,
              details TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS drive_import_link_scans(
              job_id INTEGER NOT NULL,
              source_ref TEXT NOT NULL,
              total_files INTEGER NOT NULL DEFAULT 0,
              new_files INTEGER NOT NULL DEFAULT 0,
              duplicate_files INTEGER NOT NULL DEFAULT 0,
              unresolved_files INTEGER NOT NULL DEFAULT 0,
              error_files INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'scanned',
              checked_at TEXT NOT NULL,
              PRIMARY KEY(job_id,source_ref)
            );
            """
        )
        cols = {r["name"] for r in c.execute("PRAGMA table_info(drive_import_items)").fetchall()}
        if "source_root_ref" not in cols:
            c.execute("ALTER TABLE drive_import_items ADD COLUMN source_root_ref TEXT")
        if "review_channel_id" not in cols:
            c.execute("ALTER TABLE drive_import_items ADD COLUMN review_channel_id INTEGER")
        if "review_message_id" not in cols:
            c.execute("ALTER TABLE drive_import_items ADD COLUMN review_message_id INTEGER")
        if "review_state" not in cols:
            c.execute("ALTER TABLE drive_import_items ADD COLUMN review_state TEXT")
        c.commit()
    try:
        _restore_core_review_views()
    except Exception:
        pass


def audit(action: str, *, job_id: int | None = None, item_id: int | None = None,
          actor_id: int | None = None, details: Any = None) -> None:
    payload = details if isinstance(details, str) else json.dumps(details or {}, ensure_ascii=False)
    with db() as c:
        c.execute(
            "INSERT INTO drive_import_audit(job_id,item_id,actor_id,action,details,created_at) VALUES(?,?,?,?,?,?)",
            (job_id, item_id, actor_id, action, payload, now_iso()),
        )
        c.commit()


def update_job(job_id: int, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = now_iso()
    keys = list(fields)
    with db() as c:
        c.execute(
            f"UPDATE drive_import_jobs SET {','.join(f'{k}=?' for k in keys)} WHERE id=?",
            [fields[k] for k in keys] + [job_id],
        )
        c.commit()


def update_item(item_id: int, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = now_iso()
    keys = list(fields)
    with db() as c:
        c.execute(
            f"UPDATE drive_import_items SET {','.join(f'{k}=?' for k in keys)} WHERE id=?",
            [fields[k] for k in keys] + [item_id],
        )
        c.commit()


def get_job(job_id: int):
    with db() as c:
        return c.execute("SELECT * FROM drive_import_jobs WHERE id=?", (job_id,)).fetchone()


def get_item(item_id: int):
    with db() as c:
        return c.execute("SELECT * FROM drive_import_items WHERE id=?", (item_id,)).fetchone()

# -----------------------------------------------------------------------------
# الصلاحيات وروابط Drive
# -----------------------------------------------------------------------------
def is_admin(member: discord.Member | discord.User) -> bool:
    if int(member.id) in set(int(x) for x in getattr(core, "OWNER_IDS", []) if int(x)):
        return True
    return isinstance(member, discord.Member) and (
        member.guild_permissions.administrator or member.guild_permissions.manage_guild
    )


def extract_drive_id(value: str) -> str:
    value = value.strip()
    if DRIVE_ID_RE.fullmatch(value):
        return value
    parsed = urlparse(value)
    candidates = []
    if parsed.path:
        candidates += [p for p in parsed.path.split("/") if p]
    if parsed.query:
        for part in parsed.query.split("&"):
            if part.startswith("id="):
                candidates.append(part[3:])
    for part in reversed(candidates):
        if DRIVE_ID_RE.fullmatch(part):
            return part
    m = DRIVE_ID_RE.search(value)
    if not m:
        raise ValueError(f"لم أستطع استخراج Google Drive ID من: {value[:100]}")
    return m.group(0)


def split_refs(text: str) -> list[str]:
    refs = []
    for raw in re.split(r"[\n,;]+", text):
        raw = raw.strip()
        if raw:
            refs.append(extract_drive_id(raw))
    if not refs:
        raise ValueError("أرسل رابطًا أو ID واحدًا على الأقل.")
    return list(dict.fromkeys(refs))

# -----------------------------------------------------------------------------
# التعرف على المسار والتعلم
# -----------------------------------------------------------------------------

def normalize_text(value: str) -> str:
    value = str(value or "").strip().lower().replace("ـ", "")
    trans = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    value = value.translate(trans)
    value = value.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    value = value.replace("ة", "ه").replace("ى", "ي")
    value = re.sub(r"[^\w\s/]+", " ", value, flags=re.UNICODE)
    value = re.sub(r"[\s_\-]+", " ", value)
    return value.strip()


SUBJECT_ALIASES: dict[str, tuple[str, ...]] = {
    "التربية الإسلامية": (
        "التربيه الاسلاميه", "التربية الاسلامية", "دين", "الدين", "اسلاميه", "اسلامية",
    ),
    "اللغة العربية": (
        "اللغه العربيه", "اللغة العربية", "لغه عربيه", "لغة عربية", "عربي",
        "العربي", "عربي 1", "عربي1", "عربي اول", "عربي الاول", "عربي ١",
    ),
    "اللغة الإنجليزية": (
        "اللغه الانجليزيه", "اللغة الانجليزية", "انجليزي", "الانجليزي",
        "english", "eng",
    ),
    "الرياضيات": ("رياضيات", "الرياضيات", "math", "رياضيات علمي", "رياضيات ادبي"),
    "الفيزياء": ("فيزياء", "الفيزياء", "physics"),
    "الكيمياء": ("كيمياء", "الكيمياء", "chemistry"),
    "الأحياء": ("احياء", "الاحياء", "biology"),
    "التكنولوجيا": ("تكنولوجيا", "التكنولوجيا", "technology"),
    "التاريخ": ("تاريخ", "التاريخ"),
    "الجغرافيا": ("جغرافيا", "الجغرافيا"),
    "الثقافة العلمية": ("الثقافه العلميه", "الثقافة العلمية", "ثقافه علميه", "ثقافة علمية"),
    "المحاسبة": ("محاسبه", "المحاسبة"),
    "الإدارة والاقتصاد": ("الاداره والاقتصاد", "الإدارة والاقتصاد", "اداره واقتصاد", "إدارة واقتصاد"),
    "المشاريع الصغيرة": ("المشاريع الصغيره", "المشاريع الصغيرة", "مشاريع صغيره", "مشاريع صغيرة"),
    "العلوم الزراعية": ("العلوم الزراعيه", "العلوم الزراعية", "علوم زراعيه", "علوم زراعية"),
    "الإنتاج النباتي": ("الانتاج النباتي", "الإنتاج النباتي", "انتاج نباتي"),
    "الإنتاج الحيواني": ("الانتاج الحيواني", "الإنتاج الحيواني", "انتاج حيواني"),
    "العلوم الصناعية": ("العلوم الصناعيه", "العلوم الصناعية", "علوم صناعيه", "علوم صناعية"),
    "الرسم الصناعي": ("الرسم الصناعي", "رسم صناعي"),
    "التدريب المهني": ("التدريب المهني", "تدريب مهني"),
    "العلوم الفندقية": ("العلوم الفندقيه", "العلوم الفندقية", "علوم فندقيه", "علوم فندقية"),
    "إنتاج الطعام وخدمته": ("انتاج الطعام وخدمته", "إنتاج الطعام وخدمته", "انتاج الطعام", "خدمه الطعام"),
    "إدارة الفنادق": ("اداره الفنادق", "إدارة الفنادق", "ادارة فنادق"),
    "الاقتصاد المنزلي": ("الاقتصاد المنزلي", "اقتصاد منزلي"),
    "التغذية": ("التغذيه", "التغذية", "تغذيه", "تغذية"),
    "الملابس والنسيج": ("الملابس والنسيج", "ملابس ونسيج"),
    "الفقه": ("فقه", "الفقه"),
    "التفسير وعلوم القرآن": ("التفسير وعلوم القران", "تفسير وعلوم القران", "تفسير", "علوم القران"),
    "الحديث وعلومه": ("الحديث وعلومه", "حديث وعلومه", "حديث"),
}

CATEGORY_ALIASES: dict[str, tuple[str, ...]] = {
    "امتحانات تجريبي": ("امتحان تجريبي", "امتحانات تجريبي", "تجريبي", "تجريبيه", "تجريبية"),
    "وزاري": ("وزاري", "وزاره", "وزارة", "امتحان وزاري", "امتحانات وزاري"),
    "ملخصات": ("ملخص", "ملخصات", "تلخيص", "مراجعه", "مراجعة"),
    "كتاب المادة": ("كتاب الماده", "كتاب المادة", "الكتاب", "كتاب"),
    "تأسيس": ("تاسيس", "تأسيس", "اساسيات", "أساسيات"),
    "دوسيات": ("دوسيه", "دوسية", "دوسيات", "دوسيهات"),
    "مواد": ("مواد", "اوراق عمل", "أوراق عمل", "مرفقات"),
}

BRANCH_ALIASES: dict[str, tuple[str, ...]] = {
    "علمي": ("علمي", "الفرع العلمي"),
    "أدبي": ("ادبي", "أدبي", "الفرع الادبي", "الفرع الأدبي"),
    "تجاري": ("تجاري", "الفرع التجاري"),
    "زراعي": ("زراعي", "الفرع الزراعي"),
    "صناعي": ("صناعي", "الفرع الصناعي"),
    "فندقي": ("فندقي", "الفرع الفندقي"),
    "اقتصاد منزلي": ("اقتصاد منزلي", "الاقتصاد المنزلي"),
    "شرعي": ("شرعي", "الفرع الشرعي"),
}

# إشارات قوية لمادة عربية تخص الأدبي، حسب تسمية الملفات الشائعة.
UNKNOWN_BRANCH_REVIEW_CHANNEL_ID = 1532071332240556203
AUTO_IMPORT_CONFIDENCE = 0.84

ARABIC_LITERARY_HINTS = (
    "عربي 2", "عربي2", "عربي ثاني", "العربي 2", "الادب", "الأدب",
    "ادب ونصوص", "الأدب والنصوص", "النصوص الادبيه", "النصوص الأدبية",
)


def _contains_alias(normalized: str, aliases: tuple[str, ...]) -> bool:
    padded = f" {normalized} "
    for alias in aliases:
        na = normalize_text(alias)
        if not na:
            continue
        if f" {na} " in padded or na in normalized:
            return True
    return False


def _subject_branches(subject: str) -> list[str]:
    return [branch for branch in BRANCHES if subject in MATERIALS.get(branch, [])]


def _shared_group_choice(subject: str, candidate_branches: list[str]) -> tuple[str, str | None]:
    """اختيار فرع ممثل لمادة مشتركة بدون تكرار التخزين.
    إذا كانت الفروع مربوطة فعلياً بنفس shared group نرجع فرعاً واحداً فقط.
    """
    groups: dict[str, list[str]] = {}
    for branch in candidate_branches:
        try:
            row = core.shared_group_for(branch, subject)
        except Exception:
            row = None
        if row:
            gid = str(row["group_id"] if "group_id" in row.keys() else row["id"])
            groups.setdefault(gid, []).append(branch)
    if groups:
        gid, members = max(groups.items(), key=lambda kv: len(kv[1]))
        if members:
            return members[0], gid
    return "", None


def _infer_subject_and_branch(normalized: str, explicit_branch: str = "") -> tuple[str, str, float, str]:
    # حالة خاصة: عربي 2/الأدب تعتبر أدبي إذا الاسم نفسه يحمل هذه الإشارة.
    if any(normalize_text(x) in normalized for x in ARABIC_LITERARY_HINTS):
        if "اللغة العربية" in MATERIALS.get("أدبي", []):
            return "أدبي", "اللغة العربية", 0.91, "subject_hint:arabic_literary"

    matched_subjects: list[tuple[str, int]] = []
    all_subjects = sorted({s for vals in MATERIALS.values() for s in vals}, key=len, reverse=True)
    for subject in all_subjects:
        aliases = SUBJECT_ALIASES.get(subject, (subject,))
        best = 0
        for alias in aliases:
            na = normalize_text(alias)
            if na and na in normalized:
                best = max(best, len(na))
        if best:
            matched_subjects.append((subject, best))

    if not matched_subjects:
        return explicit_branch, "", 0.0, "subject_unknown"

    matched_subjects.sort(key=lambda x: x[1], reverse=True)
    subject = matched_subjects[0][0]
    branches = _subject_branches(subject)

    if explicit_branch and explicit_branch in branches:
        return explicit_branch, subject, 0.86, "subject+explicit_branch"

    if len(branches) == 1:
        return branches[0], subject, 0.92, "unique_subject"

    # مادة موجودة في أكثر من فرع: لا نخمن فرعاً عشوائياً.
    # إذا كانت مربوطة كمادة مشتركة نستخدم فرعاً ممثلاً واحداً وسيولد النظام aliases لكل الفروع.
    shared_branch, gid = _shared_group_choice(subject, branches)
    if shared_branch:
        return shared_branch, subject, 0.90, f"shared_group:{gid}"

    # لو اسم الملف نفسه يحتوي فرعاً واضحاً، نستخدمه.
    for branch in branches:
        if _contains_alias(normalized, BRANCH_ALIASES.get(branch, (branch,))):
            return branch, subject, 0.88, "subject+branch_name"

    # مادة متعددة الفروع لكن لم يتم تعريفها كمشتركة: نتركها للمراجعة بدل التكرار/التخمين.
    return "", subject, 0.55, "ambiguous_shared_subject"


def infer_path(source_path: str, defaults: dict[str, str]) -> dict[str, Any]:
    normalized = normalize_text(source_path)
    result = {
        "branch": defaults.get("branch", ""),
        "subject": defaults.get("subject", ""),
        "category": defaults.get("category", ""),
        "year": defaults.get("year", ""),
        "session": "",
        "confidence": 0.0,
        "decision_source": "defaults" if any(defaults.values()) else "unknown",
    }

    # القواعد التي تعلمها المستخدم لها الأولوية دائماً.
    with db() as c:
        rules = c.execute(
            "SELECT * FROM drive_import_rules WHERE is_active=1 ORDER BY priority DESC,LENGTH(pattern) DESC"
        ).fetchall()
    for rule in rules:
        pattern = normalize_text(str(rule["pattern"]))
        matched = pattern in normalized if rule["pattern_type"] != "regex" else bool(re.search(pattern, normalized))
        if matched:
            result.update(
                branch=rule["branch"], subject=rule["subject"], category=rule["category"],
                year=rule["year"] or "", session=rule["session"] or "",
                confidence=0.99, decision_source=f"rule:{rule['id']}"
            )
            with db() as c:
                c.execute("UPDATE drive_import_rules SET uses=uses+1,updated_at=? WHERE id=?", (now_iso(), rule["id"]))
                c.commit()
            return result

    score = 0.0

    # 1) الفرع الصريح من الاسم/المجلد.
    explicit_branch = result["branch"]
    if not explicit_branch:
        for branch in BRANCHES:
            if _contains_alias(normalized, BRANCH_ALIASES.get(branch, (branch,))):
                explicit_branch = branch
                result["branch"] = branch
                score += 0.30
                result["decision_source"] = "branch_name"
                break

    # 2) المادة نفسها قد تحدد الفرع تلقائياً.
    if not result["subject"]:
        b, subject, subject_score, source = _infer_subject_and_branch(normalized, explicit_branch)
        if subject:
            result["subject"] = subject
            if b:
                result["branch"] = b
            score += subject_score * 0.45
            result["decision_source"] = source
    elif result["branch"]:
        score += 0.35

    # 3) التصنيف.
    if not result["category"]:
        for category in CATEGORIES:
            if _contains_alias(normalized, CATEGORY_ALIASES.get(category, (category,))):
                result["category"] = category
                score += 0.22
                break

    # 4) السنة من أي مكان في الاسم/المسار.
    if not result["year"]:
        years = re.findall(r"\b20(?:1[5-9]|2[0-9])\b", normalized)
        if years:
            result["year"] = years[-1]
            score += 0.12

    # 5) الجلسة.
    if any(x in normalized for x in ("جلسه اولي", "الجلسه الاولي", "جلسه 1", "دوره اولي")):
        result["session"] = "جلسة أولى"
        score += 0.05
    elif any(x in normalized for x in ("جلسه ثانيه", "الجلسه الثانيه", "جلسه 2", "دوره ثانيه")):
        result["session"] = "جلسة ثانية"
        score += 0.05

    if defaults.get("branch"):
        score += 0.20
    if defaults.get("subject"):
        score += 0.25
    if defaults.get("category"):
        score += 0.20
    if defaults.get("year"):
        score += 0.10

    # لا نسمح بثقة عالية إذا المادة مشتركة ومبهمة ولا يوجد shared group.
    if result["subject"] and not result["branch"]:
        score = min(score, 0.60)

    # وجود الفرع+المادة+التصنيف يكفي عادة للتصنيف التلقائي.
    if result["branch"] and result["subject"] and result["category"]:
        score = max(score, 0.84)

    result["confidence"] = min(0.99, score)
    return result

def build_logical_path(branch: str, subject: str, category: str, year: str = "", session: str = "") -> str:
    core.validate_branch_subject(branch, subject)
    if category not in CATEGORIES:
        raise ValueError("التصنيف غير معروف.")
    parts = [branch, subject, category]
    if session:
        parts.append(session)
    if year and category in ("وزاري", "امتحانات تجريبي"):
        parts.append(year)
    return core.normalize_relative_folder_path("/".join(parts))


def save_rule(pattern: str, branch: str, subject: str, category: str, year: str,
              session: str, actor_id: int) -> None:
    core.validate_branch_subject(branch, subject)
    if category not in CATEGORIES:
        raise ValueError("التصنيف غير صحيح.")
    with db() as c:
        c.execute(
            """INSERT INTO drive_import_rules(pattern,pattern_type,branch,subject,category,year,session,
               priority,uses,created_by,is_active,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,100,0,?,1,?,?)
               ON CONFLICT(pattern,pattern_type) DO UPDATE SET
               branch=excluded.branch,subject=excluded.subject,category=excluded.category,
               year=excluded.year,session=excluded.session,created_by=excluded.created_by,
               is_active=1,updated_at=excluded.updated_at""",
            (pattern.strip(), "folder_path", branch, subject, category, year or None,
             session or None, actor_id, now_iso(), now_iso()),
        )
        c.commit()

# -----------------------------------------------------------------------------
# الوصول إلى الملفات المصدر
# -----------------------------------------------------------------------------
def configured_accounts() -> list[str]:
    return [name for name in core.DRIVE_ACCOUNTS if core.account_is_configured(name) and core.drive_account_enabled(name)]


def find_source_account(file_id: str, preferred: str = "") -> tuple[str, Any, dict[str, Any]]:
    names = ([preferred] if preferred else []) + configured_accounts()
    seen = set()
    last_error: Exception | None = None
    for name in names:
        if not name or name in seen or name not in core.DRIVE_ACCOUNTS:
            continue
        seen.add(name)
        try:
            service = core.service(name)
            meta = service.files().get(
                fileId=file_id,
                fields="id,name,mimeType,size,modifiedTime,parents,md5Checksum,webViewLink,driveId",
                supportsAllDrives=True,
            ).execute()
            return name, service, meta
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"لا يوجد حساب Drive لديه صلاحية للوصول إلى العنصر {file_id}: {last_error}")


def list_children(service: Any, folder_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    token = None
    while True:
        response = service.files().list(
            q=f"'{core.qesc(folder_id)}' in parents and trashed=false",
            fields="nextPageToken,files(id,name,mimeType,size,modifiedTime,parents,md5Checksum,webViewLink,driveId)",
            pageToken=token,
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            corpora="allDrives",
        ).execute()
        items.extend(response.get("files", []))
        token = response.get("nextPageToken")
        if not token:
            return items


def scan_source(job_id: int) -> None:
    job = get_job(job_id)
    if not job:
        return
    refs = json.loads(job["source_refs"])
    defaults = {
        "branch": job["default_branch"] or "",
        "subject": job["default_subject"] or "",
        "category": job["default_category"] or "",
        "year": job["default_year"] or "",
    }
    update_job(job_id, status="scanning", started_at=job["started_at"] or now_iso())
    queue: list[tuple[str, str, str, str]] = []
    for ref in refs:
        account, service, meta = find_source_account(ref, job["source_account"] or "")
        queue.append((account, ref, str(meta.get("name") or ref), ref))

    total = 0
    total_bytes = 0
    while queue:
        account, current_id, current_path, root_ref = queue.pop(0)
        service = core.service(account)
        meta = service.files().get(
            fileId=current_id,
            fields="id,name,mimeType,size,modifiedTime,parents,md5Checksum,webViewLink,driveId",
            supportsAllDrives=True,
        ).execute()
        if meta.get("mimeType") == FOLDER_MIME:
            for child in list_children(service, current_id):
                child_path = f"{current_path}/{child.get('name','بدون اسم')}"
                if child.get("mimeType") == FOLDER_MIME:
                    queue.append((account, child["id"], child_path, root_ref))
                else:
                    inference = infer_path(child_path, defaults)
                    logical_path = ""
                    storage_path = ""
                    shared_group_id = None
                    destination_account = None
                    if inference["confidence"] >= IMPORT_CONFIDENCE_THRESHOLD and all(
                        inference.get(k) for k in ("branch", "subject", "category")
                    ):
                        logical_path = build_logical_path(
                            inference["branch"], inference["subject"], inference["category"],
                            inference.get("year", ""), inference.get("session", "")
                        )
                        target = core.resolve_storage_target(inference["branch"], inference["subject"], logical_path)
                        storage_path = str(target["storage_path"])
                        shared_group_id = target["shared_group_id"]
                        destination_account = target["drive_account"]
                    size = int(child.get("size") or 0)
                    with db() as c:
                        c.execute(
                            """INSERT OR IGNORE INTO drive_import_items(
                              job_id,source_account,source_file_id,source_root_ref,source_parent_id,source_name,source_mime,
                              source_size,source_modified_time,source_path,branch,subject,category,year,session,
                              logical_path,storage_path,shared_group_id,destination_account,status,decision_source,
                              confidence,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (job_id, account, child["id"], root_ref, current_id, child.get("name", "بدون اسم"),
                             child.get("mimeType", ""), size, child.get("modifiedTime"), child_path,
                             inference.get("branch"), inference.get("subject"), inference.get("category"),
                             inference.get("year"), inference.get("session"), logical_path, storage_path,
                             shared_group_id, destination_account,
                             "pending" if logical_path else "needs_mapping", inference["decision_source"],
                             float(inference["confidence"]), now_iso(), now_iso())
                        )
                        c.commit()
                    total += 1
                    total_bytes += size
                    update_job(job_id, total_files=total, bytes_total=total_bytes, current_name=child.get("name"))
        else:
            inference = infer_path(current_path, defaults)
            logical_path = ""
            storage_path = ""
            shared_group_id = None
            destination_account = None
            if inference["confidence"] >= IMPORT_CONFIDENCE_THRESHOLD and all(inference.get(k) for k in ("branch","subject","category")):
                logical_path = build_logical_path(inference["branch"], inference["subject"], inference["category"], inference.get("year", ""), inference.get("session", ""))
                target = core.resolve_storage_target(inference["branch"], inference["subject"], logical_path)
                storage_path = str(target["storage_path"])
                shared_group_id = target["shared_group_id"]
                destination_account = target["drive_account"]
            size = int(meta.get("size") or 0)
            with db() as c:
                c.execute(
                    """INSERT OR IGNORE INTO drive_import_items(job_id,source_account,source_file_id,source_root_ref,source_name,
                    source_mime,source_size,source_modified_time,source_path,branch,subject,category,year,session,
                    logical_path,storage_path,shared_group_id,destination_account,status,decision_source,confidence,
                    created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (job_id, account, meta["id"], root_ref, meta.get("name","بدون اسم"), meta.get("mimeType",""), size,
                     meta.get("modifiedTime"), current_path, inference.get("branch"), inference.get("subject"),
                     inference.get("category"), inference.get("year"), inference.get("session"), logical_path,
                     storage_path, shared_group_id, destination_account,
                     "pending" if (logical_path and float(inference["confidence"]) >= AUTO_IMPORT_CONFIDENCE) else "needs_mapping",
                     inference["decision_source"],
                     float(inference["confidence"]), now_iso(), now_iso())
                )
                c.commit()
            total += 1
            total_bytes += size

    with db() as c:
        unresolved = c.execute(
            "SELECT COUNT(*) n FROM drive_import_items WHERE job_id=? AND status='needs_mapping'", (job_id,)
        ).fetchone()["n"]
    update_job(job_id, status="waiting_mapping" if unresolved else "ready", total_files=total, bytes_total=total_bytes, current_name="")
    audit("scan_complete", job_id=job_id, actor_id=job["actor_id"], details={"total": total, "unresolved": unresolved})

# -----------------------------------------------------------------------------
# النسخ وكشف التكرار
# -----------------------------------------------------------------------------
def download_source(item: sqlite3.Row, destination: Path) -> str:
    source_service = core.service(item["source_account"])
    mime = str(item["source_mime"] or "")
    if mime.startswith(GOOGLE_MIME_PREFIX) and mime != FOLDER_MIME:
        exports = {
            "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
            "application/vnd.google-apps.spreadsheet": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
            "application/vnd.google-apps.presentation": ("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"),
        }
        if mime not in exports:
            raise RuntimeError(f"نوع Google Workspace غير مدعوم للتصدير: {mime}")
        export_mime, suffix = exports[mime]
        if destination.suffix.lower() != suffix:
            destination = destination.with_suffix(suffix)
        request = source_service.files().export_media(fileId=item["source_file_id"], mimeType=export_mime)
    else:
        request = source_service.files().get_media(fileId=item["source_file_id"], supportsAllDrives=True)
    with destination.open("wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=IMPORT_DOWNLOAD_CHUNK)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return str(destination)



def existing_source_id_duplicate(source_file_id: str):
    """كشف سريع لنفس Google Drive file ID قبل تنزيل الملف."""
    needle = f"%id={source_file_id}%"
    with db() as c:
        row = c.execute(
            """SELECT id,drive_url,full_path,filename FROM submissions
               WHERE status IN ('accepted','accepted_copy')
                 AND source_url LIKE ?
               ORDER BY id DESC LIMIT 1""",
            (needle,),
        ).fetchone()
        if row:
            return row
        return c.execute(
            """SELECT id,job_id,status,destination_url,storage_path,source_name
               FROM drive_import_items
               WHERE source_file_id=? AND status='imported'
               ORDER BY id DESC LIMIT 1""",
            (source_file_id,),
        ).fetchone()


def existing_duplicate(storage_path: str, digest: str, size: int, filename: str):
    with db() as c:
        row = c.execute(
            """SELECT a.*,s.drive_url,s.id submission_id FROM accepted_files a
               JOIN submissions s ON s.id=a.submission_id
               WHERE a.full_path=? AND (a.sha256=? OR (a.filename=? AND s.size_bytes=?))
               ORDER BY a.id DESC LIMIT 1""",
            (storage_path, digest, filename, size),
        ).fetchone()
        if row:
            return row
        return c.execute(
            """SELECT v.*,r.full_path,r.original_filename FROM file_versions v
               JOIN file_registry r ON r.file_uuid=v.file_uuid
               WHERE r.full_path=? AND (v.sha256=? OR (r.original_filename=? AND v.size_bytes=?))
               ORDER BY v.id DESC LIMIT 1""",
            (storage_path, digest, filename, size),
        ).fetchone()


def upload_to_destination(item: sqlite3.Row, local_path: Path, digest: str) -> tuple[str, str, str]:
    target = core.resolve_storage_target(item["branch"], item["subject"], item["logical_path"])
    account = str(target["drive_account"])
    storage_path = str(target["storage_path"])
    # إعادة اختيار الحساب إذا تغيرت المساحة منذ الفحص
    account = core.account_for_storage(
        f"group:{target['shared_group_id']}" if target["shared_group_id"] else f"subject:{item['branch']}:{item['subject']}",
        required_bytes=local_path.stat().st_size,
        preferred=account,
    )
    folder_id = core.ensure_path(account, storage_path)
    media = MediaFileUpload(
        str(local_path),
        mimetype=mimetypes.guess_type(local_path.name)[0] or "application/octet-stream",
        resumable=True,
        chunksize=IMPORT_UPLOAD_CHUNK,
    )
    body = {"name": local_path.name, "parents": [folder_id]}
    request = core.service(account).files().create(
        body=body, media_body=media, fields="id,webViewLink,name,size", supportsAllDrives=True
    )
    response = None
    while response is None:
        _, response = request.next_chunk()
    file_id = response["id"]
    url = response.get("webViewLink") or f"https://drive.google.com/file/d/{file_id}/view"
    return account, file_id, url


def register_imported_file(item: sqlite3.Row, actor_id: int, account: str,
                           file_id: str, url: str, digest: str, size: int,
                           final_filename: str) -> int:
    target = core.resolve_storage_target(item["branch"], item["subject"], item["logical_path"])
    with db() as c:
        cur = c.execute(
            """INSERT INTO submissions(kind,status,user_id,username,branch,subject,category,full_path,
               filename,size_bytes,sha256,drive_account,drive_file_id,drive_url,source_url,reviewer_id,
               created_at,reviewed_at,logical_path,storage_path,shared_group_id,subject_id)
               VALUES('drive_import','accepted',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (actor_id, f"Drive Import #{actor_id}", item["branch"], item["subject"], item["category"],
             item["logical_path"], final_filename, size, digest, account, file_id, url,
             f"https://drive.google.com/open?id={item['source_file_id']}", actor_id, now_iso(), now_iso(),
             item["logical_path"], target["storage_path"], target["shared_group_id"], target["subject_id"]),
        )
        submission_id = int(cur.lastrowid)
        c.execute(
            "INSERT INTO accepted_files(submission_id,drive_account,full_path,sha256,drive_file_id,filename,accepted_at) VALUES(?,?,?,?,?,?,?)",
            (submission_id, account, target["storage_path"], digest, file_id, final_filename, now_iso()),
        )
        for branch, subject, logical_path in core.logical_alias_paths(
            item["branch"], item["subject"], item["logical_path"], target["shared_group_id"]
        ):
            if target["shared_group_id"]:
                c.execute(
                    """INSERT OR IGNORE INTO shared_file_aliases(submission_id,group_id,branch,subject,
                    logical_path,storage_path,url,created_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (submission_id, target["shared_group_id"], branch, subject, logical_path,
                     target["storage_path"], url, now_iso()),
                )
        c.commit()

    display_name = Path(final_filename).stem
    core.register_file_version(
        submission_id=submission_id,
        branch=item["branch"], subject=item["subject"], category=item["category"],
        full_path=item["logical_path"], display_name=display_name,
        original_filename=final_filename, digest=digest, size_bytes=size,
        drive_account=account, drive_file_id=file_id, drive_url=url,
        uploader_id=actor_id, reviewer_id=actor_id, change_note="استيراد من Google Drive",
    )
    core.ensure_library_path_sync(item["logical_path"], item["branch"], item["subject"], item["category"])
    core.write_submission_to_materials_json_sync(item["logical_path"], display_name, url)
    with db() as c:
        c.execute(
            """INSERT OR REPLACE INTO library_items(submission_id,full_path,display_name,original_filename,
            url,drive_file_id,drive_account,source_kind,uploader_id,reviewer_id,created_at,updated_at,is_active)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (submission_id, item["logical_path"], display_name, final_filename, url, file_id, account,
             "drive_import", actor_id, actor_id, now_iso(), now_iso()),
        )
        c.commit()
    core.audit("drive_import", actor_id, item["logical_path"], {
        "source_file_id": item["source_file_id"], "destination_file_id": file_id,
        "drive_account": account, "sha256": digest,
    }, submission_id)
    return submission_id



def _candidate_branches_for_item(item: sqlite3.Row) -> list[str]:
    branch = str(item["branch"] or "").strip()
    if branch in BRANCHES:
        return [branch]
    subject = str(item["subject"] or "").strip()
    if subject:
        candidates = _subject_branches(subject)
        if candidates:
            return candidates
    normalized = normalize_text(str(item["source_path"] or item["source_name"] or ""))
    scored: list[tuple[int, str]] = []
    for b in BRANCHES:
        score = 0
        for alias in BRANCH_ALIASES.get(b, (b,)):
            na = normalize_text(alias)
            if na and na in normalized:
                score += max(1, len(na))
        if score:
            scored.append((score, b))
    scored.sort(reverse=True)
    return [b for _, b in scored[:4]]


def _review_channel_for_item(item: sqlite3.Row) -> int:
    branch = str(item["branch"] or "").strip()
    if branch in BRANCHES:
        return int(getattr(core, "REVIEW_CHANNELS", {}).get(branch, 0) or 0)
    return int(UNKNOWN_BRANCH_REVIEW_CHANNEL_ID)


def _review_summary_text(item: sqlite3.Row) -> str:
    branch = str(item["branch"] or "").strip()
    subject = str(item["subject"] or "").strip()
    category = str(item["category"] or "").strip()
    year = str(item["year"] or "").strip()
    confidence = float(item["confidence"] or 0.0)
    candidates = _candidate_branches_for_item(item)

    if branch:
        headline = f"⚠️ **ملف استرداد يحتاج مراجعة — {branch}**"
        suspicion = f"البوت يرجّح الفرع: **{branch}**"
    else:
        headline = "⚠️ **ملف استرداد — لم أستطع تحديد الفرع بثقة**"
        suspicion = (
            "الفروع المحتملة: " + "، ".join(f"**{x}**" for x in candidates)
            if candidates else
            "لم أجد فرعًا واضحًا من اسم الملف أو المجلد."
        )

    detected = []
    if subject:
        detected.append(f"المادة المحتملة: **{subject}**")
    if category:
        detected.append(f"التصنيف المحتمل: **{category}**")
    if year:
        detected.append(f"السنة المحتملة: **{year}**")

    return (
        f"{headline}\n\n"
        f"📄 الملف: `{str(item['source_name'] or '')[:180]}`\n"
        f"📂 المصدر: `{str(item['source_path'] or '')[:500]}`\n"
        f"{suspicion}\n"
        + ("\n".join(detected) + "\n" if detected else "")
        + f"الثقة: **{confidence:.0%}**\n\n"
        "اضغط الزر بالأسفل، ثم اختر الفرع والمادة والمسار بالضغط فقط."
    )


def _folder_children_for_review(branch: str, subject: str, relative_parts: list[str]) -> tuple[list[str], bool]:
    prefix = [branch, subject] + list(relative_parts)
    children: set[str] = set()
    selectable = False
    try:
        paths = core.upload_paths_for_subject(branch, subject)
    except Exception:
        paths = []
    for full_path in paths:
        parts = [x for x in str(full_path).split("/") if x]
        if parts[:len(prefix)] != prefix:
            continue
        if len(parts) == len(prefix):
            selectable = True
        elif len(parts) > len(prefix):
            children.add(parts[len(prefix)])
    return sorted(children), selectable


async def _finish_review_mapping(
    interaction: discord.Interaction,
    item_id: int,
    branch: str,
    subject: str,
    relative_parts: list[str],
) -> None:
    item = get_item(item_id)
    if not item:
        return await interaction.response.send_message("العنصر لم يعد موجودًا.", ephemeral=True)

    if not relative_parts:
        return await interaction.response.send_message("اختر مجلدًا نهائيًا أولًا.", ephemeral=True)

    logical = core.normalize_relative_folder_path(
        "/".join([branch, subject] + list(relative_parts))
    )
    category = relative_parts[0] if relative_parts else ""
    year = next((x for x in reversed(relative_parts) if YEAR_RE.fullmatch(str(x))), "")
    session = next((x for x in relative_parts if str(x).startswith("جلسة")), "")
    target = core.resolve_storage_target(branch, subject, logical)

    update_item(
        item_id,
        branch=branch,
        subject=subject,
        category=category,
        year=year or None,
        session=session or None,
        logical_path=logical,
        storage_path=target["storage_path"],
        shared_group_id=target["shared_group_id"],
        destination_account=target["drive_account"],
        status="pending",
        confidence=1.0,
        decision_source="discord_review",
        review_state="resolved",
    )

    # يتعلم من المجلد الأب الذي جاء منه الملف، حتى يقلّ عدد المراجعات لاحقًا.
    try:
        pattern = str(item["source_path"] or "").rsplit("/", 1)[0].strip()
        if pattern:
            save_rule(
                pattern,
                branch,
                subject,
                category,
                year,
                session,
                int(interaction.user.id),
            )
    except Exception:
        pass

    with db() as c:
        unresolved = c.execute(
            """SELECT COUNT(*) n FROM drive_import_items
               WHERE job_id=? AND status='needs_mapping'""",
            (item["job_id"],),
        ).fetchone()["n"]

    if interaction.response.is_done():
        await interaction.edit_original_response(
            content=(
                "✅ **تم اعتماد الملف للمسار:**\n"
                f"`{logical}`\n\n"
                "سيتم رفعه وتحديث المكتبة تلقائيًا."
            ),
            view=None,
        )
    else:
        await interaction.response.edit_message(
            content=(
                "✅ **تم اعتماد الملف للمسار:**\n"
                f"`{logical}`\n\n"
                "سيتم رفعه وتحديث المكتبة تلقائيًا."
            ),
            view=None,
        )

    if not unresolved:
        update_job(int(item["job_id"]), status="ready")
        asyncio.create_task(run_job(int(item["job_id"])))


class ImportReviewDecisionView(discord.ui.View):
    def __init__(self, item_id: int):
        super().__init__(timeout=None)
        self.item_id = int(item_id)

        choose = discord.ui.Button(
            label="اختيار الفرع والمسار",
            emoji="✅",
            style=discord.ButtonStyle.success,
            custom_id=f"import_review:choose:{self.item_id}",
        )
        choose.callback = self._choose
        self.add_item(choose)

        # زر تغيير الفرع متاح دائمًا حتى لو كان اقتراح البوت غير صحيح.
        wrong = discord.ui.Button(
            label="الفرع المقترح غير صحيح",
            emoji="↪️",
            style=discord.ButtonStyle.secondary,
            custom_id=f"import_review:wrong_branch:{self.item_id}",
        )
        wrong.callback = self._wrong_branch
        self.add_item(wrong)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not is_admin(interaction.user):
            await interaction.response.send_message("هذه المراجعة للإدارة فقط.", ephemeral=True)
            return False
        return True

    async def _choose(self, interaction: discord.Interaction) -> None:
        item = get_item(self.item_id)
        if not item or item["status"] != "needs_mapping":
            return await interaction.response.send_message("تمت معالجة هذا الملف سابقًا.", ephemeral=True)

        branch = str(item["branch"] or "").strip()
        if branch in BRANCHES:
            await interaction.response.send_message(
                f"✅ الفرع المقترح: **{branch}**\nاختر المادة:",
                view=ReviewSubjectSelectView(self.item_id, branch),
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "اختر الفرع الذي تريد إرسال الملف إليه:",
                view=ReviewBranchSelectView(self.item_id, _candidate_branches_for_item(item)),
                ephemeral=True,
            )

    async def _wrong_branch(self, interaction: discord.Interaction) -> None:
        item = get_item(self.item_id)
        if not item or item["status"] != "needs_mapping":
            return await interaction.response.send_message("تمت معالجة هذا الملف سابقًا.", ephemeral=True)
        await interaction.response.send_message(
            "اختر الفرع الصحيح:",
            view=ReviewBranchSelectView(self.item_id, _candidate_branches_for_item(item)),
            ephemeral=True,
        )


class ReviewBranchSelectView(discord.ui.View):
    def __init__(self, item_id: int, candidates: list[str] | None = None):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        candidates = [x for x in (candidates or []) if x in BRANCHES]
        ordered = candidates + [x for x in BRANCHES if x not in candidates]
        options = []
        for branch in ordered:
            desc = "يرجحه البوت" if branch in candidates else "فرع آخر"
            options.append(discord.SelectOption(label=branch, value=branch, description=desc, emoji="✅"))
        select = discord.ui.Select(
            placeholder="اضغط واختر الفرع الصحيح",
            options=options[:25],
            min_values=1,
            max_values=1,
        )
        select.callback = self._selected
        self.add_item(select)

    async def _selected(self, interaction: discord.Interaction) -> None:
        branch = self.children[0].values[0]
        await interaction.response.edit_message(
            content=f"✅ الفرع: **{branch}**\nاختر المادة:",
            view=ReviewSubjectSelectView(self.item_id, branch),
        )


class ReviewSubjectSelectView(discord.ui.View):
    def __init__(self, item_id: int, branch: str):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        self.branch = branch
        subjects = list(MATERIALS.get(branch, []))
        item = get_item(self.item_id)
        suspected = str(item["subject"] or "") if item else ""
        ordered = ([suspected] if suspected in subjects else []) + [s for s in subjects if s != suspected]
        options = [
            discord.SelectOption(
                label=str(subject)[:100],
                value=str(subject),
                description=("المادة التي يرجحها البوت" if subject == suspected else None),
                emoji=("✅" if subject == suspected else None),
            )
            for subject in ordered[:25]
        ]
        select = discord.ui.Select(
            placeholder="اضغط واختر المادة الصحيحة",
            options=options,
            min_values=1,
            max_values=1,
        )
        select.callback = self._selected
        self.add_item(select)

    async def _selected(self, interaction: discord.Interaction) -> None:
        subject = self.children[0].values[0]
        await interaction.response.edit_message(
            content=(
                f"✅ الفرع: **{self.branch}**\n"
                f"✅ المادة: **{subject}**\n\n"
                "الآن ادخل بالمجلدات حتى تصل للمكان المطلوب:"
            ),
            view=ReviewFolderSelectView(self.item_id, self.branch, subject, []),
        )


class ReviewFolderSelectView(discord.ui.View):
    def __init__(
        self,
        item_id: int,
        branch: str,
        subject: str,
        relative_parts: list[str],
    ):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        self.branch = branch
        self.subject = subject
        self.relative_parts = list(relative_parts)

        children, selectable = _folder_children_for_review(branch, subject, self.relative_parts)

        if children:
            options = [
                discord.SelectOption(label=str(name)[:100], value=str(name), emoji="📁")
                for name in children[:25]
            ]
            select = discord.ui.Select(
                placeholder="اختر المجلد الذي تريد الدخول إليه",
                options=options,
                min_values=1,
                max_values=1,
                row=0,
            )
            select.callback = self._open_child
            self.add_item(select)

        if selectable:
            choose = discord.ui.Button(
                label="اختيار هذا المجلد ورفع الملف",
                emoji="✅",
                style=discord.ButtonStyle.success,
                row=1,
            )
            choose.callback = self._choose_here
            self.add_item(choose)

        if self.relative_parts:
            back = discord.ui.Button(
                label="رجوع",
                emoji="↩️",
                style=discord.ButtonStyle.secondary,
                row=2,
            )
            back.callback = self._back
            self.add_item(back)

        change_subject = discord.ui.Button(
            label="تغيير المادة",
            emoji="📚",
            style=discord.ButtonStyle.secondary,
            row=2,
        )
        change_subject.callback = self._change_subject
        self.add_item(change_subject)

        change_branch = discord.ui.Button(
            label="تغيير الفرع",
            emoji="🧭",
            style=discord.ButtonStyle.secondary,
            row=2,
        )
        change_branch.callback = self._change_branch
        self.add_item(change_branch)

    def _path_text(self) -> str:
        return " → ".join([self.branch, self.subject] + self.relative_parts)

    async def _open_child(self, interaction: discord.Interaction) -> None:
        child = self.children[0].values[0]
        parts = self.relative_parts + [child]
        await interaction.response.edit_message(
            content=(
                "📂 **المسار الحالي:**\n"
                f"`{' → '.join([self.branch, self.subject] + parts)}`\n\n"
                "اختر المجلد التالي، أو اضغط اختيار هذا المجلد إذا ظهر الزر."
            ),
            view=ReviewFolderSelectView(self.item_id, self.branch, self.subject, parts),
        )

    async def _choose_here(self, interaction: discord.Interaction) -> None:
        await _finish_review_mapping(
            interaction,
            self.item_id,
            self.branch,
            self.subject,
            self.relative_parts,
        )

    async def _back(self, interaction: discord.Interaction) -> None:
        parts = self.relative_parts[:-1]
        await interaction.response.edit_message(
            content=(
                "📂 **المسار الحالي:**\n"
                f"`{' → '.join([self.branch, self.subject] + parts)}`"
            ),
            view=ReviewFolderSelectView(self.item_id, self.branch, self.subject, parts),
        )

    async def _change_subject(self, interaction: discord.Interaction) -> None:
        await interaction.response.edit_message(
            content=f"الفرع: **{self.branch}**\nاختر المادة:",
            view=ReviewSubjectSelectView(self.item_id, self.branch),
        )

    async def _change_branch(self, interaction: discord.Interaction) -> None:
        item = get_item(self.item_id)
        await interaction.response.edit_message(
            content="اختر الفرع الصحيح:",
            view=ReviewBranchSelectView(
                self.item_id,
                _candidate_branches_for_item(item) if item else [],
            ),
        )


async def _dispatch_one_review(item_id: int) -> None:
    item = get_item(item_id)
    if not item or item["status"] != "needs_mapping":
        return
    if int(item["review_message_id"] or 0):
        return

    channel_id = _review_channel_for_item(item)
    if not channel_id:
        return

    try:
        ch = await core.channel(channel_id)
        if not ch:
            return
        view = ImportReviewDecisionView(item_id)
        msg = await ch.send(_review_summary_text(item), view=view)
        update_item(
            item_id,
            review_channel_id=int(channel_id),
            review_message_id=int(msg.id),
            review_state="waiting",
        )
        try:
            core.bot.add_view(view, message_id=int(msg.id))
        except Exception:
            pass
    except Exception as exc:
        log.exception("تعذر إرسال ملف الاسترداد للمراجعة: %s", exc)


async def dispatch_unresolved_reviews(job_id: int) -> None:
    with db() as c:
        rows = c.execute(
            """SELECT id FROM drive_import_items
               WHERE job_id=? AND status='needs_mapping'
                 AND COALESCE(review_message_id,0)=0
               ORDER BY id""",
            (job_id,),
        ).fetchall()
    for row in rows:
        await _dispatch_one_review(int(row["id"]))


def _restore_core_review_views() -> None:
    try:
        if not hasattr(core, "bot"):
            return
        with db() as c:
            rows = c.execute(
                """SELECT id,review_message_id FROM drive_import_items
                   WHERE status='needs_mapping'
                     AND COALESCE(review_message_id,0)>0
                     AND COALESCE(review_state,'waiting')='waiting'
                   ORDER BY id DESC LIMIT 250"""
            ).fetchall()
        for row in rows:
            try:
                core.bot.add_view(
                    ImportReviewDecisionView(int(row["id"])),
                    message_id=int(row["review_message_id"]),
                )
            except Exception:
                pass
    except Exception:
        pass


def process_item(item_id: int, actor_id: int) -> str:
    item = get_item(item_id)
    if not item or item["status"] not in ("pending", "error"):
        return "skipped"
    update_item(item_id, status="processing", attempts=int(item["attempts"] or 0) + 1, error=None)

    same_source = existing_source_id_duplicate(str(item["source_file_id"]))
    if same_source:
        update_item(
            item_id,
            status="duplicate",
            error="نفس ملف Google Drive موجود مسبقًا",
        )
        audit(
            "source_id_duplicate",
            job_id=item["job_id"],
            item_id=item_id,
            actor_id=actor_id,
            details={"source_file_id": item["source_file_id"]},
        )
        return "duplicate"

    temp_dir = Path(tempfile.mkdtemp(prefix="drive_import_"))
    try:
        raw_name = core.clean(str(item["source_name"] or "file"))
        local = temp_dir / raw_name
        local = Path(download_source(item, local))
        digest = core.sha256(local)
        size = local.stat().st_size
        duplicate = existing_duplicate(item["storage_path"], digest, size, local.name)
        if duplicate:
            update_item(item_id, status="duplicate", sha256=digest, error="موجود مسبقًا في نفس المسار")
            return "duplicate"
        account, file_id, url = upload_to_destination(item, local, digest)
        submission_id = register_imported_file(item, actor_id, account, file_id, url, digest, size, local.name)
        update_item(
            item_id, status="imported", sha256=digest, destination_account=account,
            destination_file_id=file_id, destination_url=url, source_size=size,
            storage_path=core.resolve_storage_target(item["branch"], item["subject"], item["logical_path"])["storage_path"],
            error=None,
        )
        audit("file_imported", job_id=item["job_id"], item_id=item_id, actor_id=actor_id,
              details={"submission_id": submission_id, "account": account, "url": url})
        return "imported"
    except Exception as exc:
        update_item(item_id, status="error", error=str(exc)[:1800])
        audit("file_error", job_id=item["job_id"], item_id=item_id, actor_id=actor_id, details=str(exc))
        raise
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

# -----------------------------------------------------------------------------
# العامل والتقدم
# -----------------------------------------------------------------------------
RUNNING_JOBS: set[int] = set()


def job_counts(job_id: int) -> dict[str, int]:
    with db() as c:
        rows = c.execute(
            "SELECT status,COUNT(*) n,COALESCE(SUM(source_size),0) b FROM drive_import_items WHERE job_id=? GROUP BY status",
            (job_id,),
        ).fetchall()
    out = {"pending":0,"processing":0,"needs_mapping":0,"imported":0,"duplicate":0,"skipped":0,"error":0,"bytes_done":0}
    for row in rows:
        out[row["status"]] = int(row["n"])
        if row["status"] in ("imported","duplicate","skipped"):
            out["bytes_done"] += int(row["b"] or 0)
    return out



def update_link_scan_summary(job_id: int) -> None:
    """حساب حالة كل رابط مصدر بعد المعالجة."""
    with db() as c:
        refs_row = c.execute("SELECT source_refs FROM drive_import_jobs WHERE id=?", (job_id,)).fetchone()
        if not refs_row:
            return
        try:
            refs = json.loads(refs_row["source_refs"] or "[]")
        except Exception:
            refs = []
        for ref in refs:
            rows = c.execute(
                """SELECT status,COUNT(*) n FROM drive_import_items
                   WHERE job_id=? AND COALESCE(source_root_ref,'')=?
                   GROUP BY status""",
                (job_id, str(ref)),
            ).fetchall()
            counts = {str(r["status"]): int(r["n"]) for r in rows}
            total = sum(counts.values())
            new_files = counts.get("imported", 0)
            duplicates = counts.get("duplicate", 0)
            unresolved = counts.get("needs_mapping", 0)
            errors = counts.get("error", 0)
            if total and new_files == 0 and unresolved == 0 and errors == 0 and duplicates == total:
                status = "fully_duplicate"
            elif new_files > 0:
                status = "contains_new_files"
            elif unresolved:
                status = "needs_mapping"
            elif errors:
                status = "has_errors"
            else:
                status = "scanned"
            c.execute(
                """INSERT INTO drive_import_link_scans(
                   job_id,source_ref,total_files,new_files,duplicate_files,unresolved_files,error_files,status,checked_at)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(job_id,source_ref) DO UPDATE SET
                   total_files=excluded.total_files,new_files=excluded.new_files,
                   duplicate_files=excluded.duplicate_files,unresolved_files=excluded.unresolved_files,
                   error_files=excluded.error_files,status=excluded.status,checked_at=excluded.checked_at""",
                (job_id, str(ref), total, new_files, duplicates, unresolved, errors, status, now_iso()),
            )
        c.commit()


def job_link_summaries(job_id: int) -> list[dict[str, Any]]:
    with db() as c:
        rows = c.execute(
            "SELECT * FROM drive_import_link_scans WHERE job_id=? ORDER BY source_ref",
            (job_id,),
        ).fetchall()
    return [{k: row[k] for k in row.keys()} for row in rows]


def progress_text(job_id: int) -> str:
    job = get_job(job_id)
    if not job:
        return "العملية غير موجودة."
    c = job_counts(job_id)
    total_raw = int(job["total_files"] or 0)
    total = max(1, total_raw)
    done = c["imported"] + c["duplicate"] + c["skipped"] + c["error"] + c["needs_mapping"]
    pct = done / total * 100
    elapsed = 0.0
    if job["started_at"]:
        try:
            elapsed = max(0.1, (datetime.now(timezone.utc) - datetime.fromisoformat(job["started_at"])).total_seconds())
        except Exception:
            elapsed = 0.1
    speed = int(c["bytes_done"] / elapsed) if elapsed else 0
    remain = int((int(job["bytes_total"] or 0) - c["bytes_done"]) / speed) if speed > 0 else 0

    lines = [
        f"📥 **عملية استيراد #{job_id}**",
        f"الحالة: **{job['status']}**",
        f"التقدم: **{done}/{total_raw} — {pct:.1f}%**",
        f"✅ جديد ومستورد: **{c['imported']}** | ♻️ مكرر: **{c['duplicate']}**",
        f"⚠️ يحتاج تحديد: **{c['needs_mapping']}** | ❌ أخطاء: **{c['error']}**",
        f"📄 الحالي: `{job['current_name'] or '—'}`",
        f"⚡ السرعة: **{core.fmt_size(speed)}/ث** | المتبقي التقريبي: **{remain} ثانية**",
    ]

    if total_raw > 0 and c["imported"] == 0 and c["needs_mapping"] == 0 and c["error"] == 0 and c["duplicate"] == total_raw:
        lines += [
            "",
            "♻️ **هذا الرابط مكرر بالكامل.**",
            "لم يتم العثور على أي ملف جديد، ويمكنك حذفه من مخزون الروابط لديك.",
        ]
    elif c["imported"] > 0:
        lines += [
            "",
            f"🆕 تم العثور على **{c['imported']}** ملف جديد؛ تم تجاهل الملفات المكررة تلقائيًا.",
        ]

    summaries = job_link_summaries(job_id)
    fully = [x for x in summaries if x.get("status") == "fully_duplicate"]
    if len(summaries) > 1 and fully:
        lines += ["", f"🔗 روابط مكررة بالكامل داخل هذه العملية: **{len(fully)}** من **{len(summaries)}**."]

    return "\n".join(lines)[:1950]


async def refresh_progress_message(job_id: int) -> None:
    job = get_job(job_id)
    if not job or not job["channel_id"] or not job["progress_message_id"]:
        return
    channel = import_bot.get_channel(int(job["channel_id"]))
    if not channel:
        return
    try:
        message = await channel.fetch_message(int(job["progress_message_id"]))
        await message.edit(content=progress_text(job_id), view=ImportControlView(job_id))
    except Exception:
        log.exception("تعذر تحديث رسالة العملية %s", job_id)


async def run_job(job_id: int) -> None:
    if job_id in RUNNING_JOBS:
        return
    RUNNING_JOBS.add(job_id)
    try:
        job = get_job(job_id)
        if not job:
            return
        if job["status"] in ("queued", "scanning"):
            await asyncio.to_thread(scan_source, job_id)
            await refresh_progress_message(job_id)
        job = get_job(job_id)
        if job["status"] == "waiting_mapping":
            await dispatch_unresolved_reviews(job_id)
            await refresh_progress_message(job_id)
            return
        update_job(job_id, status="running", started_at=job["started_at"] or now_iso())
        last_edit = 0.0
        while True:
            job = get_job(job_id)
            if not job or job["status"] in ("paused", "cancelled"):
                break
            with db() as c:
                item = c.execute(
                    "SELECT * FROM drive_import_items WHERE job_id=? AND status IN ('pending','error') AND attempts<? ORDER BY id LIMIT 1",
                    (job_id, IMPORT_MAX_RETRIES),
                ).fetchone()
            if not item:
                break
            update_job(job_id, current_name=item["source_name"])
            try:
                result = await asyncio.to_thread(process_item, int(item["id"]), int(job["actor_id"]))
            except Exception as exc:
                log.exception("خطأ باستيراد العنصر %s", item["id"])
                update_job(job_id, last_error=str(exc)[:1800])
                result = "error"
            counts = job_counts(job_id)
            update_job(
                job_id,
                scanned_files=sum(counts.get(k, 0) for k in ("imported","duplicate","skipped","error")),
                imported_files=counts["imported"], duplicate_files=counts["duplicate"],
                skipped_files=counts["skipped"], error_files=counts["error"],
                bytes_done=counts["bytes_done"], current_name=item["source_name"],
            )
            if time.monotonic() - last_edit >= IMPORT_PROGRESS_EDIT_SECONDS:
                await refresh_progress_message(job_id)
                last_edit = time.monotonic()
        counts = job_counts(job_id)
        unresolved = counts["needs_mapping"]
        remaining = counts["pending"] + counts["processing"]
        status = "waiting_mapping" if unresolved else ("completed_with_errors" if counts["error"] else "completed")
        if remaining and not unresolved:
            status = "paused"
        update_job(
            job_id, status=status, finished_at=now_iso() if status.startswith("completed") else None,
            imported_files=counts["imported"], duplicate_files=counts["duplicate"],
            skipped_files=counts["skipped"], error_files=counts["error"],
            bytes_done=counts["bytes_done"], current_name="",
        )
        await asyncio.to_thread(update_link_scan_summary, job_id)
        await refresh_progress_message(job_id)
    finally:
        RUNNING_JOBS.discard(job_id)

# -----------------------------------------------------------------------------
# واجهة Discord
# -----------------------------------------------------------------------------
intents = discord.Intents.default()
intents.members = True
import_bot = commands.Bot(command_prefix="!import_", intents=intents)


class ImportControlView(discord.ui.View):
    def __init__(self, job_id: int):
        super().__init__(timeout=None)
        self.job_id = job_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not is_admin(interaction.user):
            await interaction.response.send_message("هذه اللوحة للإدارة فقط.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="إيقاف مؤقت", emoji="⏸️", style=discord.ButtonStyle.secondary)
    async def pause(self, interaction: discord.Interaction, _: discord.ui.Button):
        update_job(self.job_id, status="paused")
        await interaction.response.send_message("تم إيقاف العملية مؤقتًا.", ephemeral=True)
        await refresh_progress_message(self.job_id)

    @discord.ui.button(label="استكمال", emoji="▶️", style=discord.ButtonStyle.success)
    async def resume(self, interaction: discord.Interaction, _: discord.ui.Button):
        job = get_job(self.job_id)
        if not job:
            return await interaction.response.send_message("العملية غير موجودة.", ephemeral=True)
        if job["status"] == "waiting_mapping":
            return await interaction.response.send_message("عيّن مسارات العناصر غير المعروفة أولًا.", ephemeral=True)
        update_job(self.job_id, status="ready")
        await interaction.response.send_message("تم استكمال العملية.", ephemeral=True)
        asyncio.create_task(run_job(self.job_id))

    @discord.ui.button(label="إلغاء", emoji="🛑", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        update_job(self.job_id, status="cancelled", finished_at=now_iso())
        await interaction.response.send_message("تم إلغاء العملية.", ephemeral=True)
        await refresh_progress_message(self.job_id)

    @discord.ui.button(label="تحديث", emoji="🔄", style=discord.ButtonStyle.primary)
    async def refresh(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.edit_message(content=progress_text(self.job_id), view=self)


@import_bot.tree.command(name="استيراد_درايف", description="استيراد ملفات أو مجلدات Google Drive إلى المكتبة")
@app_commands.describe(
    الروابط="رابط أو عدة روابط، افصل بينها بسطر جديد",
    حساب_المصدر="اسم حساب Drive الذي يملك صلاحية المصدر، اختياري",
    الفرع="فرع افتراضي عند كون المصدر كله لنفس الفرع",
    المادة="مادة افتراضية",
    التصنيف="تصنيف افتراضي",
    السنة="سنة افتراضية للمجلد",
)
async def import_drive_cmd(
    interaction: discord.Interaction,
    الروابط: str,
    حساب_المصدر: str = "",
    الفرع: str = "",
    المادة: str = "",
    التصنيف: str = "",
    السنة: str = "",
):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        refs = split_refs(الروابط)
        if الفرع and الفرع not in BRANCHES:
            raise ValueError("الفرع غير صحيح.")
        if المادة and (not الفرع or المادة not in MATERIALS.get(الفرع, [])):
            raise ValueError("المادة لا تطابق الفرع.")
        if التصنيف and التصنيف not in CATEGORIES:
            raise ValueError("التصنيف غير صحيح.")
        if السنة and not YEAR_RE.fullmatch(السنة):
            raise ValueError("السنة غير صحيحة.")
        with db() as c:
            cur = c.execute(
                """INSERT INTO drive_import_jobs(actor_id,guild_id,channel_id,source_account,source_refs,
                default_branch,default_subject,default_category,default_year,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,'queued',?,?)""",
                (interaction.user.id, interaction.guild_id, interaction.channel_id, حساب_المصدر.strip(),
                 json.dumps(refs), الفرع or None, المادة or None, التصنيف or None, السنة or None,
                 now_iso(), now_iso()),
            )
            job_id = int(cur.lastrowid)
            c.commit()
        msg = await interaction.channel.send(progress_text(job_id), view=ImportControlView(job_id))
        update_job(job_id, progress_message_id=msg.id)
        audit("job_created", job_id=job_id, actor_id=interaction.user.id, details={"refs": refs})
        await interaction.followup.send(f"✅ بدأت عملية الاستيراد رقم `{job_id}`.", ephemeral=True)
        asyncio.create_task(run_job(job_id))
    except Exception as exc:
        await interaction.followup.send(f"❌ تعذر بدء الاستيراد:\n```{str(exc)[:1700]}```", ephemeral=True)


@import_bot.tree.command(name="عناصر_تحتاج_مسار", description="عرض العناصر التي لم يستطع البوت تحديد مسارها")
async def unresolved_cmd(interaction: discord.Interaction, رقم_العملية: int):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    with db() as c:
        rows = c.execute(
            "SELECT id,source_name,source_path,confidence FROM drive_import_items WHERE job_id=? AND status='needs_mapping' ORDER BY id LIMIT 20",
            (رقم_العملية,),
        ).fetchall()
    if not rows:
        return await interaction.response.send_message("لا توجد عناصر تحتاج مسارًا.", ephemeral=True)
    text = "\n\n".join(
        f"`#{r['id']}` **{r['source_name']}**\n`{str(r['source_path'])[-160:]}`\nالثقة: {float(r['confidence']):.0%}"
        for r in rows
    )
    await interaction.response.send_message(text[:1900], ephemeral=True)


@import_bot.tree.command(name="تعيين_مسار_استيراد", description="تعيين مسار عنصر غير معروف وحفظ قاعدة تعلم")
async def map_item_cmd(
    interaction: discord.Interaction,
    رقم_العنصر: int,
    الفرع: str,
    المادة: str,
    التصنيف: str,
    السنة: str = "",
    الجلسة: str = "",
    حفظ_قاعدة: bool = True,
):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        item = get_item(رقم_العنصر)
        if not item:
            raise ValueError("العنصر غير موجود.")
        logical = build_logical_path(الفرع, المادة, التصنيف, السنة, الجلسة)
        target = core.resolve_storage_target(الفرع, المادة, logical)
        update_item(
            رقم_العنصر, branch=الفرع, subject=المادة, category=التصنيف,
            year=السنة or None, session=الجلسة or None, logical_path=logical,
            storage_path=target["storage_path"], shared_group_id=target["shared_group_id"],
            destination_account=target["drive_account"], status="pending",
            confidence=1.0, decision_source="manual",
        )
        if حفظ_قاعدة:
            pattern = str(item["source_path"] or "").rsplit("/", 1)[0]
            save_rule(pattern, الفرع, المادة, التصنيف, السنة, الجلسة, interaction.user.id)
        with db() as c:
            unresolved = c.execute(
                "SELECT COUNT(*) n FROM drive_import_items WHERE job_id=? AND status='needs_mapping'",
                (item["job_id"],),
            ).fetchone()["n"]
        if not unresolved:
            update_job(item["job_id"], status="ready")
            asyncio.create_task(run_job(int(item["job_id"])))
        await interaction.followup.send(
            f"✅ تم تعيين المسار:\n`{logical}`\n"
            + ("وتم حفظ قاعدة التعلم." if حفظ_قاعدة else ""),
            ephemeral=True,
        )
    except Exception as exc:
        await interaction.followup.send(f"❌ {str(exc)[:1700]}", ephemeral=True)


@import_bot.tree.command(name="حالة_الاستيراد", description="عرض حالة عملية استيراد")
async def import_status_cmd(interaction: discord.Interaction, رقم_العملية: int):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    await interaction.response.send_message(progress_text(رقم_العملية), ephemeral=True)


@import_bot.tree.command(name="إعادة_محاولة_أخطاء_الاستيراد", description="إعادة محاولة الملفات التي فشلت")
async def retry_errors_cmd(interaction: discord.Interaction, رقم_العملية: int):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    with db() as c:
        c.execute(
            "UPDATE drive_import_items SET status='pending',attempts=0,error=NULL,updated_at=? WHERE job_id=? AND status='error'",
            (now_iso(), رقم_العملية),
        )
        c.commit()
    update_job(رقم_العملية, status="ready", last_error=None)
    await interaction.response.send_message("✅ تم تجهيز الأخطاء لإعادة المحاولة.", ephemeral=True)
    asyncio.create_task(run_job(رقم_العملية))


@import_bot.tree.command(name="قواعد_تعلم_الاستيراد", description="عرض قواعد التعلم المحفوظة")
async def list_rules_cmd(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    with db() as c:
        rows = c.execute("SELECT * FROM drive_import_rules WHERE is_active=1 ORDER BY uses DESC,id DESC LIMIT 25").fetchall()
    if not rows:
        return await interaction.response.send_message("لا توجد قواعد محفوظة.", ephemeral=True)
    text = "\n\n".join(
        f"`#{r['id']}` `{r['pattern']}`\n→ {r['branch']} / {r['subject']} / {r['category']}"
        f"{(' / ' + r['year']) if r['year'] else ''} — استخدامات: {r['uses']}"
        for r in rows
    )
    await interaction.response.send_message(text[:1900], ephemeral=True)


@tasks.loop(minutes=2)
async def resume_jobs_loop():
    with db() as c:
        rows = c.execute(
            "SELECT id FROM drive_import_jobs WHERE status IN ('queued','ready','running','scanning') ORDER BY id LIMIT 5"
        ).fetchall()
    for row in rows:
        if int(row["id"]) not in RUNNING_JOBS:
            asyncio.create_task(run_job(int(row["id"])))


@import_bot.event
async def on_ready():
    init_import_schema()
    core.load_drive_account_runtime_state()
    with db() as c:
        rows = c.execute(
            "SELECT id FROM drive_import_jobs WHERE status NOT IN ('completed','completed_with_errors','cancelled')"
        ).fetchall()
    for row in rows:
        import_bot.add_view(ImportControlView(int(row["id"])))
    try:
        if IMPORT_GUILD_ID:
            guild = discord.Object(id=IMPORT_GUILD_ID)
            import_bot.tree.copy_global_to(guild=guild)
            await import_bot.tree.sync(guild=guild)
        else:
            await import_bot.tree.sync()
    except Exception:
        log.exception("تعذر مزامنة أوامر بوت الاستيراد")
    if not resume_jobs_loop.is_running():
        resume_jobs_loop.start()
    log.info("بوت الاستيراد جاهز: %s", import_bot.user)


async def run_import_bot() -> None:
    if not IMPORT_BOT_TOKEN:
        raise RuntimeError("ضع DRIVE_IMPORT_BOT_TOKEN في Environment Variables.")
    init_import_schema()
    async with import_bot:
        await import_bot.start(IMPORT_BOT_TOKEN)


def main() -> None:
    asyncio.run(run_import_bot())


if __name__ == "__main__":
    main()
