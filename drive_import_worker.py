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
            """
        )
        c.commit()


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
    value = value.strip().lower().replace("ـ", "")
    value = re.sub(r"[\s_\-]+", " ", value)
    return value


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
    parts = [normalize_text(p) for p in source_path.split("/") if p.strip()]
    if not result["branch"]:
        for branch in BRANCHES:
            if normalize_text(branch) in parts or normalize_text(branch) in normalized:
                result["branch"] = branch
                score += 0.30
                break
    if result["branch"] and not result["subject"]:
        for subject in MATERIALS.get(result["branch"], []):
            ns = normalize_text(subject)
            if ns in parts or ns in normalized:
                result["subject"] = subject
                score += 0.32
                break
    if not result["category"]:
        for category in CATEGORIES:
            nc = normalize_text(category)
            if nc in parts or nc in normalized:
                result["category"] = category
                score += 0.23
                break
    if not result["year"]:
        for p in parts:
            if YEAR_RE.fullmatch(p):
                result["year"] = p
                score += 0.15
                break
    for p in parts:
        if "جلسة أولى" in p or "الجلسة الأولى" in p:
            result["session"] = "جلسة أولى"
        elif "جلسة ثانية" in p or "الجلسة الثانية" in p:
            result["session"] = "جلسة ثانية"

    if defaults.get("branch"): score += 0.20
    if defaults.get("subject"): score += 0.25
    if defaults.get("category"): score += 0.20
    if defaults.get("year"): score += 0.10
    result["confidence"] = min(0.95, score)
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
    queue: list[tuple[str, str, str]] = []
    for ref in refs:
        account, service, meta = find_source_account(ref, job["source_account"] or "")
        queue.append((account, ref, str(meta.get("name") or ref)))

    total = 0
    total_bytes = 0
    while queue:
        account, current_id, current_path = queue.pop(0)
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
                    queue.append((account, child["id"], child_path))
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
                              job_id,source_account,source_file_id,source_parent_id,source_name,source_mime,
                              source_size,source_modified_time,source_path,branch,subject,category,year,session,
                              logical_path,storage_path,shared_group_id,destination_account,status,decision_source,
                              confidence,created_at,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (job_id, account, child["id"], current_id, child.get("name", "بدون اسم"),
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
                    """INSERT OR IGNORE INTO drive_import_items(job_id,source_account,source_file_id,source_name,
                    source_mime,source_size,source_modified_time,source_path,branch,subject,category,year,session,
                    logical_path,storage_path,shared_group_id,destination_account,status,decision_source,confidence,
                    created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (job_id, account, meta["id"], meta.get("name","بدون اسم"), meta.get("mimeType",""), size,
                     meta.get("modifiedTime"), current_path, inference.get("branch"), inference.get("subject"),
                     inference.get("category"), inference.get("year"), inference.get("session"), logical_path,
                     storage_path, shared_group_id, destination_account,
                     "pending" if logical_path else "needs_mapping", inference["decision_source"],
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


def process_item(item_id: int, actor_id: int) -> str:
    item = get_item(item_id)
    if not item or item["status"] not in ("pending", "error"):
        return "skipped"
    update_item(item_id, status="processing", attempts=int(item["attempts"] or 0) + 1, error=None)
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


def progress_text(job_id: int) -> str:
    job = get_job(job_id)
    if not job:
        return "العملية غير موجودة."
    c = job_counts(job_id)
    total = max(1, int(job["total_files"] or 0))
    done = c["imported"] + c["duplicate"] + c["skipped"] + c["error"]
    pct = done / total * 100
    elapsed = 0.0
    if job["started_at"]:
        try:
            elapsed = max(0.1, (datetime.now(timezone.utc) - datetime.fromisoformat(job["started_at"])).total_seconds())
        except Exception:
            elapsed = 0.1
    speed = int(c["bytes_done"] / elapsed) if elapsed else 0
    remain = int((int(job["bytes_total"] or 0) - c["bytes_done"]) / speed) if speed > 0 else 0
    return (
        f"📥 **عملية استيراد #{job_id}**\n"
        f"الحالة: **{job['status']}**\n"
        f"التقدم: **{done}/{job['total_files']} — {pct:.1f}%**\n"
        f"✅ مستورد: **{c['imported']}** | ♻️ مكرر: **{c['duplicate']}**\n"
        f"⚠️ يحتاج مسار: **{c['needs_mapping']}** | ❌ أخطاء: **{c['error']}**\n"
        f"📄 الحالي: `{job['current_name'] or '—'}`\n"
        f"⚡ السرعة: **{core.fmt_size(speed)}/ث** | المتبقي التقريبي: **{remain} ثانية**"
    )


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
