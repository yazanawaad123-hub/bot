# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import discord
from discord.ext import commands

UPLOAD_REMOTE_URL = os.getenv("UPLOAD_REMOTE_URL", "").rstrip("/")
SYNC_TOKEN = os.getenv("LIBRARY_SYNC_TOKEN", "").strip()
TOKEN = os.getenv("DRIVE_IMPORT_BOT_TOKEN", "").strip()
GUILD_ID = int(os.getenv("DRIVE_IMPORT_GUILD_ID", "0") or 0)
ADMIN_IDS = {
    int(x)
    for x in os.getenv("DRIVE_IMPORT_ADMIN_IDS", "").replace(" ", "").split(",")
    if x.isdigit()
}

BRANCHES = ["علمي", "أدبي", "تجاري", "زراعي", "صناعي", "فندقي", "اقتصاد منزلي", "شرعي"]
CATEGORIES = ["امتحانات تجريبي", "وزاري", "ملخصات", "كتاب المادة", "تأسيس", "دوسيات", "مواد"]
YEARS = [str(y) for y in range(2026, 2014, -1)]
RECENT_JOBS_FILE = "drive_import_recent_jobs.json"
MATERIALS_FILE = "materials.json"

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!import_", intents=intents)


def is_admin(user):
    if int(user.id) in ADMIN_IDS:
        return True
    return isinstance(user, discord.Member) and user.guild and user.id == user.guild.owner_id


def _request(method, path, payload=None):
    if not UPLOAD_REMOTE_URL or not SYNC_TOKEN:
        raise RuntimeError("إعدادات الربط البعيد ناقصة")
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        UPLOAD_REMOTE_URL + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {SYNC_TOKEN}",
            "Content-Type": "application/json",
            "User-Agent": "Tawjihi-Drive-Recovery/2.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            message = json.loads(body).get("error", body)
        except Exception:
            message = body
        raise RuntimeError(str(message)[:1700])


async def api(method, path, payload=None):
    return await asyncio.to_thread(_request, method, path, payload)


def _load_recent_jobs():
    try:
        data = json.loads(Path(RECENT_JOBS_FILE).read_text(encoding="utf-8"))
        return [int(x) for x in data if str(x).isdigit()][:25]
    except Exception:
        return []


def _save_recent_job(job_id: int):
    jobs = [int(job_id)] + [x for x in _load_recent_jobs() if int(x) != int(job_id)]
    try:
        Path(RECENT_JOBS_FILE).write_text(json.dumps(jobs[:25]), encoding="utf-8")
    except Exception:
        pass


def _subjects_for_branch(branch: str) -> list[str]:
    try:
        data = json.loads(Path(MATERIALS_FILE).read_text(encoding="utf-8"))
    except Exception:
        return []

    # Old tree format: {"علمي":{"الفيزياء":...}}
    if isinstance(data, dict) and isinstance(data.get(branch), dict):
        return [str(x) for x in data[branch].keys()]

    # New materials format may contain branches map.
    branches = data.get("branches") if isinstance(data, dict) else None
    if isinstance(branches, dict):
        node = branches.get(branch)
        if isinstance(node, dict):
            return [str(x) for x in node.keys()]
        if isinstance(node, list):
            return [str(x) for x in node]
    return []


class JobView(discord.ui.View):
    def __init__(self, job_id: int):
        super().__init__(timeout=None)
        self.job_id = int(job_id)

    async def interaction_check(self, interaction):
        if not is_admin(interaction.user):
            await interaction.response.send_message("هذه اللوحة للإدارة فقط.", ephemeral=True)
            return False
        return True

    async def act(self, interaction, action):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await api("POST", f"/api/import/jobs/{self.job_id}/action", {"action": action})
            await interaction.followup.send("✅ تم تنفيذ العملية.", ephemeral=True)
            try:
                await interaction.message.edit(content=result.get("text", "تم التحديث."), view=self)
            except Exception:
                pass
        except Exception as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)

    @discord.ui.button(label="إيقاف مؤقت", emoji="⏸️", style=discord.ButtonStyle.secondary)
    async def pause(self, interaction, button):
        await self.act(interaction, "pause")

    @discord.ui.button(label="استكمال", emoji="▶️", style=discord.ButtonStyle.success)
    async def resume(self, interaction, button):
        await self.act(interaction, "resume")

    @discord.ui.button(label="إلغاء", emoji="🛑", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction, button):
        await self.act(interaction, "cancel")

    @discord.ui.button(label="تحديث نفس الرسالة", emoji="🔄", style=discord.ButtonStyle.primary)
    async def refresh(self, interaction, button):
        await interaction.response.defer()
        try:
            result = await api("GET", f"/api/import/jobs/{self.job_id}")
            await interaction.message.edit(content=result.get("text", ""), view=self)
        except Exception as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)


class RecoveryDashboardView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=900)

    async def interaction_check(self, interaction):
        if not is_admin(interaction.user):
            await interaction.response.send_message("هذه اللوحة للإدارة فقط.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="استرداد تلقائي ذكي", emoji="✨", style=discord.ButtonStyle.success, row=0)
    async def automatic(self, interaction, button):
        await interaction.response.send_modal(ImportLinksModal())

    @discord.ui.button(label="استرداد لمسار محدد", emoji="🗂️", style=discord.ButtonStyle.primary, row=0)
    async def guided(self, interaction, button):
        await interaction.response.edit_message(
            content="🗂️ اختر الفرع الذي تريد وضع الملفات فيه:",
            view=BranchPickerView(mode="new_import"),
        )

    @discord.ui.button(label="آخر العمليات", emoji="📋", style=discord.ButtonStyle.secondary, row=1)
    async def recent(self, interaction, button):
        jobs = _load_recent_jobs()
        if not jobs:
            return await interaction.response.send_message("لا توجد عمليات محفوظة بعد.", ephemeral=True)
        await interaction.response.edit_message(
            content="📋 اختر العملية:",
            view=RecentJobsView(jobs, action="status"),
        )

    @discord.ui.button(label="عناصر تحتاج مسار", emoji="🧭", style=discord.ButtonStyle.secondary, row=1)
    async def unresolved(self, interaction, button):
        jobs = _load_recent_jobs()
        if not jobs:
            return await interaction.response.send_message("لا توجد عمليات محفوظة بعد.", ephemeral=True)
        await interaction.response.edit_message(
            content="🧭 اختر العملية التي تريد فحص عناصرها:",
            view=RecentJobsView(jobs, action="unresolved"),
        )

    @discord.ui.button(label="إعادة محاولة الأخطاء", emoji="♻️", style=discord.ButtonStyle.secondary, row=1)
    async def retry(self, interaction, button):
        jobs = _load_recent_jobs()
        if not jobs:
            return await interaction.response.send_message("لا توجد عمليات محفوظة بعد.", ephemeral=True)
        await interaction.response.edit_message(
            content="♻️ اختر العملية التي تريد إعادة محاولة أخطائها:",
            view=RecentJobsView(jobs, action="retry"),
        )

    @discord.ui.button(label="قواعد التعلم", emoji="🧠", style=discord.ButtonStyle.secondary, row=2)
    async def rules(self, interaction, button):
        await interaction.response.defer(ephemeral=True)
        try:
            result = await api("GET", "/api/import/rules")
            rows = result.get("items", [])
            text = "\n\n".join(
                f"`#{r['id']}` `{r['pattern']}`\n"
                f"→ {r['branch']} / {r['subject']} / {r['category']} — استخدامات: {r['uses']}"
                for r in rows
            ) or "لا توجد قواعد محفوظة."
            await interaction.followup.send(text[:1900], ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)


class ImportLinksModal(discord.ui.Modal, title="استرداد من Google Drive"):
    links = discord.ui.TextInput(
        label="الروابط",
        placeholder="الصق رابطاً أو عدة روابط، كل رابط في سطر",
        style=discord.TextStyle.paragraph,
        max_length=4000,
    )

    def __init__(self, path_state: dict | None = None):
        super().__init__()
        self.path_state = path_state or {}

    async def on_submit(self, interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        payload = {
            "refs": str(self.links.value),
            "source_account": "",
            "branch": self.path_state.get("branch", ""),
            "subject": self.path_state.get("subject", ""),
            "category": self.path_state.get("category", ""),
            "year": self.path_state.get("year", ""),
            "actor_id": interaction.user.id,
            "guild_id": interaction.guild_id or 0,
        }
        try:
            result = await api("POST", "/api/import/jobs", payload)
            job_id = int(result["job_id"])
            _save_recent_job(job_id)
            status = await api("GET", f"/api/import/jobs/{job_id}")
            await interaction.channel.send(status.get("text", f"عملية #{job_id}"), view=JobView(job_id))
            await interaction.followup.send(f"✅ بدأت العملية رقم `{job_id}`.", ephemeral=True)
        except Exception as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)


class BranchPickerView(discord.ui.View):
    def __init__(self, mode: str, state: dict | None = None):
        super().__init__(timeout=900)
        self.mode = mode
        self.state = dict(state or {})
        options = [discord.SelectOption(label=b, value=b, emoji="✅") for b in BRANCHES]
        select = discord.ui.Select(placeholder="اختر الفرع", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        branch = select.values[0]
        self.state["branch"] = branch
        subjects = _subjects_for_branch(branch)
        if not subjects:
            # لا نخمن أسماء مواد غير موجودة في المكتبة المحلية.
            return await interaction.response.edit_message(
                content=(
                    f"✅ الفرع: **{branch}**\n\n"
                    "لم تصل قائمة مواد هذا الفرع إلى ملف materials.json بعد.\n"
                    "يمكنك استخدام **الاسترداد التلقائي** الآن، وبعد أول مزامنة للمكتبة ستظهر المواد هنا بالضغط."
                ),
                view=RecoveryDashboardView(),
            )
        await interaction.response.edit_message(
            content=f"✅ الفرع: **{branch}**\nاختر المادة:",
            view=SubjectPickerView(self.mode, self.state, subjects),
        )


class SubjectPickerView(discord.ui.View):
    def __init__(self, mode: str, state: dict, subjects: list[str]):
        super().__init__(timeout=900)
        self.mode = mode
        self.state = dict(state)
        options = [discord.SelectOption(label=s[:100], value=s) for s in subjects[:25]]
        select = discord.ui.Select(placeholder="اختر المادة", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        self.state["subject"] = select.values[0]
        await interaction.response.edit_message(
            content=(
                f"الفرع: **{self.state['branch']}**\n"
                f"المادة: **{self.state['subject']}**\n"
                "اختر التصنيف:"
            ),
            view=CategoryPickerView(self.mode, self.state),
        )


class CategoryPickerView(discord.ui.View):
    def __init__(self, mode: str, state: dict):
        super().__init__(timeout=900)
        self.mode = mode
        self.state = dict(state)
        options = [discord.SelectOption(label=x, value=x) for x in CATEGORIES]
        select = discord.ui.Select(placeholder="اختر التصنيف", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        self.state["category"] = select.values[0]
        if self.state["category"] == "مواد":
            self.state["year"] = ""
            return await self._finish(interaction)
        await interaction.response.edit_message(
            content=(
                f"المسار حتى الآن:\n"
                f"**{self.state['branch']} → {self.state['subject']} → {self.state['category']}**\n"
                "اختر السنة أو بدون سنة:"
            ),
            view=YearPickerView(self.mode, self.state),
        )

    async def _finish(self, interaction):
        if self.mode == "new_import":
            return await interaction.response.send_modal(ImportLinksModal(self.state))
        await interaction.response.edit_message(
            content="اختر العنصر أولاً من قائمة العناصر التي تحتاج مساراً.",
            view=RecoveryDashboardView(),
        )


class YearPickerView(discord.ui.View):
    def __init__(self, mode: str, state: dict):
        super().__init__(timeout=900)
        self.mode = mode
        self.state = dict(state)
        options = [discord.SelectOption(label="بدون سنة", value="")] + [
            discord.SelectOption(label=y, value=y) for y in YEARS
        ]
        select = discord.ui.Select(placeholder="اختر السنة", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        self.state["year"] = select.values[0]
        if self.mode == "new_import":
            await interaction.response.send_modal(ImportLinksModal(self.state))
        else:
            await interaction.response.edit_message(
                content="تم تجهيز المسار. ارجع لقائمة العنصر واضغط تعيين المسار.",
                view=RecoveryDashboardView(),
            )


class RecentJobsView(discord.ui.View):
    def __init__(self, jobs: list[int], action: str):
        super().__init__(timeout=900)
        self.action = action
        options = [
            discord.SelectOption(label=f"العملية #{job_id}", value=str(job_id), emoji="✅")
            for job_id in jobs[:25]
        ]
        select = discord.ui.Select(placeholder="اختر العملية", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        job_id = int(select.values[0])

        if self.action == "status":
            await interaction.response.defer(ephemeral=True)
            try:
                result = await api("GET", f"/api/import/jobs/{job_id}")
                await interaction.followup.send(result.get("text", ""), view=JobView(job_id), ephemeral=True)
            except Exception as exc:
                await interaction.followup.send(f"❌ {exc}", ephemeral=True)
            return

        if self.action == "retry":
            await interaction.response.defer(ephemeral=True)
            try:
                await api("POST", f"/api/import/jobs/{job_id}/action", {"action": "retry"})
                await interaction.followup.send("✅ تم تجهيز الأخطاء لإعادة المحاولة.", ephemeral=True)
            except Exception as exc:
                await interaction.followup.send(f"❌ {exc}", ephemeral=True)
            return

        if self.action == "unresolved":
            await interaction.response.defer(ephemeral=True)
            try:
                result = await api("GET", f"/api/import/jobs/{job_id}/unresolved")
                rows = result.get("items", [])
                if not rows:
                    return await interaction.followup.send("✅ لا توجد عناصر تحتاج مساراً.", ephemeral=True)
                await interaction.followup.send(
                    "🧭 اختر العنصر الذي تريد تعيين مساره:",
                    view=UnresolvedItemsView(rows),
                    ephemeral=True,
                )
            except Exception as exc:
                await interaction.followup.send(f"❌ {exc}", ephemeral=True)


class UnresolvedItemsView(discord.ui.View):
    def __init__(self, rows: list[dict]):
        super().__init__(timeout=900)
        self.rows = rows[:25]
        options = []
        for row in self.rows:
            name = str(row.get("source_name") or f"عنصر #{row['id']}")
            options.append(
                discord.SelectOption(
                    label=name[:100],
                    value=str(row["id"]),
                    description=f"الثقة {float(row.get('confidence', 0)):.0%}"[:100],
                )
            )
        select = discord.ui.Select(placeholder="اختر العنصر", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        select = self.children[0]
        item_id = int(select.values[0])
        row = next(x for x in self.rows if int(x["id"]) == item_id)
        await interaction.response.edit_message(
            content=(
                f"🧭 **{row.get('source_name', item_id)}**\n"
                "اختر الفرع أولاً، وبعدها المادة والتصنيف والسنة بالضغط."
            ),
            view=MapBranchPickerView(item_id),
        )


class MapBranchPickerView(discord.ui.View):
    def __init__(self, item_id: int):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        options = [discord.SelectOption(label=b, value=b) for b in BRANCHES]
        select = discord.ui.Select(placeholder="اختر الفرع", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        branch = self.children[0].values[0]
        subjects = _subjects_for_branch(branch)
        if not subjects:
            return await interaction.response.send_message(
                "لم تصل مواد هذا الفرع إلى materials.json بعد.",
                ephemeral=True,
            )
        await interaction.response.edit_message(
            content=f"الفرع: **{branch}**\nاختر المادة:",
            view=MapSubjectPickerView(self.item_id, {"branch": branch}, subjects),
        )


class MapSubjectPickerView(discord.ui.View):
    def __init__(self, item_id: int, state: dict, subjects: list[str]):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        self.state = dict(state)
        options = [discord.SelectOption(label=s[:100], value=s) for s in subjects[:25]]
        select = discord.ui.Select(placeholder="اختر المادة", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        self.state["subject"] = self.children[0].values[0]
        await interaction.response.edit_message(
            content=f"اختر التصنيف لـ **{self.state['subject']}**:",
            view=MapCategoryPickerView(self.item_id, self.state),
        )


class MapCategoryPickerView(discord.ui.View):
    def __init__(self, item_id: int, state: dict):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        self.state = dict(state)
        options = [discord.SelectOption(label=x, value=x) for x in CATEGORIES]
        select = discord.ui.Select(placeholder="اختر التصنيف", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        self.state["category"] = self.children[0].values[0]
        if self.state["category"] == "مواد":
            self.state["year"] = ""
            return await interaction.response.edit_message(
                content="المسار جاهز. اضغط حفظ.",
                view=MapConfirmView(self.item_id, self.state),
            )
        await interaction.response.edit_message(
            content="اختر السنة:",
            view=MapYearPickerView(self.item_id, self.state),
        )


class MapYearPickerView(discord.ui.View):
    def __init__(self, item_id: int, state: dict):
        super().__init__(timeout=900)
        self.item_id = int(item_id)
        self.state = dict(state)
        options = [discord.SelectOption(label="بدون سنة", value="")] + [
            discord.SelectOption(label=y, value=y) for y in YEARS
        ]
        select = discord.ui.Select(placeholder="اختر السنة", options=options)
        select.callback = self._choose
        self.add_item(select)

    async def _choose(self, interaction):
        self.state["year"] = self.children[0].values[0]
        await interaction.response.edit_message(
            content=(
                "✅ **المسار المختار:**\n"
                f"`{self.state['branch']} / {self.state['subject']} / "
                f"{self.state['category']} / {self.state.get('year', '')}`\n\n"
                "اضغط حفظ لتعيينه."
            ),
            view=MapConfirmView(self.item_id, self.state),
        )


class MapConfirmView(discord.ui.View):
    def __init__(self, item_id: int, state: dict):
        super().__init__(timeout=600)
        self.item_id = int(item_id)
        self.state = dict(state)

    @discord.ui.button(label="حفظ المسار", emoji="💾", style=discord.ButtonStyle.success)
    async def save(self, interaction, button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await api(
                "POST",
                f"/api/import/items/{self.item_id}/map",
                {
                    "branch": self.state["branch"],
                    "subject": self.state["subject"],
                    "category": self.state["category"],
                    "year": self.state.get("year", ""),
                    "session": "",
                    "save_rule": True,
                    "actor_id": interaction.user.id,
                },
            )
            await interaction.followup.send(
                f"✅ تم تعيين المسار:\n`{result.get('logical_path', '')}`",
                ephemeral=True,
            )
        except Exception as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)


@bot.tree.command(name="إدارة_الاسترداد", description="لوحة واحدة لكل عمليات استرداد واستيراد Google Drive")
async def recovery_dashboard(interaction: discord.Interaction):
    if not is_admin(interaction.user):
        return await interaction.response.send_message("هذا الأمر للإدارة فقط.", ephemeral=True)
    await interaction.response.send_message(
        "♻️ **إدارة الاسترداد**\nالاسترداد التلقائي يفصل كل ملف لحاله على الفرع والمادة والتصنيف، ويتجاهل المكرر تلقائيًا.\n\nاختر ما تريد عمله بالضغط:",
        view=RecoveryDashboardView(),
        ephemeral=True,
    )


@bot.event
async def on_ready():
    # حذف أي أوامر قديمة بقيت مسجلة من النسخة السابقة.
    for old_name in (
        "استيراد_درايف",
        "حالة_الاستيراد",
        "عناصر_تحتاج_مسار",
        "تعيين_مسار_استيراد",
        "إعادة_محاولة_أخطاء_الاستيراد",
        "قواعد_تعلم_الاستيراد",
    ):
        try:
            bot.tree.remove_command(old_name)
        except Exception:
            pass
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            await bot.tree.sync(guild=guild)
        else:
            await bot.tree.sync()
    except Exception as exc:
        print(f"[drive-recovery] تعذر مزامنة الأوامر: {exc}")
    print(f"[drive-recovery] جاهز: {bot.user}")


async def run_import_client():
    if not TOKEN:
        raise RuntimeError("DRIVE_IMPORT_BOT_TOKEN غير موجود")
    async with bot:
        await bot.start(TOKEN)
