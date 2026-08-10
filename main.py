# -*- coding: utf-8 -*-
"""تشغيل بوت المكتبة وبوت استيراد Drive مع Flask داخل Process واحد."""

import asyncio
import os
import threading

import discord

from library_bot import build_library_bot, run_library_web
from drive_import_client import bot as import_bot


def start_flask() -> None:
    thread = threading.Thread(
        target=run_library_web,
        daemon=True,
        name="library-flask",
    )
    thread.start()


async def run_forever(bot: discord.Client, token: str, name: str) -> None:
    if not token:
        raise RuntimeError(f"متغير التوكن الخاص بـ{name} غير موجود.")

    while True:
        try:
            print(f"[{name}] بدء التشغيل...")
            await bot.start(token)
        except discord.LoginFailure:
            print(f"[{name}] التوكن غير صحيح أو تم تغييره.")
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[{name}] خطأ: {exc} — إعادة المحاولة بعد 15 ثانية")
            await asyncio.sleep(15)


async def main_async() -> None:
    library_bot = build_library_bot()

    await asyncio.gather(
        run_forever(
            library_bot,
            os.getenv("DISCORD_TOKEN_LIBRARY", "").strip(),
            "library",
        ),
        run_forever(
            import_bot,
            os.getenv("DRIVE_IMPORT_BOT_TOKEN", "").strip(),
            "drive-import",
        ),
    )


def main() -> None:
    start_flask()
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
