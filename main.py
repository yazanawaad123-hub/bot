# -*- coding: utf-8 -*-
"""تشغيل بوت رفع الملفات وخادم Flask داخل Process واحد."""

import asyncio
import os
import threading

import discord

from upload_bot import build_upload_bot, run_web


def start_flask() -> None:
    thread = threading.Thread(
        target=run_web,
        daemon=True,
        name="upload-flask",
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
    upload_bot = build_upload_bot()
    await run_forever(
        upload_bot,
        os.getenv("DISCORD_TOKEN_UPLOAD", "").strip(),
        "upload",
    )


def main() -> None:
    start_flask()
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
