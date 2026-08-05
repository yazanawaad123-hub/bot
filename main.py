import asyncio, os, threading
from upload_service import build_upload_bot, run_web

def main():
    token=os.getenv('DISCORD_TOKEN_UPLOAD','').strip()
    if not token: raise RuntimeError('DISCORD_TOKEN_UPLOAD غير موجود')
    threading.Thread(target=run_web,daemon=True,name='upload-web').start()
    asyncio.run(build_upload_bot().start(token))

if __name__=='__main__': main()
