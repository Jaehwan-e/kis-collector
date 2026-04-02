"""엔트리포인트 — python run.py"""
import asyncio
import os

os.makedirs("logs", exist_ok=True)

from app.main import main

asyncio.run(main())
