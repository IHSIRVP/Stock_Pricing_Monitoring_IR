from fastapi import FastAPI
from typing import Union
import time 
import asyncio
import os
from pathlib import Path
from pathlib import Path
from dotenv import load_dotenv
import os
from pydantic import BaseModel
from fastapi.concurrency import run_in_threadpool


@app.get("/generate-stock-report")
def generate_stock_report():
    try:
        file_path = main_entry_function()

        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=500, detail="Report generation failed")

        return FileResponse(
            path=file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(file_path)
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
