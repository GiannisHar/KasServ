import os
import base64
import asyncio
import asyncpg
from fastapi import FastAPI, File, Form, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DB config (same style as the Socket.IO server) ---
db_host     = os.environ.get("DB_HOST")
db_user     = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name     = os.environ.get("DB_NAME")
db_port     = int(os.environ.get("DB_PORT", 5432))

# --- Connection pool ---
pool: asyncpg.Pool = None

@app.on_event("startup")
async def startup():
    global pool
    print("DB_HOST:", db_host)
    pool = await asyncpg.create_pool(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=db_port,
        min_size=2,
        max_size=50,
    )
    print("✅ Database pool created!")
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                serial_number SERIAL PRIMARY KEY,
                image_data    BYTEA        NOT NULL,
                image_mime    TEXT         NOT NULL DEFAULT 'image/jpeg',
                width         INTEGER      NOT NULL,
                height        INTEGER      NOT NULL,
                amount        NUMERIC(12,2) NOT NULL,
                created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)

@app.on_event("shutdown")
async def shutdown():
    await pool.close()


# --- WebSocket manager ---
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: dict):
        message = json.dumps(data)
        dead = []
        for ws in self.active:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active.remove(ws)

manager = ConnectionManager()


# --- REST: upload entry ---
@app.post("/upload")
async def upload_entry(
    image: UploadFile = File(...),
    width: int = Form(...),
    height: int = Form(...),
    amount: float = Form(...),
):
    image_bytes = await image.read()
    mime = image.content_type or "image/jpeg"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO entries (image_data, image_mime, width, height, amount)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING serial_number, width, height, amount, image_mime, created_at
            """,
            image_bytes, mime, width, height, amount
        )

    entry = {
        "serial_number": row["serial_number"],
        "width":         row["width"],
        "height":        row["height"],
        "amount":        float(row["amount"]),
        "image_mime":    row["image_mime"],
        "image_b64":     base64.b64encode(image_bytes).decode(),
        "created_at":    row["created_at"].isoformat(),
    }

    # Push to all connected clients
    await manager.broadcast({"event": "new_entry", "data": entry})

    return {"ok": True, "serial_number": entry["serial_number"]}


# --- REST: get all entries (for initial page load) ---
@app.get("/entries")
async def get_entries():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT serial_number, image_data, image_mime, width, height, amount, created_at "
            "FROM entries ORDER BY serial_number ASC"
        )
    result = []
    for row in rows:
        result.append({
            "serial_number": row["serial_number"],
            "width":         row["width"],
            "height":        row["height"],
            "amount":        float(row["amount"]),
            "image_mime":    row["image_mime"],
            "image_b64":     base64.b64encode(row["image_data"]).decode(),
            "created_at":    row["created_at"].isoformat(),
        })
    return result


# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; we only push from server side
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"event": "ping"}))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


# --- Serve frontend ---
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    with open("index.html", "r") as f:
        return f.read()
