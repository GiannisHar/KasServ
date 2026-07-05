import os
import base64
import asyncio
import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI, File, Form, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from typing import List
import json

# --- DB config (same style as the Socket.IO server) ---
db_host     = os.environ.get("DB_HOST")
db_user     = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name     = os.environ.get("DB_NAME")
db_port     = int(os.environ.get("DB_PORT", 5432))

# --- Connection pool ---
pool: asyncpg.Pool = None

# --- Amount adjustment step (change this if you want bigger/smaller +/- jumps) ---
AMOUNT_STEP = 1


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
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
        # Fresh installs get the full schema.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                serial_number SERIAL PRIMARY KEY,
                image_data    BYTEA         NOT NULL,
                image_mime    TEXT          NOT NULL DEFAULT 'image/jpeg',
                width         INTEGER       NOT NULL,
                height        INTEGER       NOT NULL,
                amount        NUMERIC(12,2) NOT NULL DEFAULT 0,
                created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
            )
        """)
        # Cleanup: drop the old history-tracking columns if they exist from
        # a previous version of this app. History no longer exists.
        await conn.execute("""
            ALTER TABLE entries
                DROP COLUMN IF EXISTS fulfilled,
                DROP COLUMN IF EXISTS fulfilled_at
        """)
        # Safety net: make sure amount defaults to 0 even on an older table.
        await conn.execute("""
            ALTER TABLE entries
                ALTER COLUMN amount SET DEFAULT 0
        """)
    print("✅ Schema ready!")
    yield
    # --- shutdown ---
    await pool.close()
    print("Database pool closed!")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- WebSocket manager ---
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
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
            self.disconnect(ws)

manager = ConnectionManager()


def row_to_entry(row, image_bytes=None):
    """Build the JSON-safe entry dict the frontend expects."""
    img = image_bytes if image_bytes is not None else row["image_data"]
    return {
        "serial_number": row["serial_number"],
        "width":         row["width"],
        "height":        row["height"],
        "amount":        float(row["amount"]),
        "image_mime":    row["image_mime"],
        "image_b64":     base64.b64encode(img).decode(),
        "created_at":    row["created_at"].isoformat() if row["created_at"] else None,
    }


# --- REST: upload entry (one-time image upload, amount always starts at 0) ---
@app.post("/upload")
async def upload_entry(
    image: UploadFile = File(...),
    width: int = Form(...),
    height: int = Form(...),
):
    image_bytes = await image.read()
    mime = image.content_type or "image/jpeg"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO entries (image_data, image_mime, width, height, amount)
            VALUES ($1, $2, $3, $4, 0)
            RETURNING serial_number, width, height, amount, image_mime, created_at
            """,
            image_bytes, mime, width, height
        )

    entry = row_to_entry(row, image_bytes)

    # Push to all connected clients
    await manager.broadcast({"event": "new_entry", "data": entry})

    return {"ok": True, "serial_number": entry["serial_number"]}


# --- REST: all entries ---
@app.get("/entries")
async def get_entries():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT serial_number, image_data, image_mime, width, height, amount, created_at
            FROM entries
            ORDER BY serial_number ASC
            """
        )
    return [row_to_entry(r) for r in rows]


# --- REST: bump amount up by one step ---
@app.post("/increment")
async def increment_amount(serial_number: int = Form(...)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE entries
            SET amount = amount + $2
            WHERE serial_number = $1
            RETURNING serial_number, width, height, amount, image_mime, created_at
            """,
            serial_number, AMOUNT_STEP
        )

    if row is None:
        return {"ok": False, "reason": "not_found"}

    payload = {"serial_number": row["serial_number"], "amount": float(row["amount"])}
    await manager.broadcast({"event": "amount_updated", "data": payload})
    return {"ok": True, **payload}


# --- REST: bump amount down by one step (never goes below 0) ---
@app.post("/decrement")
async def decrement_amount(serial_number: int = Form(...)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE entries
            SET amount = GREATEST(amount - $2, 0)
            WHERE serial_number = $1
            RETURNING serial_number, width, height, amount, image_mime, created_at
            """,
            serial_number, AMOUNT_STEP
        )

    if row is None:
        return {"ok": False, "reason": "not_found"}

    payload = {"serial_number": row["serial_number"], "amount": float(row["amount"])}
    await manager.broadcast({"event": "amount_updated", "data": payload})
    return {"ok": True, **payload}


# --- REST: delete an entry permanently (no history kept) ---
@app.post("/delete")
async def delete_entry(serial_number: int = Form(...)):
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM entries WHERE serial_number = $1",
            serial_number
        )

    deleted = result.endswith("1")  # "DELETE 1" vs "DELETE 0"

    if not deleted:
        return {"ok": False, "reason": "not_found"}

    await manager.broadcast({"event": "entry_deleted", "data": {"serial_number": serial_number}})
    return {"ok": True, "serial_number": serial_number}


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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)