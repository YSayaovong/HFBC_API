from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List
import os, psycopg2, psycopg2.extras

# --- Config ---
API_TITLE = "HFBC KPIs API"
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")  # e.g. "https://ysayaovong.github.io"
API_KEY = os.environ.get("API_KEY")  # optional
DATABASE_URL = os.environ.get("DATABASE_URL")  # required in deploy

app = FastAPI(title=API_TITLE, version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL)

# --- Bootstrap: create tables if they don't exist (no Alembic needed) ---
DDL = """
CREATE TABLE IF NOT EXISTS songs_catalog (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL UNIQUE,
  song_number INT,
  in_hymnal BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS setlist (
  id SERIAL PRIMARY KEY,
  service_date DATE NOT NULL,
  title TEXT NOT NULL,
  source TEXT DEFAULT 'Unknown',
  UNIQUE(service_date, title)
);
"""
@app.on_event("startup")
def ensure_schema():
    try:
        with conn() as c, c.cursor() as cur:
            cur.execute(DDL)
    except Exception as e:
        print(f"[BOOTSTRAP] Failed to ensure schema: {e}")

# --- Security (optional key) ---
def require_key(req: Request):
    if not API_KEY:  # open API if no key configured
        return
    if req.headers.get("x-api-key") != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# --- Models ---
class SetlistRow(BaseModel):
    service_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")  # "YYYY-MM-DD"
    title: str
    source: Optional[str] = "Unknown"

# --- Helpers ---
def cache_headers(resp):
    resp.headers["Cache-Control"] = "public, max-age=300"  # 5 min
    return resp

# --- Health ---
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/ready")
def ready():
    try:
        with conn() as c, c.cursor() as cur:
            cur.execute("SELECT 1")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# --- Read endpoints ---
@app.get("/api/top10")
def top10():
    with conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
          SELECT title AS "Title", COUNT(*)::int AS "Plays"
          FROM setlist
          WHERE service_date >= CURRENT_DATE - INTERVAL '364 days'
            AND lower(title) NOT IN ('na','n/a','church close','church close - flood')
          GROUP BY title
          ORDER BY "Plays" DESC
          LIMIT 10
        """)
        rows = cur.fetchall()
    from fastapi.responses import JSONResponse
    return cache_headers(JSONResponse(rows))

@app.get("/api/hymnal-coverage")
def hymnal_coverage():
    with conn() as c, c.cursor() as cur:
        cur.execute("""
          WITH used AS (
            SELECT DISTINCT title FROM setlist
            WHERE service_date >= CURRENT_DATE - INTERVAL '364 days'
          )
          SELECT
            COALESCE(SUM(CASE WHEN in_hymnal THEN 1 ELSE 0 END),0)::int AS hymnal_songs,
            COALESCE(SUM(CASE WHEN in_hymnal AND title IN (SELECT title FROM used) THEN 1 ELSE 0 END),0)::int AS used_hymnal
          FROM songs_catalog
        """)
        h, u = cur.fetchone()
        cov = 0.0 if h == 0 else round(100 * u / h, 2)
    from fastapi.responses import JSONResponse
    return cache_headers(JSONResponse({"Hymnal_Songs": h, "Used_Hymnal_Songs": u, "Coverage_%": cov}))

# --- Write endpoint (CRUD demo) ---
@app.post("/api/setlist", dependencies=[Depends(require_key)])
def add_setlist(row: SetlistRow):
    title = row.title.strip()
    source = (row.source or "Unknown").strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title required")
    with conn() as c, c.cursor() as cur:
        cur.execute("""
          INSERT INTO setlist(service_date, title, source)
          VALUES (%s,%s,%s)
          ON CONFLICT(service_date, title) DO NOTHING
        """, (row.service_date, title, source))
    return {"ok": True}
