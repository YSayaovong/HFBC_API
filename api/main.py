from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os, psycopg2

app = FastAPI(title="HFBC KPIs API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

@app.get("/health")
def health(): return {"ok": True}

@app.get("/api/top10")
def top10():
    with conn() as c, c.cursor() as cur:
        cur.execute("""
          SELECT title, COUNT(*) AS plays
          FROM setlist
          WHERE service_date >= CURRENT_DATE - INTERVAL '364 days'
          AND lower(title) NOT IN ('na','n/a','church close','church close - flood')
          GROUP BY title ORDER BY plays DESC LIMIT 10
        """)
        return [{"Title": r[0], "Plays": int(r[1])} for r in cur.fetchall()]
