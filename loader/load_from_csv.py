import os, psycopg2, pandas as pd
from urllib.request import urlopen

BASE = os.environ.get("SOURCE_BASE", "https://raw.githubusercontent.com/YSayaovong/HFBC_Praise_Worship/main")
SETLIST = f"{BASE}/setlist/setlist.xlsx"
CATALOG = f"{BASE}/setlist/songs_catalog.csv"

def df_xlsx(url): return pd.read_excel(urlopen(url))
def df_csv(url):  return pd.read_csv(urlopen(url))

def main():
    cx = psycopg2.connect(os.environ["DATABASE_URL"]); cx.autocommit = True
    with cx, cx.cursor() as cur:
        # songs_catalog
        cat = df_csv(CATALOG)
        cat.columns = [c.strip() for c in cat.columns]
        title = cat.filter(regex=r"(?i)title").iloc[:,0].astype(str).str.strip()
        num   = cat.filter(regex=r"(?i)^(song[_ ]?number|song #|no|number)$", axis=1)
        num   = num.iloc[:,0] if not num.empty else None
        inh   = cat.filter(regex=r"(?i)in[_ ]?hymnal", axis=1)
        inh   = inh.iloc[:,0].astype(str).str.lower().isin({"1","true","yes","y"}) if not inh.empty else None
        for i, t in enumerate(title):
            cur.execute("""
              INSERT INTO songs_catalog(title, song_number, in_hymnal)
              VALUES (%s,%s,%s)
              ON CONFLICT(title) DO UPDATE SET
                song_number = COALESCE(EXCLUDED.song_number, songs_catalog.song_number),
                in_hymnal   = COALESCE(EXCLUDED.in_hymnal, songs_catalog.in_hymnal)
            """, (t, None if num is None else num.iloc[i], True if inh is None else bool(inh.iloc[i])))

        # setlist
        sl = df_xlsx(SETLIST)
        date = pd.to_datetime(sl.filter(regex=r"(?i)date|service_date").iloc[:,0], errors="coerce").dt.date
        title = sl.filter(regex=r"(?i)title|song").iloc[:,0].astype(str).str.strip()
        src   = sl.filter(regex=r"(?i)source|category", axis=1)
        src   = src.iloc[:,0].astype(str).str.strip() if not src.empty else None
        added = 0
        for i in range(len(sl)):
            if pd.isna(date.iloc[i]) or not title.iloc[i]: continue
            cur.execute("""
              INSERT INTO setlist(service_date, title, source)
              VALUES (%s,%s,%s)
              ON CONFLICT(service_date, title) DO NOTHING
            """, (date.iloc[i], title.iloc[i], (None if src is None else src.iloc[i]) or "Unknown"))
            added += 1
        print(f"[OK] Upserted {len(cat)} catalog rows; inserted/upserted ~{added} setlist rows.")

if __name__ == "__main__":
    main()
