CREATE TABLE songs_catalog (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL UNIQUE,
  song_number INT,
  in_hymnal BOOLEAN DEFAULT TRUE
);
CREATE TABLE setlist (
  id SERIAL PRIMARY KEY,
  service_date DATE NOT NULL,
  title TEXT NOT NULL,
  source TEXT DEFAULT 'Unknown',
  UNIQUE(service_date, title)
);
