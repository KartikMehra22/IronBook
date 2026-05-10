CREATE TABLE IF NOT EXISTS submissions (
  id            UUID PRIMARY KEY,
  sha256        BYTEA NOT NULL UNIQUE,
  language      TEXT NOT NULL CHECK (language IN ('rust','go','cpp')),
  status        TEXT NOT NULL CHECK (status IN ('PENDING','BUILDING','READY','REJECTED')),
  image_digest  TEXT,
  reject_reason TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS submissions_status_idx ON submissions (status);
INSERT INTO schema_version (version) VALUES (2) ON CONFLICT DO NOTHING;
