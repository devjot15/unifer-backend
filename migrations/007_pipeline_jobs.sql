-- Migration 007: pipeline_jobs table for stage-by-stage bulk ingestion
-- Each university gets one row per pipeline stage.
-- The worker processes ALL universities at stage N before advancing to stage N+1.
-- Universities that fail a stage have all subsequent stages automatically skipped.

CREATE TABLE IF NOT EXISTS ingestion.pipeline_jobs (
  id            UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  university_id UUID        NOT NULL,
  stage         TEXT        NOT NULL CHECK (stage IN ('crawl', 'scrape', 'parse', 'fee_scrape', 'fix')),
  status        TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped')),
  attempts      INTEGER     NOT NULL DEFAULT 0,
  max_attempts  INTEGER     NOT NULL DEFAULT 3,
  error_message TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at    TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,

  UNIQUE (university_id, stage)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_stage_status
  ON ingestion.pipeline_jobs (stage, status);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_university_id
  ON ingestion.pipeline_jobs (university_id);
