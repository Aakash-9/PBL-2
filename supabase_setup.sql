-- ============================================================
-- QueryMind — Supabase Setup
-- Run this ONCE in your Supabase SQL Editor before starting the backend
-- Dashboard → SQL Editor → New Query → paste → Run
-- ============================================================

-- 1. Dynamic SQL execution function (required for NL2SQL)
CREATE OR REPLACE FUNCTION exec_sql(query text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  result json;
BEGIN
  EXECUTE 'SELECT json_agg(row_to_json(t)) FROM (' || query || ') t' INTO result;
  RETURN COALESCE(result, '[]'::json);
END;
$$;

GRANT EXECUTE ON FUNCTION exec_sql(text) TO service_role;
GRANT EXECUTE ON FUNCTION exec_sql(text) TO anon;
GRANT EXECUTE ON FUNCTION exec_sql(text) TO authenticated;

-- 2. Query audit log (tracks every NL2SQL query)
CREATE TABLE IF NOT EXISTS query_audit_log (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id text,
  user_query text,
  generated_sql text,
  reasoning text,
  confidence text,
  row_count integer,
  rules_used jsonb,
  created_at timestamptz DEFAULT now()
);

-- 3. Metric baselines for anomaly detection
CREATE TABLE IF NOT EXISTS metric_baselines (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  metric_id text NOT NULL UNIQUE,
  baseline_value numeric NOT NULL DEFAULT 0,
  updated_at timestamptz DEFAULT now()
);

-- Done! Your QueryMind backend is ready to connect.
-- Next: fill in .env → pip install -r requirements.txt → python main.py
