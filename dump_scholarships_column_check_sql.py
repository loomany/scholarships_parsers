#!/usr/bin/env python3
"""
Print SQL for Supabase SQL Editor: lists upsert columns missing from public.scholarships.
Run from the parsers directory:
  py -3 dump_scholarships_column_check_sql.py
"""
from __future__ import annotations

from scholarship_db_columns import SCHOLARSHIP_UPSERT_BODY_KEYS


def main() -> None:
    rows = ",\n  ".join(f"('{k}')" for k in SCHOLARSHIP_UPSERT_BODY_KEYS)
    print(
        """-- Expected upsert columns (scholarship_db_columns.SCHOLARSHIP_UPSERT_BODY_KEYS).
-- Empty result = OK. Otherwise run migrations or
-- supabase/migrations/20260401000000_scholarships_full_schema_reconcile.sql
WITH expected (column_name) AS (VALUES
  """
        + rows
        + """
)
SELECT e.column_name AS missing_in_public_scholarships
FROM expected e
WHERE NOT EXISTS (
  SELECT 1 FROM information_schema.columns c
  WHERE c.table_schema = 'public'
    AND c.table_name = 'scholarships'
    AND c.column_name = e.column_name
)
ORDER BY 1;
"""
    )


if __name__ == "__main__":
    main()
