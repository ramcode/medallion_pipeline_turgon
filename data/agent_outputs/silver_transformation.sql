INSERT INTO silver_tickets
SELECT
  UPPER(TRIM(ticket_id)) AS ticket_id,
  TRY_CAST(created_at AS TIMESTAMP) AS created_at_parsed,
  TRY_CAST(resolved_at AS TIMESTAMP) AS resolved_at_parsed,
  CASE WHEN LOWER(category) LIKE '%pest%' THEN 'Pest Control' ELSE 'Other' END AS category,
  TRY_CAST(REPLACE(cost, '$', '') AS DECIMAL(12,2)) AS cost_decimal,
  TRY_CAST(sla_hours AS DECIMAL(10,2)) AS sla_hours_decimal,
  _row_hash, _source_file, _ingested_at
FROM bronze_tickets
WHERE REGEXP_LIKE(ticket_id, '^TKT-[0-9]+$');
