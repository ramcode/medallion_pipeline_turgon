CREATE TABLE silver_tickets (
  ticket_id VARCHAR(32) NOT NULL,
  created_at_parsed TIMESTAMP,
  resolved_at_parsed TIMESTAMP,
  category VARCHAR(64) NOT NULL,
  priority VARCHAR(16) NOT NULL,
  status VARCHAR(32) NOT NULL,
  building VARCHAR(128),
  cost_decimal DECIMAL(12,2),
  sla_hours_decimal DECIMAL(10,2),
  resolution_hours DECIMAL(12,2),
  is_sla_breached BOOLEAN,
  _row_hash CHAR(64) NOT NULL,
  _source_file VARCHAR(512) NOT NULL,
  _ingested_at TIMESTAMP NOT NULL
);
