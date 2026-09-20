CREATE TABLE IF NOT EXISTS ingestion_count (
    id BIGSERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL UNIQUE,
    file_path TEXT NOT NULL,
    file_name VARCHAR(500),
    file_type VARCHAR(50),
    target_table VARCHAR(255),
    expected_count BIGINT,
    actual_count BIGINT,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASSED', 'FAILED', 'RUNNING')),
    failure_reason TEXT,
    execution_timestamp TIMESTAMP NOT NULL,
    creation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

CREATE INDEX IF NOT EXISTS idx_ingestion_count_execution_id ON ingestion_count(execution_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_count_status ON ingestion_count(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_count_creation_timestamp ON ingestion_count(creation_timestamp DESC);