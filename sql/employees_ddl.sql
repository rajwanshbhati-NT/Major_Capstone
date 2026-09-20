CREATE TABLE IF NOT EXISTS stg_employees (
    employee_id            VARCHAR(7),
    first_name             VARCHAR(15),
    last_name              VARCHAR(15),
    email                  VARCHAR(100),
    phone_number_encrypted TEXT,
    hire_date              VARCHAR(10),
    department             VARCHAR(20),
    job_title              VARCHAR(30),
    salary_encrypted       TEXT,
    currency               VARCHAR(3),
    employment_status      VARCHAR(13),
    manager_id             VARCHAR(7),
    is_active              VARCHAR(4),
    skills_json            TEXT,
    address_json           TEXT,
    emergency_contact_json TEXT,
    ingestion_timestamp    TIMESTAMP NOT NULL,
    execution_id           VARCHAR(64) NOT NULL,
    source_creation_time   TIMESTAMP NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_stg_employees_execution_id
    ON stg_employees (execution_id);
