CREATE TABLE IF NOT EXISTS appointments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_code VARCHAR(64) NOT NULL,
    doctor VARCHAR(128) NOT NULL,
    room VARCHAR(32) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    duration_minutes INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Використовуємо явне приведення часу до UTC для сумісності з індексом
CREATE INDEX IF NOT EXISTS idx_appointments_start_time_date
ON appointments (((start_time AT TIME ZONE 'UTC')::date));

-- ===== Лабораторна 4: черга задач і результати =====
CREATE TABLE IF NOT EXISTS tasks (
    id           BIGSERIAL PRIMARY KEY,
    record_id    UUID NOT NULL REFERENCES appointments (id),
    status       VARCHAR(16) NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'done', 'failed')),
    seed         INTEGER NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    token        BIGINT NOT NULL DEFAULT 0,
    lease_until  TIMESTAMPTZ,
    worker_id    VARCHAR(128),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks (status, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_record_status ON tasks (record_id, status);

CREATE TABLE IF NOT EXISTS task_results (
    task_id    BIGINT PRIMARY KEY REFERENCES tasks (id),
    token      BIGINT NOT NULL,
    worker_id  VARCHAR(128) NOT NULL,
    result     JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
