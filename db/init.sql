CREATE TABLE IF NOT EXISTS files (
    file_id       TEXT PRIMARY KEY,
    filename      TEXT NOT NULL,
    total_size    BIGINT,
    chunk_count   INTEGER,
    processing_timings JSONB DEFAULT '{}'::jsonb,
    uploaded_at   TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE files
    ADD COLUMN IF NOT EXISTS processing_timings JSONB DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS unique_chunks (
    chunk_hash TEXT PRIMARY KEY,
    size_bytes BIGINT NOT NULL,
    crc32 BIGINT NOT NULL,
    object_key TEXT NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS file_chunk_refs (
    id SERIAL PRIMARY KEY,
    file_id TEXT NOT NULL REFERENCES files(file_id),
    chunk_index INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL REFERENCES unique_chunks(chunk_hash),
    logical_size BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS chunk_metadata (
    id            SERIAL PRIMARY KEY,
    file_id       TEXT REFERENCES files(file_id),
    chunk_index   INTEGER NOT NULL,
    sha256        TEXT NOT NULL,
    crc32         BIGINT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    object_key    TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL,
    stored_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS benchmark_results (
    id                      SERIAL PRIMARY KEY,
    benchmark_mode          TEXT NOT NULL,
    backend                 TEXT NOT NULL,
    dataset_type            TEXT NOT NULL,
    file_size_mb            INTEGER NOT NULL,
    chunk_size_kb           INTEGER NOT NULL,
    run_number              INTEGER NOT NULL,
    elapsed_seconds         FLOAT NOT NULL,
    throughput_mb_s         FLOAT NOT NULL,
    speedup_vs_sequential   FLOAT,
    chunk_count             INTEGER NOT NULL,
    unique_chunks           INTEGER NOT NULL,
    duplicate_chunks        INTEGER NOT NULL,
    bytes_original          BIGINT NOT NULL,
    bytes_stored            BIGINT NOT NULL,
    bytes_saved             BIGINT NOT NULL,
    storage_saving_pct      FLOAT NOT NULL,
    dedup_ratio             FLOAT NOT NULL,
    integrity_ok            BOOLEAN NOT NULL,
    cpu_avg_pct             FLOAT,
    cpu_max_pct             FLOAT,
    ram_delta_mb            FLOAT,
    peak_memory_mb          FLOAT,
    ray_workers             INTEGER,
    recorded_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS batch_benchmark_results (
    id                      SERIAL PRIMARY KEY,
    batch_id                TEXT NOT NULL,
    dataset_type            TEXT NOT NULL,
    num_files               INTEGER NOT NULL,
    file_size_mb            INTEGER NOT NULL,
    total_input_mb          INTEGER NOT NULL,
    chunk_size_kb           INTEGER NOT NULL,
    backend                 TEXT NOT NULL,
    run_number              INTEGER NOT NULL,
    generation_seconds      FLOAT DEFAULT 0,
    send_wait_seconds       FLOAT DEFAULT 0,
    aggregate_seconds       FLOAT DEFAULT 0,
    total_elapsed_seconds   FLOAT NOT NULL,
    batch_throughput_mb_s   FLOAT NOT NULL,
    files_per_second        FLOAT NOT NULL,
    upload_request_total_seconds FLOAT DEFAULT 0,
    upload_request_avg_seconds   FLOAT DEFAULT 0,
    integrity_download_total_seconds FLOAT DEFAULT 0,
    integrity_download_avg_seconds   FLOAT DEFAULT 0,
    payload_load_avg_seconds FLOAT DEFAULT 0,
    chunk_split_avg_seconds FLOAT DEFAULT 0,
    preprocess_avg_seconds  FLOAT DEFAULT 0,
    dedup_store_avg_seconds FLOAT DEFAULT 0,
    encryption_avg_seconds  FLOAT DEFAULT 0,
    object_upload_avg_seconds FLOAT DEFAULT 0,
    cleanup_avg_seconds     FLOAT DEFAULT 0,
    worker_total_avg_seconds FLOAT DEFAULT 0,
    total_chunks            INTEGER NOT NULL,
    total_unique_chunks     INTEGER NOT NULL,
    total_duplicate_chunks  INTEGER NOT NULL,
    total_bytes_original    BIGINT NOT NULL,
    total_bytes_stored      BIGINT NOT NULL,
    total_bytes_saved       BIGINT NOT NULL,
    storage_saving_pct      FLOAT NOT NULL,
    dedup_ratio             FLOAT NOT NULL,
    integrity_ok_count      INTEGER NOT NULL,
    integrity_failed_count  INTEGER NOT NULL,
    cpu_avg_pct             FLOAT,
    cpu_max_pct             FLOAT,
    ram_delta_mb            FLOAT,
    peak_memory_mb          FLOAT,
    ray_workers             INTEGER,
    upload_workers          INTEGER,
    concurrency             INTEGER NOT NULL,
    recorded_at             TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE batch_benchmark_results
    ADD COLUMN IF NOT EXISTS generation_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS send_wait_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS aggregate_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS upload_request_total_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS upload_request_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS integrity_download_total_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS integrity_download_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS payload_load_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS chunk_split_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS preprocess_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dedup_store_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS encryption_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS object_upload_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cleanup_avg_seconds FLOAT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS worker_total_avg_seconds FLOAT DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_unique_chunks_ref_count
    ON unique_chunks(ref_count);

CREATE INDEX IF NOT EXISTS idx_file_chunk_refs_file_id
    ON file_chunk_refs(file_id, chunk_index);

CREATE INDEX IF NOT EXISTS idx_chunk_metadata_file_id
    ON chunk_metadata(file_id);

CREATE INDEX IF NOT EXISTS idx_benchmark_backend
    ON benchmark_results(backend, file_size_mb);

CREATE INDEX IF NOT EXISTS idx_benchmark_dataset
    ON benchmark_results(dataset_type, chunk_size_kb);

CREATE INDEX IF NOT EXISTS idx_batch_benchmark_batch
    ON batch_benchmark_results(batch_id);

CREATE INDEX IF NOT EXISTS idx_batch_benchmark_dataset
    ON batch_benchmark_results(dataset_type, backend);
