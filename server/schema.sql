-- Control plane schema (MNEMO_DSN).

CREATE TABLE IF NOT EXISTS tenants (
  id              VARCHAR(36)   PRIMARY KEY,
  name            VARCHAR(255)  NOT NULL,
  db_host         VARCHAR(255)  NOT NULL,
  db_port         INT           NOT NULL,
  db_user         VARCHAR(255)  NOT NULL,
  db_password     VARCHAR(255)  NOT NULL,
  db_name         VARCHAR(255)  NOT NULL,
  db_tls          TINYINT(1)    NOT NULL DEFAULT 0,
  provider        VARCHAR(50)   NOT NULL,
  cluster_id      VARCHAR(255)  NULL,
  claim_url       TEXT          NULL,
  claim_expires_at TIMESTAMP    NULL,
  status          VARCHAR(20)   NOT NULL DEFAULT 'provisioning'
                  COMMENT 'provisioning|active|suspended|deleted',
  schema_version  INT           NOT NULL DEFAULT 1,
  created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted_at      TIMESTAMP     NULL,
  UNIQUE INDEX idx_tenant_name (name),
  INDEX idx_tenant_status (status),
  INDEX idx_tenant_provider (provider)
);

CREATE TABLE IF NOT EXISTS tenant_activity (
  tenant_id                  VARCHAR(36) NOT NULL PRIMARY KEY,
  last_activity_at           TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  active_memory_total        BIGINT      NOT NULL DEFAULT 0,
  active_memory_7d_total     BIGINT      NOT NULL DEFAULT 0,
  memory_stats_observed_at   TIMESTAMP   NULL,
  CONSTRAINT fk_tenant_activity FOREIGN KEY (tenant_id) REFERENCES tenants(id),
  INDEX idx_tenant_activity_last_activity (last_activity_at)
);

CREATE TABLE IF NOT EXISTS space_chains (
  id                  VARCHAR(36)   PRIMARY KEY,
  project_id          VARCHAR(255)  NULL,
  name                VARCHAR(255)  NOT NULL,
  description         TEXT          NULL,
  created_by_user_id  VARCHAR(255)  NULL,
  deleted_at          TIMESTAMP     NULL,
  deleted_by_user_id  VARCHAR(255)  NULL,
  created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_space_chains_project (project_id),
  INDEX idx_space_chains_deleted (deleted_at)
);

CREATE TABLE IF NOT EXISTS space_chain_bindings (
  id                  VARCHAR(36)   PRIMARY KEY,
  chain_id            VARCHAR(36)   NOT NULL,
  chain_api_key       VARCHAR(255)  NOT NULL,
  created_by_user_id  VARCHAR(255)  NULL,
  disabled            TINYINT(1)    NOT NULL DEFAULT 0,
  disabled_at         TIMESTAMP     NULL,
  disabled_by_user_id VARCHAR(255)  NULL,
  created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  UNIQUE INDEX idx_space_chain_bindings_key (chain_api_key),
  INDEX idx_space_chain_bindings_chain (chain_id),
  CONSTRAINT fk_space_chain_bindings_chain FOREIGN KEY (chain_id) REFERENCES space_chains(id)
);

CREATE TABLE IF NOT EXISTS space_chain_nodes (
  id                  VARCHAR(36)   PRIMARY KEY,
  chain_id            VARCHAR(36)   NOT NULL,
  tenant_id           VARCHAR(36)   NOT NULL,
  external_space_id   VARCHAR(255)  NULL,
  display_name        VARCHAR(255)  NULL,
  position            INT           NOT NULL,
  routing_policy_enabled TINYINT(1) NOT NULL DEFAULT 0,
  routing_policy_prompt  TEXT       NULL,
  created_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at          TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE INDEX idx_space_chain_nodes_tenant (chain_id, tenant_id),
  UNIQUE INDEX idx_space_chain_nodes_external_space (chain_id, external_space_id),
  UNIQUE INDEX idx_space_chain_nodes_position (chain_id, position),
  INDEX idx_space_chain_nodes_external_lookup (external_space_id),
  CONSTRAINT fk_space_chain_nodes_chain FOREIGN KEY (chain_id) REFERENCES space_chains(id),
  CONSTRAINT fk_space_chain_nodes_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

-- Tenant data plane schema (per-tenant TiDB Serverless).
CREATE TABLE IF NOT EXISTS memories (
  id              VARCHAR(36)     PRIMARY KEY,
  content         MEDIUMTEXT      NOT NULL,
  source          VARCHAR(100),
  tags            JSON,
  metadata        JSON,
  embedding       VECTOR(1536)    NULL,

  -- Classification
  memory_type     VARCHAR(20)     NOT NULL DEFAULT 'pinned'
                  COMMENT 'pinned|insight|digest',

  -- Agent & session tracking
  agent_id        VARCHAR(100)    NULL     COMMENT 'Agent that created this memory',
  session_id      VARCHAR(100)    NULL     COMMENT 'Session this memory originated from',

  -- Lifecycle
  state           VARCHAR(20)     NOT NULL DEFAULT 'active'
                  COMMENT 'active|paused|archived|deleted',
  version         INT             DEFAULT 1,
  updated_by      VARCHAR(100),
  created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  superseded_by   VARCHAR(36)     NULL     COMMENT 'ID of the memory that replaced this one',
  INDEX idx_memory_type         (memory_type),
  INDEX idx_source              (source),
  INDEX idx_state               (state),
  INDEX idx_agent               (agent_id),
  INDEX idx_session             (session_id),
  INDEX idx_updated             (updated_at)
);

-- Full-text search index (TiDB Cloud Serverless with MULTILINGUAL tokenizer).
-- ADD_COLUMNAR_REPLICA_ON_DEMAND auto-provisions TiFlash on Serverless clusters.
-- Run after the memories table is created. Safe to re-run (fails silently if index exists).
-- ALTER TABLE memories
--   ADD FULLTEXT INDEX idx_fts_content (content)
--   WITH PARSER MULTILINGUAL
--   ADD_COLUMNAR_REPLICA_ON_DEMAND;

-- Vector index requires TiFlash. May fail on plain MySQL; safe to ignore.
-- ALTER TABLE memories ADD VECTOR INDEX idx_cosine ((VEC_COSINE_DISTANCE(embedding)));

-- Auto-embedding variant (TiDB Cloud Serverless only):
-- Replace the embedding column above with a generated column:
--
--   embedding VECTOR(1024) GENERATED ALWAYS AS (
--     EMBED_TEXT("tidbcloud_free/amazon/titan-embed-text-v2", content)
--   ) STORED,
--
-- Then add vector index:
--   VECTOR INDEX idx_cosine ((VEC_COSINE_DISTANCE(embedding)))
--
-- Set MNEMO_EMBED_AUTO_MODEL=tidbcloud_free/amazon/titan-embed-text-v2 to enable.


-- Migration: tombstone -> state (4-step plan).
-- Step 1: Add new columns (backward compatible — existing code still uses tombstone).
-- ALTER TABLE memories
--   ADD COLUMN memory_type  VARCHAR(20) NOT NULL DEFAULT 'pinned',
--   ADD COLUMN agent_id     VARCHAR(100) NULL,
--   ADD COLUMN session_id   VARCHAR(100) NULL,
--   ADD COLUMN state        VARCHAR(20) NOT NULL DEFAULT 'active',
--   ADD COLUMN superseded_by VARCHAR(36) NULL;
-- CREATE INDEX idx_memory_type ON memories(memory_type);
-- CREATE INDEX idx_state ON memories(state);
-- CREATE INDEX idx_agent ON memories(agent_id);
-- CREATE INDEX idx_session ON memories(session_id);
-- Step 2: Migrate tombstoned records.
-- UPDATE memories SET state = 'deleted', deleted_at = updated_at WHERE tombstone = 1;
-- Step 3: Add constraint (AFTER code migration).
-- ALTER TABLE memories ADD CONSTRAINT chk_state CHECK (state IN ('active','paused','archived','deleted'));
-- Step 4: Drop tombstone (separate deployment).
-- ALTER TABLE memories DROP COLUMN tombstone;
-- DROP INDEX idx_tombstone ON memories;


-- K=>V recall refactor (feat/k-v-recall).
-- Two-table split: memory_values holds the canonical fact (V); memory_keys
-- holds query-side aliases (K) that point to a V via FK. Multiple K can
-- reference the same V. Recall queries hit both tables (fast path + RRF
-- over K-FTS / V-FTS / K-VEC / V-VEC subset, gated by retrieval_strategy
-- bitmask).
--
-- Step-1 migration: additive. The existing `memories` table is unchanged.
-- Double-write, backfill, read switchover, and eventual drop happen in
-- subsequent steps.
CREATE TABLE IF NOT EXISTS memory_values (
  id                  VARCHAR(36)     PRIMARY KEY,
  content             MEDIUMTEXT      NOT NULL,
  content_hash        CHAR(64)        NOT NULL    COMMENT 'sha256(keynorm.HashValue(content)) for dedup; HashValue is NFKC+lowercase only (less aggressive than NormalizeKey, which is for K equality)',
  source              VARCHAR(100)    NULL,
  tags                JSON            NULL,
  metadata            JSON            NULL,
  embedding           VECTOR(1536)    NULL,

  -- Classification (mirrors memories.memory_type).
  memory_type         VARCHAR(20)     NOT NULL DEFAULT 'pinned'
                      COMMENT 'pinned|insight|digest',

  -- Agent and session provenance (mirrors memories).
  agent_id            VARCHAR(100)    NULL,
  session_id          VARCHAR(100)    NULL,

  -- Lifecycle (mirrors memories).
  state               VARCHAR(20)     NOT NULL DEFAULT 'active'
                      COMMENT 'active|paused|archived|deleted',
  version             INT             DEFAULT 1,
  updated_by          VARCHAR(100)    NULL,
  created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
  updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  superseded_by       VARCHAR(36)     NULL,

  -- K-extraction lifecycle. NULL = never extracted (orphan, eligible for
  -- backfill). Set on every successful extract_keys call, including the
  -- "extracted 0 keys" case. Timeout/error leaves it NULL for retry.
  keys_extracted_at   TIMESTAMP       NULL,

  UNIQUE KEY uq_content_hash      (content_hash),
  INDEX idx_mv_memory_type        (memory_type),
  INDEX idx_mv_source             (source),
  INDEX idx_mv_state              (state),
  INDEX idx_mv_agent              (agent_id),
  INDEX idx_mv_session            (session_id),
  INDEX idx_mv_updated            (updated_at),
  INDEX idx_mv_keys_extracted_at  (keys_extracted_at)
);

-- FULLTEXT and VECTOR indexes for memory_values:
-- ALTER TABLE memory_values
--   ADD FULLTEXT INDEX idx_mv_fts_content (content)
--   WITH PARSER MULTILINGUAL
--   ADD_COLUMNAR_REPLICA_ON_DEMAND;
-- ALTER TABLE memory_values ADD VECTOR INDEX idx_mv_vec_cosine ((VEC_COSINE_DISTANCE(embedding)));

CREATE TABLE IF NOT EXISTS memory_keys (
  id                  VARCHAR(36)     PRIMARY KEY,
  memory_value_id     VARCHAR(36)     NOT NULL,
  key_text            VARCHAR(512)    NOT NULL    COMMENT 'Original surface of the key',
  key_norm            VARCHAR(512)    NOT NULL    COMMENT 'NormalizeKey(key_text); fast-path equality target',
  key_embedding       VECTOR(1536)    NULL        COMMENT 'NULL until K-VEC ablation backfill',
  source              VARCHAR(20)     NOT NULL DEFAULT 'extract'
                      COMMENT 'extract|extract_translation|user|feedback',
  weight              FLOAT           NOT NULL DEFAULT 1.0
                      COMMENT 'Reserved for future per-K ranking; V1 RRF treats all K hits as weight=1.0',
  created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT fk_memory_keys_value
    FOREIGN KEY (memory_value_id) REFERENCES memory_values(id) ON DELETE CASCADE,

  -- Same V can't carry duplicate aliases (normalized form).
  UNIQUE KEY uq_value_key_norm    (memory_value_id, key_norm),
  INDEX idx_mk_value              (memory_value_id),
  INDEX idx_mk_source             (source)
);

-- FULLTEXT and VECTOR indexes for memory_keys:
-- ALTER TABLE memory_keys
--   ADD FULLTEXT INDEX idx_mk_fts_key_text (key_text)
--   WITH PARSER MULTILINGUAL
--   ADD_COLUMNAR_REPLICA_ON_DEMAND;
-- ALTER TABLE memory_keys ADD VECTOR INDEX idx_mk_vec_cosine ((VEC_COSINE_DISTANCE(key_embedding)));


-- Marketing attribution captured at provision time (control plane).
CREATE TABLE IF NOT EXISTS tenant_utm (
  tenant_id  VARCHAR(36)   NOT NULL PRIMARY KEY,
  source     VARCHAR(255)  NULL,
  medium     VARCHAR(255)  NULL,
  campaign   VARCHAR(255)  NULL,
  content    VARCHAR(255)  NULL,
  created_at TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_tenant_utm FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

-- Upload task tracking (control plane).
CREATE TABLE IF NOT EXISTS upload_tasks (
  task_id       VARCHAR(36)   PRIMARY KEY,
  tenant_id     VARCHAR(36)   NOT NULL,
  file_name     VARCHAR(255)  NOT NULL,
  file_path     TEXT          NOT NULL,
  agent_id      VARCHAR(100)  NULL,
  session_id    VARCHAR(100)  NULL,
  file_type     VARCHAR(20)   NOT NULL COMMENT 'session|memory',
  total_chunks  INT           NOT NULL DEFAULT 0,
  done_chunks   INT           NOT NULL DEFAULT 0,
  status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                COMMENT 'pending|processing|done|failed',
  error_msg     TEXT          NULL,
  created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_upload_tenant (tenant_id),
  INDEX idx_upload_poll (status, created_at)
);

CREATE TABLE IF NOT EXISTS runtime_usage_outbox (
  operation_id      VARCHAR(36) PRIMARY KEY,
  tenant_id         VARCHAR(36) NOT NULL,
  cluster_id        VARCHAR(255) NULL,
  subject_version   VARCHAR(32) NOT NULL DEFAULT 'tenant_id_v1',
  step              VARCHAR(32) NOT NULL,
  phase             VARCHAR(32) NOT NULL,
  payload_json      JSON        NOT NULL,
  payload_hash      VARCHAR(64) NOT NULL,
  expires_at        TIMESTAMP   NULL,
  status            VARCHAR(20) NOT NULL DEFAULT 'pending',
  attempt_count     INT         NOT NULL DEFAULT 0,
  next_attempt_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_error        TEXT        NULL,
  created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
  updated_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_runtime_usage_outbox_poll (status, next_attempt_at)
);
