CREATE TABLE admission_studies (
    study_id TEXT NOT NULL,
    study_version TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    metrics_version TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (study_id, study_version)
);

CREATE TABLE admission_shadow_runs (
    id TEXT PRIMARY KEY,
    project TEXT NOT NULL,
    session_id TEXT,
    mode TEXT NOT NULL,
    evidence_version TEXT NOT NULL,
    generator_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    diagnostic_codes TEXT NOT NULL DEFAULT '[]',
    included_items INTEGER NOT NULL DEFAULT 0 CHECK (included_items >= 0),
    included_content_bytes INTEGER NOT NULL DEFAULT 0 CHECK (included_content_bytes >= 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    study_id TEXT,
    study_version TEXT,
    study_contract_hash TEXT,
    cohort TEXT,
    cohort_kind TEXT,
    adapter TEXT,
    project_type TEXT,
    session_shape TEXT,
    consent_attestation TEXT,
    independent_review_required BOOLEAN NOT NULL DEFAULT 0
);

CREATE TABLE admission_shadow_proposals (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    type TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    scope TEXT NOT NULL,
    category TEXT NOT NULL,
    protected BOOLEAN NOT NULL DEFAULT 0,
    recommendation TEXT NOT NULL,
    proposal_reason_codes TEXT NOT NULL DEFAULT '[]',
    assessment_reason_codes TEXT NOT NULL DEFAULT '[]',
    evidence_refs TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES admission_shadow_runs(id) ON DELETE CASCADE,
    UNIQUE (run_id, ordinal)
);

CREATE TABLE admission_shadow_reviews (
    id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    verdict TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    unsupported BOOLEAN NOT NULL DEFAULT 0,
    privacy_leak BOOLEAN NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    reviewer_id TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (proposal_id) REFERENCES admission_shadow_proposals(id) ON DELETE CASCADE,
    UNIQUE (proposal_id, ordinal)
);

CREATE TABLE admission_study_omissions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    reviewer_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    category TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    annotation TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (run_id) REFERENCES admission_shadow_runs(id) ON DELETE CASCADE,
    UNIQUE (run_id, reviewer_id, ordinal)
);

CREATE INDEX idx_admission_shadow_runs_project_created
    ON admission_shadow_runs(project, created_at, id);
CREATE INDEX idx_admission_shadow_proposals_run_ordinal
    ON admission_shadow_proposals(run_id, ordinal);
CREATE INDEX idx_admission_shadow_reviews_proposal_ordinal
    ON admission_shadow_reviews(proposal_id, ordinal);
CREATE INDEX idx_admission_shadow_runs_study_cohort
    ON admission_shadow_runs(study_id, study_version, cohort, created_at, id);
CREATE UNIQUE INDEX idx_admission_shadow_runs_study_session
    ON admission_shadow_runs(study_id, study_version, session_id)
    WHERE study_id IS NOT NULL AND session_id IS NOT NULL;
CREATE INDEX idx_admission_shadow_reviews_reviewer
    ON admission_shadow_reviews(proposal_id, reviewer_id, ordinal);
CREATE INDEX idx_admission_study_omissions_run_reviewer
    ON admission_study_omissions(run_id, reviewer_id, ordinal);

INSERT INTO admission_studies (
    study_id, study_version, contract_version, metrics_version,
    contract_hash, contract_json
) VALUES (
    'retired-study', 'v1', 'admission-study-v1', 'metrics-v1',
    'retired-contract-hash', '{}'
);

INSERT INTO admission_shadow_runs (
    id, project, session_id, mode, evidence_version, generator_version,
    policy_version, study_id, study_version, study_contract_hash, cohort,
    cohort_kind, adapter, project_type, session_shape, consent_attestation,
    independent_review_required, included_items, included_content_bytes
) VALUES (
    'retired-run', 'engram', 'retired-session', 'session', 'evidence-v1',
    'generator-v1', 'policy-v1', 'retired-study', 'v1',
    'retired-contract-hash', 'calibration', 'calibration', 'codex', 'go',
    'long', 'consent-v1', 1, 1, 48
);

INSERT INTO admission_shadow_proposals (
    id, run_id, ordinal, type, title, content, scope, category, protected,
    recommendation, proposal_reason_codes, assessment_reason_codes, evidence_refs
) VALUES (
    'retired-proposal', 'retired-run', 0, 'decision',
    'RETIRED_ADMISSION_CANARY_TITLE', 'RETIRED_ADMISSION_CANARY_CONTENT',
    'project', 'architecture', 1, 'review', '["bounded"]', '["insufficient"]',
    '["observation:obs-retired"]'
);

INSERT INTO admission_shadow_reviews (
    id, proposal_id, reviewer_id, ordinal, verdict, note, unsupported, privacy_leak
) VALUES (
    'retired-review', 'retired-proposal', 'reviewer-a', 0, 'reject',
    'retired experiment evidence', 0, 0
);

INSERT INTO admission_study_omissions (
    id, run_id, reviewer_id, ordinal, category, reason_code, annotation
) VALUES (
    'retired-omission', 'retired-run', 'reviewer-a', 0, 'decision',
    'missing-decision', 'retired experiment omission'
);
