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
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
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

CREATE INDEX idx_admission_shadow_runs_project_created
    ON admission_shadow_runs(project, created_at, id);
CREATE INDEX idx_admission_shadow_proposals_run_ordinal
    ON admission_shadow_proposals(run_id, ordinal);

INSERT INTO admission_shadow_runs (
    id, project, session_id, mode, evidence_version, generator_version,
    policy_version, included_items, included_content_bytes
) VALUES (
    'legacy-run', 'engram', NULL, 'session', 'evidence-v0', 'generator-v0',
    'policy-v0', 1, 32
);

INSERT INTO admission_shadow_proposals (
    id, run_id, ordinal, type, title, content, scope, category, protected,
    recommendation
) VALUES (
    'legacy-proposal', 'legacy-run', 0, 'decision', 'Legacy proposal',
    'Legacy proposal content', 'project', 'architecture', 0, 'review'
);
