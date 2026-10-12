package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func seedIdentityRecovery(t *testing.T, source string) *Store {
	t.Helper()
	cfg := mustDefaultConfig(t)
	cfg.DataDir = t.TempDir()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, err = s.DB().Exec(`INSERT INTO sessions(id,project,directory) VALUES (?, 'fixture', '/historical');
	`, source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`INSERT INTO observations(session_id,type,title,content,project,sync_id) VALUES (?, 'manual', 'Preserved', 'original memory', 'fixture', 'obs-original')`, source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.DB().Exec(`INSERT INTO user_prompts(session_id,content,project,sync_id) VALUES (?, 'original archive', 'fixture', 'prompt-original')`, source)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSessionIdentityRecoveryPreservesOwnedDataAndAllowsReceiptReplay(t *testing.T) {
	s := seedIdentityRecovery(t, "")
	request := SessionIdentityRecoveryRequest{Project: "fixture", SourceID: "", CanonicalID: "canonical"}
	before, err := s.GetObservationBySyncID("obs-original")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != "planned" || plan.PreviewToken == "" || len(plan.ObservationIDs) != 1 || len(plan.ArchivedPromptIDs) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	if _, err := s.GetSession(""); err != nil {
		t.Fatalf("preview changed source: %v", err)
	}
	result, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "applied" {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(result.BackupPath); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetObservationBySyncID("obs-original")
	if err != nil {
		t.Fatal(err)
	}
	before.SessionID = "canonical"
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("memory changed beyond identity: before=%+v after=%+v", before, after)
	}
	afterArchive, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	archive[0].SessionID = "canonical"
	if !reflect.DeepEqual(archive, afterArchive) {
		t.Fatal("archive changed beyond linkage")
	}
	if _, err := s.GetSession(""); err == nil {
		t.Fatal("blank source survived")
	}
	if _, err := s.GetSession("canonical"); err != nil {
		t.Fatal(err)
	}
	replay, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken)
	if err != nil || replay.Status != "already_recovered" || replay.BackupPath != "" {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	enrolled, err := s.IsProjectEnrolled("fixture")
	if err != nil || enrolled {
		t.Fatalf("enrolled=%v err=%v", enrolled, err)
	}
}

func TestSessionIdentityRecoveryChangesOnlyExactPendingReferences(t *testing.T) {
	s := seedIdentityRecovery(t, "\t")
	request := SessionIdentityRecoveryRequest{Project: "fixture", SourceID: "\t", CanonicalID: " canonical "}
	payloads := []string{
		`{"id":"\t","project":"fixture","directory":"/historical","unknown":{"preserve":true}}`,
		`{"sync_id":"obs-original","session_id":"\t","project":"fixture","content":"original memory"}`,
	}
	for i, entity := range []string{SyncEntitySession, SyncEntityObservation} {
		key := "\t"
		if i == 1 {
			key = "obs-original"
		}
		_, err := s.DB().Exec(`INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project) VALUES ('cloud',?,?, 'upsert',?, 'fixture')`, entity, key, payloads[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	// Historical rows are deliberately retained as evidence, including prompt copies.
	for _, disposition := range []string{"quarantined", "abandoned"} {
		_, err := s.DB().Exec(`INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project,disposition) VALUES ('cloud','session',?,'upsert',?,'fixture',?)`, "\t", payloads[0], disposition)
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.PendingMutations) != 2 || len(plan.PreservedMutations) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken); err != nil {
		t.Fatal(err)
	}
	if err := s.EnrollProject("fixture"); err != nil {
		t.Fatal(err)
	}
	mutations, err := s.ListPendingSyncMutations("cloud", 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := range payloads {
		var original, after map[string]any
		if err := json.Unmarshal([]byte(payloads[i]), &original); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(mutations[i].Payload), &after); err != nil {
			t.Fatal(err)
		}
		field := "id"
		if i == 1 {
			field = "session_id"
		}
		original[field] = " canonical "
		if !reflect.DeepEqual(original, after) {
			t.Fatalf("payload changed beyond identity: %v", after)
		}
	}
	history, err := s.PreviewSessionIdentityRecovery(SessionIdentityRecoveryRequest{Project: "fixture", SourceID: "\t", CanonicalID: " canonical "})
	if err != nil || history.Status != "already_recovered" {
		t.Fatalf("history=%+v err=%v", history, err)
	}
}

func TestSessionIdentityRecoveryRejectsUnsafeArchiveObjects(t *testing.T) {
	s := seedIdentityRecovery(t, "")
	_, err := s.DB().Exec(`CREATE TRIGGER custom_archive_change AFTER UPDATE ON user_prompts BEGIN UPDATE user_prompts SET content='destroyed' WHERE id=new.id; END`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.PreviewSessionIdentityRecovery(SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"})
	var rejected *SessionIdentityRecoveryError
	if !errors.As(err, &rejected) || rejected.Code != "unsafe_schema_object" || !strings.Contains(rejected.Message, "custom_archive_change") {
		t.Fatalf("err=%v", err)
	}
	archive, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
	if err != nil || archive[0].Content != "original archive" || archive[0].SessionID != "" {
		t.Fatalf("archive=%+v err=%v", archive, err)
	}
}

func TestSessionIdentityRecoveryRollbackDoesNotPublishAReceipt(t *testing.T) {
	s := seedIdentityRecovery(t, "")
	request := SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	s.hooks.commit = func(*sql.Tx) error { return errors.New("database commit failed") }
	result, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken)
	s.hooks.commit = nil
	if err == nil || result.Status == "applied" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := s.GetSession(""); err != nil {
		t.Fatalf("source did not roll back: %v", err)
	}
	if _, err := s.GetSession("canonical"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("destination survived: %v", err)
	}
	unchanged, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil || unchanged.Status != "planned" || unchanged.PreviewToken != plan.PreviewToken {
		t.Fatalf("receipt/state survived rollback: %+v %v", unchanged, err)
	}
}

func TestSessionIdentityRecoveryRejectsChangedTargetsAndAmbiguity(t *testing.T) {
	for _, change := range []struct {
		name string
		sql  string
		args []any
		code string
	}{
		{"memory ownership", `UPDATE observations SET project='other'`, nil, "ownership_conflict"},
		{"destination collision", `INSERT INTO sessions(id,project,directory) VALUES ('canonical','other','/elsewhere')`, nil, "destination_collision"},
		{"changed source", `UPDATE sessions SET directory='/changed'`, nil, "stale_preview"},
		{"new dependent", `INSERT INTO observations(session_id,type,title,content,project,sync_id) VALUES ('','manual','New','new memory','fixture','obs-new')`, nil, "stale_preview"},
		{"unsafe object", `CREATE TRIGGER dangerous AFTER UPDATE ON sessions BEGIN DELETE FROM user_prompts; END`, nil, "unsafe_schema_object"},
	} {
		t.Run(change.name, func(t *testing.T) {
			s := seedIdentityRecovery(t, "")
			request := SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"}
			plan, err := s.PreviewSessionIdentityRecovery(request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.DB().Exec(change.sql, change.args...); err != nil {
				t.Fatal(err)
			}
			_, err = s.ApplySessionIdentityRecovery(request, plan.PreviewToken)
			var rejected *SessionIdentityRecoveryError
			if !errors.As(err, &rejected) || rejected.Code != change.code {
				t.Fatalf("err=%v want=%s", err, change.code)
			}
			if _, err := s.GetSession(""); err != nil {
				t.Fatalf("source mutated: %v", err)
			}
		})
	}
}

func TestSessionIdentityRecoveryPendingValidationAndStaleAcknowledgement(t *testing.T) {
	for _, scenario := range []struct{ name, entity, key, payload, project, change, code string }{
		{"malformed session", "session", "", "{", "fixture", "", "invalid_pending_payload"},
		{"malformed memory", "observation", "obs-original", "{", "fixture", "", "invalid_pending_payload"},
		{"nonstring project", "session", "", `{"id":"","project":7}`, "fixture", "", "invalid_pending_payload"},
		{"duplicate fields", "session", "", `{"id":"","project":"fixture","content":"first","content":"last"}`, "fixture", "", "invalid_pending_payload"},
		{"conflicting payload", "session", "", `{"id":"another","project":"fixture"}`, "fixture", "", "ambiguous_pending_reference"},
		{"foreign project", "session", "", `{"id":"","project":"other"}`, "other", "", "ownership_conflict"},
		{"acknowledged after preview", "session", "", `{"id":"","project":"fixture"}`, "fixture", `UPDATE sync_mutations SET acked_at='2026-01-01'`, "stale_preview"},
		{"quarantined after preview", "session", "", `{"id":"","project":"fixture"}`, "fixture", `UPDATE sync_mutations SET disposition='quarantined'`, "stale_preview"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s := seedIdentityRecovery(t, "")
			request := SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"}
			_, err := s.DB().Exec(`INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project) VALUES ('cloud',?,?,'upsert',?,?)`, scenario.entity, scenario.key, scenario.payload, scenario.project)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := s.PreviewSessionIdentityRecovery(request)
			if scenario.change != "" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.DB().Exec(scenario.change); err != nil {
					t.Fatal(err)
				}
				_, err = s.ApplySessionIdentityRecovery(request, plan.PreviewToken)
			}
			var rejected *SessionIdentityRecoveryError
			if !errors.As(err, &rejected) || rejected.Code != scenario.code {
				t.Fatalf("err=%v want=%s", err, scenario.code)
			}
		})
	}
}

func TestSessionIdentityRecoveryRequestAndReceiptGuards(t *testing.T) {
	s := seedIdentityRecovery(t, " ")
	for _, request := range []SessionIdentityRecoveryRequest{
		{SourceID: " ", CanonicalID: "canonical"},
		{Project: "fixture", SourceID: "nonblank", CanonicalID: "canonical"},
		{Project: "fixture", SourceID: " ", CanonicalID: "\t"},
		{Project: "other", SourceID: " ", CanonicalID: "canonical"},
		{Project: "fixture", SourceID: "", CanonicalID: "canonical"},
	} {
		if _, err := s.PreviewSessionIdentityRecovery(request); err == nil {
			t.Fatalf("accepted invalid request %+v", request)
		}
	}
	request := SessionIdentityRecoveryRequest{Project: "fixture", SourceID: " ", CanonicalID: "canonical"}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PreviewSessionIdentityRecovery(SessionIdentityRecoveryRequest{Project: "fixture", SourceID: " ", CanonicalID: "other"}); err == nil {
		t.Fatal("accepted conflicting replay")
	}
	if _, err := s.DB().Exec(`UPDATE sessions SET project='other' WHERE id='canonical'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken); err == nil {
		t.Fatal("accepted changed destination ownership")
	}
}

func TestSessionIdentityRecoveryPreservesIndependentProvenanceAndHistoricalPayloads(t *testing.T) {
	s := seedIdentityRecovery(t, "\t")
	_, err := s.DB().Exec(`
	INSERT INTO memory_relations(sync_id,source_id,target_id,session_id,reason) VALUES ('rel-independent','obs-original','obs-original',char(9),'original judgment');
	INSERT INTO prompt_tombstones(sync_id,session_id,project) VALUES ('historical-deletion',char(9),'fixture');
	INSERT INTO sessions(id,project,directory) VALUES ('unrelated','other','/other');
	INSERT INTO observations(session_id,type,title,content,project,sync_id,deleted_at) VALUES (char(9),'manual','Hidden','hidden memory','fixture','obs-hidden','2026-01-01');
	INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project,acked_at) VALUES ('cloud','session',char(9),'upsert',' { "id":"\t", "directory":"/historical" } ','fixture','2026-01-01');
	INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project,disposition) VALUES ('cloud','prompt','prompt-original','upsert','{"session_id":"\t","content":"frozen copy"}','fixture','abandoned');
	INSERT INTO sync_mutations(target_key,entity,entity_key,op,payload,project,disposition) VALUES ('cloud','session',char(9),'upsert','{"id":"\t"}','fixture','quarantined');
	UPDATE sync_state SET last_acked_seq=17,last_pulled_seq=23 WHERE target_key='cloud';`)
	if err != nil {
		t.Fatal(err)
	}
	relation, err := s.GetRelation("rel-independent")
	if err != nil {
		t.Fatal(err)
	}
	checkpointID := CheckpointIdentity{Host: "codex", SessionID: "valid-host-conversation", RootTurnID: "root"}
	checkpoint, _, err := s.RecordSkippedCheckpoint(RecordSkippedCheckpointParams{Identity: checkpointID, ReasonCode: CheckpointSkipReasonNoDurableKnowledge})
	if err != nil {
		t.Fatal(err)
	}
	syncState, err := s.GetSyncState("cloud")
	if err != nil {
		t.Fatal(err)
	}
	request := SessionIdentityRecoveryRequest{Project: "fixture", SourceID: "\t", CanonicalID: "canonical"}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ObservationIDs) != 2 || len(plan.PreservedMutations) != 3 {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken); err != nil {
		t.Fatal(err)
	}
	afterRelation, err := s.GetRelation("rel-independent")
	if err != nil || !reflect.DeepEqual(relation, afterRelation) {
		t.Fatalf("relation changed: %+v %v", afterRelation, err)
	}
	afterCheckpoint, err := s.GetMemoryCheckpoint(checkpointID)
	if err != nil || !reflect.DeepEqual(checkpoint, afterCheckpoint) {
		t.Fatalf("host checkpoint changed: %+v %v", afterCheckpoint, err)
	}
	afterSyncState, err := s.GetSyncState("cloud")
	if err != nil || !reflect.DeepEqual(syncState, afterSyncState) {
		t.Fatalf("sync state changed: %+v %v", afterSyncState, err)
	}
	if session, err := s.GetSession("unrelated"); err != nil || session.Project != "other" {
		t.Fatalf("unrelated project changed: %+v %v", session, err)
	}
	results, err := s.Search("hidden", SearchOptions{Project: "fixture"})
	if err != nil || len(results) != 0 {
		t.Fatalf("deleted Memory became searchable: %+v %v", results, err)
	}
	// Historical payload byte equality is an explicit persistence contract;
	// transport APIs intentionally exclude these rows, so inspect via Store.DB.
	rows, err := s.DB().Query(`SELECT payload FROM sync_mutations ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var preserved []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		preserved = append(preserved, payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{` { "id":"\t", "directory":"/historical" } `, `{"session_id":"\t","content":"frozen copy"}`, `{"id":"\t"}`}
	if !reflect.DeepEqual(preserved, want) {
		t.Fatalf("historical payloads changed: %q", preserved)
	}
}

func TestSessionIdentityRecoveryRejectsActiveSynchronization(t *testing.T) {
	s := seedIdentityRecovery(t, "")
	acquired, err := s.AcquireSyncLease("cloud", "active-sync", time.Minute, time.Now().UTC())
	if err != nil || !acquired {
		t.Fatalf("lease=%v err=%v", acquired, err)
	}
	_, err = s.PreviewSessionIdentityRecovery(SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"})
	var rejected *SessionIdentityRecoveryError
	if !errors.As(err, &rejected) || rejected.Code != "sync_in_progress" {
		t.Fatalf("err=%v", err)
	}
}

func TestSessionIdentityRecoveryRejectsCustomizedSchemaEffects(t *testing.T) {
	for _, custom := range []struct{ name, ddl, object string }{
		{"archive content cascade", `DROP TABLE user_prompts;
		CREATE TABLE user_prompts(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),content TEXT NOT NULL REFERENCES sessions(id) ON UPDATE CASCADE,project TEXT,sync_id TEXT,created_at TEXT NOT NULL DEFAULT(datetime('now')));
		INSERT INTO user_prompts(session_id,content,project,sync_id) VALUES ('','','fixture','prompt-original');`, "user_prompts"},
		{"downstream archive cascade", `CREATE UNIQUE INDEX custom_archive_session ON user_prompts(session_id);
		CREATE TABLE custom_history(content TEXT REFERENCES user_prompts(session_id) ON UPDATE CASCADE);
		INSERT INTO custom_history(content) VALUES ('');`, "custom_history"},
		{"downstream memory cascade", `CREATE UNIQUE INDEX custom_memory_session ON observations(session_id);
		CREATE TABLE custom_history(content TEXT REFERENCES observations(session_id) ON UPDATE CASCADE);
		INSERT INTO custom_history(content) VALUES ('');`, "custom_history"},
		{"generated archive content", `DROP TABLE user_prompts;
		CREATE TABLE user_prompts(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id),content TEXT GENERATED ALWAYS AS ('history:' || session_id) STORED,project TEXT,sync_id TEXT,created_at TEXT NOT NULL DEFAULT(datetime('now')));
		INSERT INTO user_prompts(session_id,project,sync_id) VALUES ('','fixture','prompt-original');`, "user_prompts"},
		{"replacing archive conflict", `PRAGMA foreign_keys=OFF;
		DROP TABLE user_prompts;
		CREATE TABLE user_prompts(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id) UNIQUE ON CONFLICT REPLACE,content TEXT NOT NULL,project TEXT,sync_id TEXT,created_at TEXT NOT NULL DEFAULT(datetime('now')));
		INSERT INTO user_prompts(session_id,content,project,sync_id) VALUES ('','source','fixture','prompt-original'),('canonical','unrelated','other','prompt-unrelated');
		PRAGMA foreign_keys=ON;`, "user_prompts"},
		{"ignoring archive conflict", `PRAGMA foreign_keys=OFF;
		DROP TABLE user_prompts;
		CREATE TABLE user_prompts(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL REFERENCES sessions(id) UNIQUE ON CONFLICT IGNORE,content TEXT NOT NULL,project TEXT,sync_id TEXT,created_at TEXT NOT NULL DEFAULT(datetime('now')));
		INSERT INTO user_prompts(session_id,content,project,sync_id) VALUES ('','source','fixture','prompt-original'),('canonical','unrelated','other','prompt-unrelated');
		PRAGMA foreign_keys=ON;`, "user_prompts"},
		{"numeric source identity", `PRAGMA foreign_keys=OFF;
		DROP TABLE sessions;
		CREATE TABLE sessions(id NUMERIC PRIMARY KEY,project TEXT NOT NULL,directory TEXT NOT NULL,started_at TEXT NOT NULL DEFAULT(datetime('now')),ended_at TEXT,summary TEXT);
		INSERT INTO sessions(id,project,directory) VALUES ('','fixture','/historical');
		PRAGMA foreign_keys=ON;`, "sessions"},
		{"misleading insert trigger body", `DROP TRIGGER obs_fts_insert;
		CREATE TRIGGER obs_fts_insert AFTER UPDATE ON observations BEGIN UPDATE user_prompts SET content='changed by customized trigger'; SELECT 'after','insert','on','observations'; END;`, "obs_fts_insert"},
		{"misleading delete trigger body", `DROP TRIGGER obs_fts_delete;
		CREATE TRIGGER obs_fts_delete AFTER UPDATE ON observations BEGIN UPDATE user_prompts SET content='changed by customized trigger'; SELECT 'after','delete','on','observations'; END;`, "obs_fts_delete"},
	} {
		t.Run(custom.name, func(t *testing.T) {
			s := seedIdentityRecovery(t, "")
			if _, err := s.DB().Exec(custom.ddl); err != nil {
				t.Fatal(err)
			}
			before, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.PreviewSessionIdentityRecovery(SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "canonical"})
			var rejected *SessionIdentityRecoveryError
			if !errors.As(err, &rejected) || rejected.Code != "unsafe_schema_object" || !strings.Contains(rejected.Message, custom.object) {
				t.Fatalf("err=%v", err)
			}
			after, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("archive changed: %+v %v", after, err)
			}
			if _, err := s.GetSession(""); err != nil {
				t.Fatalf("source changed: %v", err)
			}
		})
	}
}

func TestSessionIdentityRecoveryKeepsDistinctWhitespaceLinksUnderCustomCollation(t *testing.T) {
	s := seedIdentityRecovery(t, "")
	_, err := s.DB().Exec(`DROP TABLE user_prompts;
	CREATE TABLE user_prompts(id INTEGER PRIMARY KEY,session_id TEXT COLLATE RTRIM NOT NULL REFERENCES sessions(id),content TEXT NOT NULL,project TEXT,sync_id TEXT,created_at TEXT DEFAULT(datetime('now')));
	INSERT INTO sessions(id,project,directory) VALUES (' ','fixture','/space');
	INSERT INTO user_prompts(session_id,content,project,sync_id) VALUES ('','selected archive','fixture','selected'),(' ','independent archive','fixture','independent');`)
	if err != nil {
		t.Fatal(err)
	}
	request := SessionIdentityRecoveryRequest{Project: "fixture", CanonicalID: "007"}
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.ArchivedPromptIDs) != 1 {
		t.Fatalf("selected another identity: %+v", plan)
	}
	if _, err := s.ApplySessionIdentityRecovery(request, plan.PreviewToken); err != nil {
		t.Fatal(err)
	}
	archive, err := s.ExportLegacyPrompts(LegacyPromptScope{Project: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if archive[0].SessionID != "007" || archive[1].SessionID != " " || archive[1].Content != "independent archive" {
		t.Fatalf("collapsed opaque identities: %+v", archive)
	}
	if _, err := s.GetSession("007"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetSession(" "); err != nil {
		t.Fatal(err)
	}
}
