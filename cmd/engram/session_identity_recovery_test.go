package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestDoctorExplicitSessionIdentityRecoveryCLI(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	initDoctorStore(t, cfg)
	db, err := sql.Open("sqlite", filepath.Join(cfg.DataDir, "engram.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO sessions(id,project,directory) VALUES ('','fixture','/historical'); INSERT INTO observations(session_id,type,title,content,project,sync_id) VALUES ('','manual','Preserved','original memory','fixture','obs-original')`)
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"engram", "doctor", "repair", "--project", "fixture", "--check", "invalid_session_identity", "--source-id", "", "--canonical-id", "canonical", "--json"}
	withArgs(t, append(append([]string{}, base...), "--plan")...)
	stdout, stderr, initialFailure := captureOutputAndRecover(t, func() { cmdDoctor(cfg) })
	var plan store.SessionIdentityRecoveryPlan
	if err := json.Unmarshal([]byte(stdout), &plan); err != nil || initialFailure != nil || stderr != "" || plan.Status != "planned" || plan.PreviewToken == "" {
		t.Fatalf("stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
	withArgs(t, append(append([]string{}, base...), "--apply", "--preview-token", plan.PreviewToken)...)
	var stopped any
	_, stderr = captureOutput(t, func() {
		defer func() { stopped = recover() }()
		cmdDoctor(cfg)
	})
	if stopped == nil || !strings.Contains(stderr, "confirmation_required") {
		t.Fatalf("confirmation=%s panic=%v", stderr, stopped)
	}
	withArgs(t, append(append([]string{}, base...), "--apply", "--preview-token", plan.PreviewToken, "--yes")...)
	stdout, stderr = captureOutput(t, func() { cmdDoctor(cfg) })
	var applied store.SessionIdentityRecoveryPlan
	if err := json.Unmarshal([]byte(stdout), &applied); err != nil || stderr != "" || applied.Status != "applied" {
		t.Fatalf("stdout=%s stderr=%s err=%v", stdout, stderr, err)
	}
	if _, err := os.Stat(applied.BackupPath); err != nil {
		t.Fatal(err)
	}
	withArgs(t, append(append([]string{}, base...), "--apply", "--preview-token", plan.PreviewToken, "--yes")...)
	stdout, stderr = captureOutput(t, func() { cmdDoctor(cfg) })
	applied = store.SessionIdentityRecoveryPlan{}
	if err := json.Unmarshal([]byte(stdout), &applied); err != nil || stderr != "" || applied.Status != "already_recovered" || applied.BackupPath != "" {
		t.Fatalf("replay=%s stderr=%s err=%v", stdout, stderr, err)
	}
}

func TestDoctorIdentityRecoveryErrorsAreStructuredWithoutCreatingData(t *testing.T) {
	stubExitWithPanic(t)
	for _, args := range [][]string{
		{"--source-id", "", "--canonical-id", "canonical", "--plan"},
		{"--project", "fixture", "--source-id", "", "--canonical-id", "canonical", "--apply"},
		{"--project", "fixture", "--source-id", "", "--canonical-id", "canonical", "--plan", "--unknown"},
	} {
		cfg := testConfig(t)
		command := append([]string{"engram", "doctor", "repair", "--check", "invalid_session_identity", "--json"}, args...)
		withArgs(t, command...)
		stdout, stderr, stopped := captureOutputAndRecover(t, func() { cmdDoctor(cfg) })
		var rejected cliErrorEnvelope
		if json.Unmarshal([]byte(stderr), &rejected) != nil || rejected.Code == "" || stdout != "" || stopped == nil {
			t.Fatalf("args=%v stdout=%s stderr=%s panic=%v", args, stdout, stderr, stopped)
		}
		if _, err := os.Stat(filepath.Join(cfg.DataDir, "engram.db")); !os.IsNotExist(err) {
			t.Fatalf("invalid invocation created data: %v", err)
		}
	}
}
