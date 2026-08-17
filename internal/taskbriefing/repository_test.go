package taskbriefing

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yersonargotev/engram/internal/store"
)

func TestGenerateUsesCleanBranchEvidenceAndResolvesBaseInOrder(t *testing.T) {
	tests := []struct {
		name         string
		explicitBase string
		configure    func(t *testing.T, repo string)
		wantRef      string
		wantSource   BaseSource
	}{
		{
			name:         "explicit base",
			explicitBase: "main",
			configure: func(t *testing.T, repo string) {
				runGit(t, repo, "branch", "--set-upstream-to=origin/main")
			},
			wantRef:    "main",
			wantSource: BaseSourceExplicit,
		},
		{
			name: "configured upstream",
			configure: func(t *testing.T, repo string) {
				runGit(t, repo, "update-ref", "refs/remotes/origin/integration", "main")
				runGit(t, repo, "branch", "--set-upstream-to=origin/integration")
			},
			wantRef:    "origin/integration",
			wantSource: BaseSourceUpstream,
		},
		{
			name: "same branch upstream falls back to remote default",
			configure: func(t *testing.T, repo string) {
				runGit(t, repo, "update-ref", "refs/remotes/origin/feat/clean-branch-evidence", "HEAD")
				runGit(t, repo, "branch", "--set-upstream-to=origin/feat/clean-branch-evidence")
			},
			wantRef:    "origin/main",
			wantSource: BaseSourceRemoteDefault,
		},
		{
			name: "upstream suffix is a distinct branch",
			configure: func(t *testing.T, repo string) {
				runGit(t, repo, "update-ref", "refs/remotes/origin/integration/feat/clean-branch-evidence", "main")
				runGit(t, repo, "branch", "--set-upstream-to=origin/integration/feat/clean-branch-evidence")
			},
			wantRef:    "origin/integration/feat/clean-branch-evidence",
			wantSource: BaseSourceUpstream,
		},
		{
			name:       "remote default branch",
			wantRef:    "origin/main",
			wantSource: BaseSourceRemoteDefault,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newCleanFeatureRepository(t, "engram")
			if tt.configure != nil {
				tt.configure(t, repo)
			}
			memoryStore := newTestStore(t)
			seedBriefingMemory(t, memoryStore, "branch-evidence", "engram", "Clean branch evidence", "Use committed branch evidence from repository paths.")

			result, err := New(memoryStore).Generate(Input{
				Project:          "engram",
				WorkingDirectory: repo,
				ExplicitBase:     tt.explicitBase,
			})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if result.BaseResolution == nil || result.BaseResolution.Ref != tt.wantRef || result.BaseResolution.Source != tt.wantSource {
				t.Fatalf("base resolution = %#v, want %s from %s", result.BaseResolution, tt.wantRef, tt.wantSource)
			}
			if len(result.Memories) != 1 {
				t.Fatalf("memories = %#v, want clean-branch memory", result.Memories)
			}
			var signals []SignalType
			for _, evidence := range result.Memories[0].Evidence {
				signals = append(signals, evidence.Signal)
			}
			wantSignals := []SignalType{SignalBranch, SignalBranchDiff, SignalAffectedPath, SignalCommitSubject}
			if !reflect.DeepEqual(signals, wantSignals) {
				t.Fatalf("signals = %v, want %v", signals, wantSignals)
			}
		})
	}
}

func TestGenerateDoesNotFallbackWhenExplicitBaseIsInvalid(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "task-invalid-base", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")

	result, err := New(memoryStore).Generate(Input{
		Project:          "engram",
		TaskIntent:       "implement deterministic briefing selection",
		WorkingDirectory: repo,
		ExplicitBase:     "missing-base",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.BaseResolution != nil || !hasDiagnostic(result.Diagnostics, DiagnosticBranchBaseUnresolved) {
		t.Fatalf("result = %#v, want unresolved explicit base without remote fallback", result)
	}
}

func TestGenerateDegradesWhenBranchBaseCannotResolve(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	runGit(t, repo, "symbolic-ref", "-d", "refs/remotes/origin/HEAD")
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "task-only", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")

	result, err := New(memoryStore).Generate(Input{
		Project:          "engram",
		TaskIntent:       "implement deterministic briefing selection",
		WorkingDirectory: repo,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.BaseResolution != nil {
		t.Fatalf("base resolution = %#v, want unresolved", result.BaseResolution)
	}
	if !hasDiagnostic(result.Diagnostics, DiagnosticBranchBaseUnresolved) {
		t.Fatalf("diagnostics = %#v, want %s", result.Diagnostics, DiagnosticBranchBaseUnresolved)
	}
	if len(result.Memories) != 1 || len(result.Memories[0].Evidence) != 1 || result.Memories[0].Evidence[0].Signal != SignalTaskIntent {
		t.Fatalf("result = %#v, want task-only continuation", result)
	}
}

func TestGenerateDisablesRepositoryEvidenceForAnotherSelectedProject(t *testing.T) {
	repo := newCleanFeatureRepository(t, "other-project")
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "task", "engram", "Database migration strategy", "Use expand-contract for the database migration strategy.")
	seedBriefingMemory(t, memoryStore, "repository", "engram", "Clean branch evidence", "Use committed branch evidence from repository paths.")

	result, err := New(memoryStore).Generate(Input{
		Project:          "engram",
		TaskIntent:       "database migration strategy",
		WorkingDirectory: repo,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !hasDiagnostic(result.Diagnostics, DiagnosticRepositoryProjectMismatch) {
		t.Fatalf("diagnostics = %#v, want %s", result.Diagnostics, DiagnosticRepositoryProjectMismatch)
	}
	if result.BaseResolution != nil {
		t.Fatalf("base resolution = %#v, want repository metadata disabled", result.BaseResolution)
	}
	if len(result.Memories) != 1 || result.Memories[0].Memory.Title != "Database migration strategy" {
		t.Fatalf("memories = %#v, want task-only selection", result.Memories)
	}
}

func TestGenerateContinuesWhenRepositoryCommandsDegrade(t *testing.T) {
	tests := []struct {
		name       string
		wantSource SignalType
		stub       func(t *testing.T)
	}{
		{
			name:       "committed diff",
			wantSource: SignalBranchDiff,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool {
					return len(args) > 1 && args[0] == "diff" && args[1] == "--no-ext-diff"
				})
			},
		},
		{
			name:       "affected paths",
			wantSource: SignalAffectedPath,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool {
					return len(args) > 1 && args[0] == "diff" && args[1] == "--name-only"
				})
			},
		},
		{
			name:       "commit subjects",
			wantSource: SignalCommitSubject,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool { return len(args) > 0 && args[0] == "log" })
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newCleanFeatureRepository(t, "engram")
			memoryStore := newTestStore(t)
			seedBriefingMemory(t, memoryStore, "command-degradation", "engram", "Clean branch evidence", "Use committed branch evidence from repository paths.")
			tt.stub(t)

			result, err := New(memoryStore).Generate(Input{Project: "engram", WorkingDirectory: repo, ExplicitBase: "main"})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if len(result.Memories) != 1 {
				t.Fatalf("memories = %#v, want remaining repository signals to continue", result.Memories)
			}
			if sources := diagnosticSources(result.Diagnostics, DiagnosticGitOperationFailed); !reflect.DeepEqual(sources, []SignalType{tt.wantSource}) {
				t.Fatalf("failure sources = %v, want %v", sources, []SignalType{tt.wantSource})
			}
		})
	}
}

func TestGenerateReportsRepositoryBoundaryDegradations(t *testing.T) {
	t.Run("non repository", func(t *testing.T) {
		memoryStore := newTestStore(t)
		seedBriefingMemory(t, memoryStore, "non-repository", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")
		result, err := New(memoryStore).Generate(Input{
			Project:          "engram",
			TaskIntent:       "implement deterministic briefing selection",
			WorkingDirectory: t.TempDir(),
		})
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if !hasDiagnostic(result.Diagnostics, DiagnosticRepositoryProjectUnresolved) || len(result.Memories) != 1 {
			t.Fatalf("result = %#v, want task-only continuation with unresolved repository", result)
		}
	})

	t.Run("detached head", func(t *testing.T) {
		repo := newCleanFeatureRepository(t, "engram")
		runGit(t, repo, "checkout", "--detach")
		memoryStore := newTestStore(t)
		seedBriefingMemory(t, memoryStore, "detached-head", "engram", "Clean branch evidence", "Use committed branch evidence from repository paths.")
		result, err := New(memoryStore).Generate(Input{Project: "engram", WorkingDirectory: repo})
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if sources := diagnosticSources(result.Diagnostics, DiagnosticGitOperationFailed); !reflect.DeepEqual(sources, []SignalType{SignalBranch}) {
			t.Fatalf("failure sources = %v, want branch", sources)
		}
		if result.BaseResolution == nil || result.BaseResolution.Source != BaseSourceRemoteDefault || len(result.Memories) != 1 {
			t.Fatalf("result = %#v, want committed evidence without branch identity", result)
		}
	})

	t.Run("merge base", func(t *testing.T) {
		repo := newCleanFeatureRepository(t, "engram")
		memoryStore := newTestStore(t)
		seedBriefingMemory(t, memoryStore, "merge-base", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")
		stubGitCommandFailure(t, func(args []string) bool { return len(args) > 0 && args[0] == "merge-base" })
		result, err := New(memoryStore).Generate(Input{
			Project: "engram", TaskIntent: "implement deterministic briefing selection",
			WorkingDirectory: repo, ExplicitBase: "main",
		})
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if !hasDiagnostic(result.Diagnostics, DiagnosticBranchBaseUnresolved) || result.BaseResolution != nil || len(result.Memories) != 1 {
			t.Fatalf("result = %#v, want unresolved base with task continuation", result)
		}
	})

	t.Run("remote enumeration", func(t *testing.T) {
		repo := newCleanFeatureRepository(t, "engram")
		memoryStore := newTestStore(t)
		seedBriefingMemory(t, memoryStore, "remote-list", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")
		stubGitCommandFailure(t, func(args []string) bool { return len(args) == 1 && args[0] == "remote" })
		result, err := New(memoryStore).Generate(Input{
			Project: "engram", TaskIntent: "implement deterministic briefing selection", WorkingDirectory: repo,
		})
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if !hasDiagnostic(result.Diagnostics, DiagnosticBranchBaseUnresolved) || len(result.Memories) != 1 {
			t.Fatalf("result = %#v, want unresolved base with task continuation", result)
		}
	})
}

func TestGenerateBoundsAcquiredCommittedDiffTerms(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	large := "package repository\n\n// "
	for index := 0; index < 200; index++ {
		large += "uniqueterm" + string(rune('a'+index%26)) + " "
	}
	writeTestFile(t, filepath.Join(repo, "internal", "repository", "branch_evidence.go"), large)
	runGit(t, repo, "add", "internal/repository/branch_evidence.go")
	runGit(t, repo, "commit", "-m", "Expand bounded branch evidence")
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "bounded-acquisition", "engram", "Deterministic briefing", "Implement deterministic briefing selection.")

	result, err := New(memoryStore).Generate(Input{
		Project: "engram", TaskIntent: "implement deterministic briefing selection",
		WorkingDirectory: repo, ExplicitBase: "main",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	truncations := diagnosticTruncations(result.Diagnostics, DiagnosticRepositoryInputTruncated)
	if len(truncations) == 0 || truncations[0].Signal != SignalBranchDiff || truncations[0].OmittedTerms == 0 {
		t.Fatalf("truncations = %#v, want bounded committed diff evidence", truncations)
	}
}

func TestRunGitTermsCommandReportsProcessFailures(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")

	t.Run("start", func(t *testing.T) {
		t.Setenv("PATH", "")
		if _, _, err := runGitTermsCommand(repo, 4, "status"); err == nil {
			t.Fatal("runGitTermsCommand succeeded without a Git executable")
		}
	})

	t.Run("wait", func(t *testing.T) {
		if _, _, err := runGitTermsCommand(repo, 4, "not-a-git-subcommand"); err == nil {
			t.Fatal("runGitTermsCommand succeeded for a failing Git command")
		}
	})

	t.Run("scanner", func(t *testing.T) {
		longToken := strings.Repeat("a", 70*1024)
		writeTestFile(t, filepath.Join(repo, "oversized.txt"), longToken)
		runGit(t, repo, "add", "oversized.txt")
		runGit(t, repo, "commit", "-m", "Add oversized token")
		if _, _, err := runGitTermsCommand(repo, 4, "show", "--format=", "HEAD", "--", "oversized.txt"); err == nil {
			t.Fatal("runGitTermsCommand succeeded for an oversized token")
		}
	})
}

func newCleanFeatureRepository(t *testing.T, remoteProject string) string {
	t.Helper()
	repo := t.TempDir()
	runGit(t, repo, "init", "-b", "main")
	runGit(t, repo, "config", "user.name", "Engram Test")
	runGit(t, repo, "config", "user.email", "engram@example.test")
	writeTestFile(t, filepath.Join(repo, "README.md"), "base\n")
	runGit(t, repo, "add", "README.md")
	runGit(t, repo, "commit", "-m", "Initial repository")
	runGit(t, repo, "remote", "add", "origin", "https://example.test/acme/"+remoteProject+".git")
	runGit(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	runGit(t, repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
	runGit(t, repo, "switch", "-c", "feat/clean-branch-evidence")
	writeTestFile(t, filepath.Join(repo, "internal", "repository", "branch_evidence.go"), "package repository\n\n// Clean branch evidence guides selection.\n")
	runGit(t, repo, "add", "internal/repository/branch_evidence.go")
	runGit(t, repo, "commit", "-m", "Add clean branch evidence")
	return repo
}

func seedBriefingMemory(t *testing.T, memoryStore *store.Store, sessionID, project, title, content string) {
	t.Helper()
	if err := memoryStore.CreateSession(sessionID, project, "/tmp/"+project); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := memoryStore.AddObservation(store.AddObservationParams{
		SessionID: sessionID,
		Type:      "decision",
		Title:     title,
		Content:   content,
		Project:   project,
		Scope:     "project",
	}); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func stubGitCommandFailure(t *testing.T, fail func(args []string) bool) {
	t.Helper()
	original := gitCommandOutput
	gitCommandOutput = func(directory string, args ...string) (string, error) {
		if fail(args) {
			return "", errors.New("git command unavailable")
		}
		return original(directory, args...)
	}
	t.Cleanup(func() { gitCommandOutput = original })
}

func stubGitTermsFailure(t *testing.T, fail func(args []string) bool) {
	t.Helper()
	original := gitTermsOutput
	gitTermsOutput = func(directory string, termLimit int, args ...string) (string, int, error) {
		if fail(args) {
			return "", 0, errors.New("git command unavailable")
		}
		return original(directory, termLimit, args...)
	}
	t.Cleanup(func() { gitTermsOutput = original })
}

func diagnosticSources(diagnostics []Diagnostic, code DiagnosticCode) []SignalType {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return diagnostic.Sources
		}
	}
	return nil
}

func diagnosticTruncations(diagnostics []Diagnostic, code DiagnosticCode) []InputTruncation {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return diagnostic.Truncations
		}
	}
	return nil
}
