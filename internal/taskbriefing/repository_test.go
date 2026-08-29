package taskbriefing

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

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

func TestGenerateUsesDirtyWorkingTreeEvidenceWithoutBranchBase(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	runGit(t, repo, "symbolic-ref", "-d", "refs/remotes/origin/HEAD")
	writeTestFile(t, filepath.Join(repo, "staged", "schema_migration.go"), "package staged\n\n// schema migration contract\n")
	runGit(t, repo, "add", "staged/schema_migration.go")
	writeTestFile(t, filepath.Join(repo, "unstaged", "cache_invalidation.go"), "package unstaged\n")
	runGit(t, repo, "add", "unstaged/cache_invalidation.go")
	writeTestFile(t, filepath.Join(repo, "unstaged", "cache_invalidation.go"), "package unstaged\n\n// cache invalidation policy\n")
	writeTestFile(t, filepath.Join(repo, "untracked", "secret_evidence.go"), "untracked contents must not affect selection\n")

	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "dirty-working-tree", "engram", "Dirty working tree", "Schema migration contract and cache invalidation policy.")

	result, err := New(memoryStore).Generate(Input{Project: "engram", WorkingDirectory: repo})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !hasDiagnostic(result.Diagnostics, DiagnosticBranchBaseUnresolved) {
		t.Fatalf("diagnostics = %#v, want unresolved branch base", result.Diagnostics)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("memories = %#v, want dirty working tree evidence", result.Memories)
	}
	var signals []SignalType
	for _, evidence := range result.Memories[0].Evidence {
		signals = append(signals, evidence.Signal)
	}
	for _, signal := range []SignalType{SignalStagedDiff, SignalUnstagedDiff, SignalAffectedPath} {
		if !containsSignal(signals, signal) {
			t.Fatalf("signals = %v, want %s", signals, signal)
		}
	}
}

func TestGenerateUsesSingleDirtyWorktreeSource(t *testing.T) {
	tests := []struct {
		name       string
		branch     string
		want       SignalType
		notWant    SignalType
		prepare    func(t *testing.T, repo string)
		memoryText string
	}{
		{
			name: "staged only", branch: "feat/schema-migration", want: SignalStagedDiff, notWant: SignalUnstagedDiff,
			prepare: func(t *testing.T, repo string) {
				writeTestFile(t, filepath.Join(repo, "schema_migration.go"), "package migration\n\n// schema migration\n")
				runGit(t, repo, "add", "schema_migration.go")
			},
			memoryText: "Schema migration",
		},
		{
			name: "unstaged only", branch: "feat/cache-invalidation", want: SignalUnstagedDiff, notWant: SignalStagedDiff,
			prepare: func(t *testing.T, repo string) {
				writeTestFile(t, filepath.Join(repo, "README.md"), "base\ncache invalidation\n")
			},
			memoryText: "Cache invalidation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newCleanFeatureRepository(t, "engram")
			runGit(t, repo, "branch", "-m", tt.branch)
			tt.prepare(t, repo)
			memoryStore := newTestStore(t)
			seedBriefingMemory(t, memoryStore, "single-source", "engram", tt.memoryText, tt.memoryText+" guidance.")

			result, err := New(memoryStore).Generate(Input{Project: "engram", WorkingDirectory: repo, ExplicitBase: "main"})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if len(result.Memories) != 1 {
				t.Fatalf("memories = %#v, want single-source worktree selection", result.Memories)
			}
			var signals []SignalType
			for _, evidence := range result.Memories[0].Evidence {
				signals = append(signals, evidence.Signal)
			}
			if !containsSignal(signals, tt.want) || containsSignal(signals, tt.notWant) {
				t.Fatalf("signals = %v, want %s without %s", signals, tt.want, tt.notWant)
			}
		})
	}
}

func TestGenerateDeduplicatesIdenticalEvidenceBeforeRanking(t *testing.T) {
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "duplicate-evidence", "engram", "Token migration", "Token migration guidance.")
	input := Input{Project: "engram", RepositoryProject: "engram", Repository: RepositorySignals{
		StagedDiff: "token migration", UnstagedDiff: "token migration",
	}}

	result, err := New(memoryStore).Generate(input)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 0 {
		t.Fatalf("memories = %#v, duplicated evidence must not cross the relevance threshold", result.Memories)
	}
}

func TestGeneratePreservesSourcesForDeduplicatedEvidence(t *testing.T) {
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "duplicate-evidence", "engram", "Token migration", "Token migration guidance.")
	input := Input{
		Project: "engram", TaskIntent: "token migration",
		RepositoryProject: "engram",
		Repository:        RepositorySignals{StagedDiff: "token migration", UnstagedDiff: "token migration"},
	}

	first, err := New(memoryStore).Generate(input)
	if err != nil {
		t.Fatalf("Generate(first): %v", err)
	}
	second, err := New(memoryStore).Generate(input)
	if err != nil {
		t.Fatalf("Generate(second): %v", err)
	}
	if !reflect.DeepEqual(first, second) || len(first.Memories) != 1 {
		t.Fatalf("results are not deterministic or candidate was duplicated: first=%#v second=%#v", first, second)
	}
	var signals []SignalType
	for _, evidence := range first.Memories[0].Evidence {
		signals = append(signals, evidence.Signal)
	}
	if !reflect.DeepEqual(signals, []SignalType{SignalTaskIntent, SignalStagedDiff, SignalUnstagedDiff}) {
		t.Fatalf("signals = %v, want each source traceable exactly once", signals)
	}
	if first.Memories[0].Score != CalibratedDefaults.TaskWeight+CalibratedDefaults.TitleOrTopicBonus+2 {
		t.Fatalf("score = %d, duplicated payloads must contribute only the strongest grouped weight", first.Memories[0].Score)
	}
}

func TestGenerateUsesUntrackedPathsWithoutReadingTheirContents(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	writeTestFile(t, filepath.Join(repo, "untracked", "clean_branch_release_playbook.md"), "private launch password rotation")
	memoryStore := newTestStore(t)
	seedBriefingMemory(t, memoryStore, "untracked-path", "engram", "Clean branch release playbook", "Follow the clean branch release playbook.")
	seedBriefingMemory(t, memoryStore, "untracked-content", "engram", "Private launch password", "Rotate the private launch password.")

	result, err := New(memoryStore).Generate(Input{Project: "engram", WorkingDirectory: repo, ExplicitBase: "main"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(result.Memories) != 1 {
		t.Fatalf("result = %#v, want untracked path evidence", result)
	}
	found := make([]SignalType, 0, len(result.Memories[0].Evidence))
	for _, evidence := range result.Memories[0].Evidence {
		found = append(found, evidence.Signal)
	}
	if !containsSignal(found, SignalUntrackedPath) {
		t.Fatalf("signals = %v, want %s", found, SignalUntrackedPath)
	}
	if result.Memories[0].Memory.Title != "Clean branch release playbook" {
		t.Fatalf("selected %q, want path-derived memory without private content", result.Memories[0].Memory.Title)
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
					return len(args) > 1 && args[0] == "diff" && args[1] == "--no-ext-diff" && strings.Contains(strings.Join(args, " "), "..HEAD")
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
		{
			name:       "staged diff",
			wantSource: SignalStagedDiff,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool {
					return len(args) > 1 && args[0] == "diff" && args[1] == "--cached"
				})
			},
		},
		{
			name:       "unstaged diff",
			wantSource: SignalUnstagedDiff,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool {
					return len(args) > 1 && args[0] == "diff" && args[1] == "--no-ext-diff" && !strings.Contains(strings.Join(args, " "), "..HEAD")
				})
			},
		},
		{
			name:       "untracked paths",
			wantSource: SignalUntrackedPath,
			stub: func(t *testing.T) {
				stubGitTermsFailure(t, func(args []string) bool { return len(args) > 0 && args[0] == "ls-files" })
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
	if len(truncations) == 0 || truncations[0].Signal != SignalBranchDiff || truncations[0].OmittedTerms == 0 || truncations[0].TotalTerms != truncations[0].AnalyzedTerms+truncations[0].OmittedTerms {
		t.Fatalf("truncations = %#v, want bounded committed diff evidence", truncations)
	}
}

func TestRunGitTermsCommandReportsProcessFailures(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")

	t.Run("start", func(t *testing.T) {
		t.Setenv("PATH", "")
		if _, _, _, err := runGitTermsCommand(repo, 4, "status"); err == nil {
			t.Fatal("runGitTermsCommand succeeded without a Git executable")
		}
	})

	t.Run("wait", func(t *testing.T) {
		if _, _, _, err := runGitTermsCommand(repo, 4, "not-a-git-subcommand"); err == nil {
			t.Fatal("runGitTermsCommand succeeded for a failing Git command")
		}
	})

	t.Run("oversized token is a truncation", func(t *testing.T) {
		longToken := strings.Repeat("a", 70*1024)
		writeTestFile(t, filepath.Join(repo, "oversized.txt"), "retained "+longToken+" preserved")
		runGit(t, repo, "add", "oversized.txt")
		runGit(t, repo, "commit", "-m", "Add oversized token")
		terms, omitted, complete, err := runGitTermsCommand(repo, 4, "show", "HEAD:oversized.txt")
		if err != nil || !complete || terms != "retained preserved" || omitted != 1 {
			t.Fatalf("terms = %q, omitted = %d, err = %v; want usable terms plus typed truncation", terms, omitted, err)
		}
	})
}

func TestRunGitTermsCommandCountsOmittedOccurrencesWithBoundedVocabulary(t *testing.T) {
	repo := newCleanFeatureRepository(t, "engram")
	writeTestFile(t, filepath.Join(repo, "dedupe.txt"), "alpha beta gamma gamma gamma\n")
	runGit(t, repo, "add", "dedupe.txt")
	runGit(t, repo, "commit", "-m", "Add repeated terms")

	terms, omitted, complete, err := runGitTermsCommand(repo, 2, "show", "HEAD:dedupe.txt")
	if err != nil {
		t.Fatalf("runGitTermsCommand: %v", err)
	}
	if !complete || terms != "alpha beta" || omitted != 3 {
		t.Fatalf("terms = %q, omitted = %d, want two retained terms and three omitted occurrences", terms, omitted)
	}
}

func TestNormalizeTermsWithCountMatchesStreamingOmissionSemantics(t *testing.T) {
	terms, omitted := normalizeTermsWithCount("alpha beta gamma gamma delta alpha epsilon", 2)
	if !reflect.DeepEqual(terms, []string{"alpha", "beta"}) || omitted != 4 {
		t.Fatalf("terms = %v, omitted = %d, want retained vocabulary and bounded omitted occurrences", terms, omitted)
	}
}

func TestReadBoundedGitTermsStopsAtDeterministicByteLimit(t *testing.T) {
	input := strings.NewReader(strings.Repeat("alpha ", int(CalibratedDefaults.GitInputByteLimit)/6+10))
	terms, omitted, complete, err := readBoundedGitTerms(input, 2)
	if err != nil {
		t.Fatalf("readBoundedGitTerms: %v", err)
	}
	if complete || terms != "alpha" || omitted != 0 {
		t.Fatalf("terms = %q, omitted = %d, complete = %v; want bounded partial scan", terms, omitted, complete)
	}
}

func TestAcquireRepositoryTermsMarksIncompletePrefixCounts(t *testing.T) {
	original := gitTermsOutput
	gitTermsOutput = func(string, int, ...string) (string, int, bool, error) {
		return "alpha beta", 7, false, nil
	}
	t.Cleanup(func() { gitTermsOutput = original })

	repository := RepositorySignals{}
	acquireRepositoryTerms(&repository, "/unused", SignalBranchDiff, 2, func(value string) {
		repository.BranchDiff = value
	}, "diff")
	if repository.BranchDiff != "alpha beta" || len(repository.AcquisitionTruncations) != 1 {
		t.Fatalf("repository = %#v", repository)
	}
	truncation := repository.AcquisitionTruncations[0]
	if truncation.CountComplete || truncation.TotalTerms != 9 || truncation.AnalyzedTerms != 2 || truncation.OmittedTerms != 7 {
		t.Fatalf("truncation = %#v, want explicitly incomplete prefix counts", truncation)
	}
}

func TestReadBoundedGitTermsPropagatesReaderFailure(t *testing.T) {
	_, _, _, err := readBoundedGitTerms(iotest.TimeoutReader(strings.NewReader("alpha beta")), 4)
	if !errors.Is(err, iotest.ErrTimeout) {
		t.Fatalf("error = %v, want %v", err, iotest.ErrTimeout)
	}
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
	gitTermsOutput = func(directory string, termLimit int, args ...string) (string, int, bool, error) {
		if fail(args) {
			return "", 0, false, errors.New("git command unavailable")
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
