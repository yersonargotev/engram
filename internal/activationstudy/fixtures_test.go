package activationstudy

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareFixturesAblatesOnlyMemoryGuidanceAndIsolatesNeutral(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	writeTestFile(t, source, "AGENTS.md", "# Skills\n| Skill | Trigger |\n| `engram-memory-protocol` | decisions |\n| `engram-memory-cli` | recall |\n| `testing` | code |\n")
	writeTestFile(t, source, "README.md", "# Fixture project\n")
	writeTestFile(t, source, "internal/example.go", "package internal\n")
	revision := initTestGitRepo(t, source)

	skillDir := filepath.Join(t.TempDir(), "engram-memory-cli")
	writeTestFile(t, skillDir, "SKILL.md", "---\nname: engram-memory-cli\ndescription: test\n---\nUse the CLI.\n")
	writeTestFile(t, skillDir, "references/curation.md", "# Curation\n")
	skillHash, err := HashTree(skillDir)
	if err != nil {
		t.Fatal(err)
	}

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	study.Contract.SourceRevision = revision
	study.Contract.Engram.SourceRevision = revision
	study.Contract.UserSkill.TreeSHA256 = skillHash

	fixtures, err := study.PrepareFixtures(context.Background(), FixtureOptions{
		SourceRepo: source,
		Root:       t.TempDir(),
		UserSkill:  skillDir,
	})
	if err != nil {
		t.Fatalf("PrepareFixtures() error = %v", err)
	}

	normal := readTestFile(t, fixtures.Path("engram-normal"), "AGENTS.md")
	ablated := readTestFile(t, fixtures.Path("engram-ablated"), "AGENTS.md")
	if !strings.Contains(normal, "engram-memory-protocol") || !strings.Contains(normal, "engram-memory-cli") {
		t.Fatalf("normal fixture lost memory guidance:\n%s", normal)
	}
	if strings.Contains(ablated, "engram-memory-protocol") || strings.Contains(ablated, "engram-memory-cli") {
		t.Fatalf("ablated fixture retained memory guidance:\n%s", ablated)
	}
	if !strings.Contains(ablated, "| `testing` | code |") {
		t.Fatalf("ablated fixture changed unrelated guidance:\n%s", ablated)
	}
	if got := readTestFile(t, fixtures.Path("engram-ablated"), "README.md"); got != "# Fixture project\n" {
		t.Fatalf("ablated README = %q", got)
	}

	neutral := fixtures.Path("neutral")
	for _, forbidden := range []string{"AGENTS.md", "CLAUDE.md", ".codex/config.toml", ".engram/config.json"} {
		if _, err := os.Stat(filepath.Join(neutral, filepath.FromSlash(forbidden))); !os.IsNotExist(err) {
			t.Fatalf("neutral fixture inherited %s", forbidden)
		}
	}
	for _, fixture := range fixtures.Report.Fixtures {
		if fixture.ManifestSHA256 == "" || strings.Contains(fixture.ManifestSHA256, string(filepath.Separator)) {
			t.Fatalf("unsafe fixture identity: %#v", fixture)
		}
		if fixture.PluginEnabled || fixture.MCPEnabled || fixture.PromptHooksEnabled || fixture.StopVerifierEnabled {
			t.Fatalf("fixture %s retained an integration: %#v", fixture.ID, fixture)
		}
	}
}

func TestPrepareFixturesRejectsUserSkillDrift(t *testing.T) {
	t.Parallel()

	source := t.TempDir()
	writeTestFile(t, source, "AGENTS.md", "# Skills\n| `engram-memory-protocol` | decisions |\n| `engram-memory-cli` | recall |\n")
	revision := initTestGitRepo(t, source)
	skillDir := t.TempDir()
	writeTestFile(t, skillDir, "SKILL.md", "changed\n")

	contractPath, hashPath := writeFrozenContract(t, validContractJSON())
	study, err := Load(contractPath, hashPath)
	if err != nil {
		t.Fatal(err)
	}
	study.Contract.SourceRevision = revision
	study.Contract.Engram.SourceRevision = revision

	_, err = study.PrepareFixtures(context.Background(), FixtureOptions{
		SourceRepo: source,
		Root:       t.TempDir(),
		UserSkill:  skillDir,
	})
	if err == nil || !strings.Contains(err.Error(), "user skill hash mismatch") {
		t.Fatalf("PrepareFixtures() error = %v, want user skill hash mismatch", err)
	}
}

func TestHashTreeFollowsRootSymlinkWithoutHashingItsPrivateTarget(t *testing.T) {
	t.Parallel()

	target := filepath.Join(t.TempDir(), "private-target")
	writeTestFile(t, target, "SKILL.md", "stable\n")
	writeTestFile(t, target, "references/rules.md", "rules\n")
	link := filepath.Join(t.TempDir(), "engram-memory-cli")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	targetHash, err := HashTree(target)
	if err != nil {
		t.Fatal(err)
	}
	linkHash, err := HashTree(link)
	if err != nil {
		t.Fatal(err)
	}
	if linkHash != targetHash {
		t.Fatalf("symlink hash = %s, target hash = %s", linkHash, targetHash)
	}
}

func writeTestFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, root, relative string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func initTestGitRepo(t *testing.T, root string) string {
	t.Helper()
	commands := [][]string{
		{"git", "init", "-b", "main"},
		{"git", "config", "user.name", "Activation Study"},
		{"git", "config", "user.email", "activation@example.invalid"},
		{"git", "add", "."},
		{"git", "commit", "-m", "test: add fixture"},
	}
	for _, command := range commands {
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Dir = root
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", strings.Join(command, " "), err, output)
		}
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}
