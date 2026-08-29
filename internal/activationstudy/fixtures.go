package activationstudy

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type FixtureOptions struct {
	SourceRepo string
	Root       string
	UserSkill  string
}

type Fixtures struct {
	Report VerificationReport `json:"verification"`
	paths  map[string]string
}

type VerificationReport struct {
	ContractSHA256           string            `json:"contract_sha256"`
	SourceRevision           string            `json:"source_revision"`
	UserSkill                SkillIdentity     `json:"user_skill"`
	Fixtures                 []FixtureIdentity `json:"fixtures"`
	Ablation                 AblationIdentity  `json:"ablation"`
	CodexSkillInventory      []string          `json:"codex_skill_inventory"`
	CodexPromptInputVerified bool              `json:"codex_prompt_input_verified"`
	CleanupVerified          bool              `json:"cleanup_verified"`
}

type SkillIdentity struct {
	Name       string `json:"name"`
	Revision   string `json:"revision"`
	TreeSHA256 string `json:"tree_sha256"`
}

type FixtureIdentity struct {
	ID                  string `json:"id"`
	ManifestSHA256      string `json:"manifest_sha256"`
	PluginEnabled       bool   `json:"plugin_enabled"`
	MCPEnabled          bool   `json:"mcp_enabled"`
	PromptHooksEnabled  bool   `json:"prompt_hooks_enabled"`
	StopVerifierEnabled bool   `json:"stop_verifier_enabled"`
}

type AblationIdentity struct {
	ChangedFiles        []string `json:"changed_files"`
	RemovedGuidanceRows int      `json:"removed_guidance_rows"`
}

func (fixtures *Fixtures) Path(treatment string) string {
	return fixtures.paths[treatment]
}

// PrepareFixtures materializes and verifies the three frozen treatments.
func (study *Study) PrepareFixtures(ctx context.Context, options FixtureOptions) (*Fixtures, error) {
	if err := study.Contract.validate(); err != nil {
		return nil, err
	}
	actualSkillHash, err := HashTree(options.UserSkill)
	if err != nil {
		return nil, fmt.Errorf("hash user skill: %w", err)
	}
	if actualSkillHash != study.Contract.UserSkill.TreeSHA256 {
		return nil, fmt.Errorf("user skill hash mismatch: got %s, want %s", actualSkillHash, study.Contract.UserSkill.TreeSHA256)
	}
	if strings.TrimSpace(options.SourceRepo) == "" || strings.TrimSpace(options.Root) == "" {
		return nil, fmt.Errorf("fixture source repository and root are required")
	}
	if err := verifyCommit(ctx, options.SourceRepo, study.Contract.SourceRevision); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.Root, 0o700); err != nil {
		return nil, fmt.Errorf("create fixture root: %w", err)
	}

	paths := map[string]string{
		"engram-normal":  filepath.Join(options.Root, "engram-normal"),
		"engram-ablated": filepath.Join(options.Root, "engram-ablated"),
		"neutral":        filepath.Join(options.Root, "neutral"),
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return nil, fmt.Errorf("fixture path already exists: %s", filepath.Base(path))
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return nil, fmt.Errorf("create fixture %s: %w", filepath.Base(path), err)
		}
	}
	for _, treatment := range []string{"engram-normal", "engram-ablated"} {
		if err := extractGitArchive(ctx, options.SourceRepo, study.Contract.SourceRevision, paths[treatment]); err != nil {
			return nil, fmt.Errorf("prepare %s fixture: %w", treatment, err)
		}
	}
	removedRows, err := ablateMemoryGuidance(filepath.Join(paths["engram-ablated"], "AGENTS.md"))
	if err != nil {
		return nil, err
	}
	if removedRows != 2 {
		return nil, fmt.Errorf("Engram ablation removed %d memory guidance rows, want 2", removedRows)
	}
	if err := writeNeutralFixture(paths["neutral"]); err != nil {
		return nil, err
	}

	normalFiles, normalHash, err := treeManifest(paths["engram-normal"])
	if err != nil {
		return nil, err
	}
	ablatedFiles, ablatedHash, err := treeManifest(paths["engram-ablated"])
	if err != nil {
		return nil, err
	}
	changed := changedManifestFiles(normalFiles, ablatedFiles)
	if len(changed) != 1 || changed[0] != "AGENTS.md" {
		return nil, fmt.Errorf("Engram ablation changed %v, want only AGENTS.md", changed)
	}
	_, neutralHash, err := treeManifest(paths["neutral"])
	if err != nil {
		return nil, err
	}
	for _, forbidden := range []string{"AGENTS.md", "CLAUDE.md", ".codex/config.toml", ".engram/config.json", ".mcp.json"} {
		if _, err := os.Lstat(filepath.Join(paths["neutral"], filepath.FromSlash(forbidden))); !os.IsNotExist(err) {
			return nil, fmt.Errorf("neutral fixture inherited forbidden surface %s", forbidden)
		}
	}

	for id, path := range paths {
		remote := "https://example.invalid/acme/catalog-lab.git"
		if id != "neutral" {
			remote = "https://github.com/yersonargotev/engram.git"
		}
		if err := initializeFixtureGit(ctx, path, remote); err != nil {
			return nil, fmt.Errorf("initialize %s fixture: %w", id, err)
		}
	}

	report := VerificationReport{
		ContractSHA256: study.Hash,
		SourceRevision: study.Contract.SourceRevision,
		UserSkill: SkillIdentity{
			Name: study.Contract.UserSkill.Name, Revision: study.Contract.UserSkill.Revision, TreeSHA256: actualSkillHash,
		},
		Fixtures: []FixtureIdentity{
			{ID: "engram-normal", ManifestSHA256: normalHash},
			{ID: "engram-ablated", ManifestSHA256: ablatedHash},
			{ID: "neutral", ManifestSHA256: neutralHash},
		},
		Ablation: AblationIdentity{ChangedFiles: changed, RemovedGuidanceRows: removedRows},
	}
	return &Fixtures{Report: report, paths: paths}, nil
}

// HashTree returns a path-independent digest of every regular file and symlink.
func HashTree(root string) (string, error) {
	_, digest, err := treeManifest(root)
	return digest, err
}

func treeManifest(root string) (map[string]string, string, error) {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, "", err
	}
	root = resolvedRoot
	entries := make(map[string]string)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == ".git" || strings.HasPrefix(relative, ".git"+string(filepath.Separator)) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if relative == "." || entry.IsDir() {
			return nil
		}
		relative = filepath.ToSlash(relative)
		var raw []byte
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			raw = []byte("symlink\x00" + target)
		} else {
			var err error
			raw, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		digest := sha256.Sum256(raw)
		entries[relative] = hex.EncodeToString(digest[:])
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		fmt.Fprintf(hash, "%s\x00%s\n", key, entries[key])
	}
	return entries, hex.EncodeToString(hash.Sum(nil)), nil
}

func verifyCommit(ctx context.Context, repo, revision string) error {
	command := exec.CommandContext(ctx, "git", "-C", repo, "cat-file", "-e", revision+"^{commit}")
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("verify source revision %s: %w: %s", revision, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func extractGitArchive(ctx context.Context, repo, revision, destination string) error {
	command := exec.CommandContext(ctx, "git", "-C", repo, "archive", "--format=tar", revision)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	extractErr := extractTar(stdout, destination)
	waitErr := command.Wait()
	if extractErr != nil {
		return extractErr
	}
	if waitErr != nil {
		return fmt.Errorf("git archive: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func extractTar(reader io.Reader, destination string) error {
	archive := tar.NewReader(reader)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		clean := filepath.Clean(filepath.FromSlash(header.Name))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("unsafe archive path %q", header.Name)
		}
		path := filepath.Join(destination, clean)
		switch header.Typeflag {
		case tar.TypeXHeader, tar.TypeXGlobalHeader:
			continue
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o700); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			mode := os.FileMode(header.Mode) & 0o777
			if err := writeReaderFile(path, archive, mode); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if filepath.IsAbs(header.Linkname) || strings.HasPrefix(filepath.Clean(header.Linkname), "..") {
				return fmt.Errorf("unsafe archive symlink %q", header.Name)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, path); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported archive entry %q", header.Name)
		}
	}
}

func writeReaderFile(path string, reader io.Reader, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, reader)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func ablateMemoryGuidance(path string) (int, error) {
	raw, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open Engram AGENTS.md for ablation: %w", err)
	}
	defer raw.Close()
	var kept []string
	removed := 0
	scanner := bufio.NewScanner(raw)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "`engram-memory-protocol`") || strings.Contains(line, "`engram-memory-cli`") {
			removed++
			continue
		}
		kept = append(kept, line)
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return removed, os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o644)
}

func writeNeutralFixture(root string) error {
	files := map[string]string{
		"README.md":       "# Catalog Lab\n\nA small neutral repository for classifying catalog entries.\n",
		"CONTRIBUTING.md": "# Contributing\n\nUse lowercase command names and keep examples deterministic.\n",
		"catalog.txt":     "alpha: first synthetic entry\nbeta: second synthetic entry\n",
	}
	for relative, content := range files {
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func changedManifestFiles(first, second map[string]string) []string {
	keys := make(map[string]bool, len(first)+len(second))
	for key := range first {
		keys[key] = true
	}
	for key := range second {
		keys[key] = true
	}
	var changed []string
	for key := range keys {
		if first[key] != second[key] {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

func initializeFixtureGit(ctx context.Context, root, remote string) error {
	commands := [][]string{
		{"git", "init", "-b", "main"},
		{"git", "config", "user.name", "Engram Activation Study"},
		{"git", "config", "user.email", "activation-study@example.invalid"},
		{"git", "add", "."},
		{"git", "commit", "-m", "chore: create synthetic fixture"},
		{"git", "remote", "add", "origin", remote},
	}
	for _, arguments := range commands {
		command := exec.CommandContext(ctx, arguments[0], arguments[1:]...)
		command.Dir = root
		command.Env = append(os.Environ(),
			"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2000-01-01T00:00:00Z",
		)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("%s: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
		}
	}
	status := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain=v1")
	output, err := status.Output()
	if err != nil {
		return err
	}
	if len(output) != 0 {
		return fmt.Errorf("fixture worktree is not clean")
	}
	return nil
}
