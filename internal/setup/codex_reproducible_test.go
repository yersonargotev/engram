package setup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testReleaseCommit = "7417bbcc5da4d0946f5746eed5c1b63ed9f0ca6c"

func TestInstallCodexStableSetupPinsReleaseTag(t *testing.T) {
	resetSetupSeams(t)
	useTestHome(t)

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}

	var commands [][]string
	runCommand = func(name string, args ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, args...))
		return nil, nil
	}

	_, err := InstallWithOptions("codex", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}

	want := []string{
		"/usr/local/bin/codex",
		"plugin", "marketplace", "add",
		codexMarketplace,
		"--ref", "v2.2.1",
		"--json",
	}
	if !slices.ContainsFunc(commands, func(command []string) bool {
		return slices.Equal(command, want)
	}) {
		t.Fatalf("stable setup commands = %v, want %v", commands, want)
	}
	for _, command := range commands {
		if slices.Contains(command, "main") {
			t.Fatalf("stable setup must not follow main, got command %v", command)
		}
	}
}

func TestInstallCodexRejectsUnexpectedMarketplaceCommitBeforePluginInstall(t *testing.T) {
	resetSetupSeams(t)
	useTestHome(t)

	marketplaceRoot := t.TempDir()
	gitDir := filepath.Join(marketplaceRoot, ".git")
	if err := os.MkdirAll(gitDir, 0755); err != nil {
		t.Fatalf("create marketplace git metadata: %v", err)
	}
	wrongCommit := strings.Repeat("a", 40)
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte(wrongCommit+"\n"), 0644); err != nil {
		t.Fatalf("write marketplace HEAD: %v", err)
	}
	gitConfig := "[remote \"origin\"]\n\turl = https://github.com/yersonargotev/engram.git\n"
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(gitConfig), 0644); err != nil {
		t.Fatalf("write marketplace git config: %v", err)
	}

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	pluginAddCalls := 0
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}) {
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		}
		if len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}) {
			pluginAddCalls++
		}
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{
		Version: "2.2.1",
		Commit:  testReleaseCommit,
	})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}
	if result.Complete {
		t.Fatal("setup with an unexpected marketplace commit must be incomplete")
	}
	pluginCheck, ok := result.Check("plugin")
	if !ok {
		t.Fatalf("missing plugin capability check: %+v", result.Checks)
	}
	if pluginCheck.Status != CheckFailed || !strings.Contains(pluginCheck.Detail, wrongCommit) {
		t.Fatalf("plugin check = %+v, want failed check naming unexpected commit", pluginCheck)
	}
	if pluginAddCalls != 0 {
		t.Fatalf("plugin add ran %d times before source verification", pluginAddCalls)
	}
}

func TestInstallCodexPreservesCustomInstructionsWhenPluginVerificationFails(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)

	customInstructions := filepath.Join(home, "custom", "instructions.md")
	customCompact := filepath.Join(home, "custom", "compact.md")
	if err := os.MkdirAll(filepath.Dir(customInstructions), 0755); err != nil {
		t.Fatalf("create custom instruction directory: %v", err)
	}
	instructionBytes := []byte("user-owned instructions\r\nkeep these bytes\r\n")
	compactBytes := []byte("user-owned compact prompt\n")
	if err := os.WriteFile(customInstructions, instructionBytes, 0644); err != nil {
		t.Fatalf("write custom instructions: %v", err)
	}
	if err := os.WriteFile(customCompact, compactBytes, 0644); err != nil {
		t.Fatalf("write custom compact prompt: %v", err)
	}

	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	configBytes := []byte(fmt.Sprintf("model_instructions_file = %q\nexperimental_compact_prompt_file = %q\nmodel = \"gpt-user\"\n", customInstructions, customCompact))
	if err := os.WriteFile(configPath, configBytes, 0644); err != nil {
		t.Fatalf("write Codex config: %v", err)
	}

	marketplaceRoot := t.TempDir()
	gitDir := filepath.Join(marketplaceRoot, ".git")
	if err := os.MkdirAll(gitDir, 0755); err != nil {
		t.Fatalf("create marketplace git metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte(strings.Repeat("b", 40)+"\n"), 0644); err != nil {
		t.Fatalf("write marketplace HEAD: %v", err)
	}
	gitConfig := "[remote \"origin\"]\n\turl = https://github.com/yersonargotev/engram.git\n"
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(gitConfig), 0644); err != nil {
		t.Fatalf("write marketplace git config: %v", err)
	}

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}) {
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		}
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}

	assertFileBytes(t, configPath, configBytes)
	assertFileBytes(t, customInstructions, instructionBytes)
	assertFileBytes(t, customCompact, compactBytes)
	if !slices.Contains(result.Preserved, "model_instructions_file") ||
		!slices.Contains(result.Preserved, "experimental_compact_prompt_file") {
		t.Fatalf("preserved state = %v, want both custom instruction settings", result.Preserved)
	}
	if _, err := os.Stat(filepath.Join(home, ".codex", "engram-instructions.md")); !os.IsNotExist(err) {
		t.Fatalf("unverified setup wrote legacy instructions: %v", err)
	}
}

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s changed\ngot:  %q\nwant: %q", path, got, want)
	}
}

func TestInstallCodexReportsIndependentCapabilities(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) {
		return "/opt/homebrew/Caskroom/engram/2.2.1/engram", nil
	}
	statFn = func(path string) (os.FileInfo, error) {
		if filepath.ToSlash(path) == "/opt/homebrew/bin/engram" {
			return nil, nil
		}
		return nil, os.ErrNotExist
	}

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}

	wantStatuses := map[string]CheckStatus{
		"plugin":         CheckReady,
		"mcp":            CheckReady,
		"activation-cue": CheckMissing,
		"verifier":       CheckMissing,
	}
	if len(result.Checks) != len(wantStatuses) {
		t.Fatalf("checks = %+v, want exactly four capability checks", result.Checks)
	}
	for capability, wantStatus := range wantStatuses {
		check, ok := result.Check(capability)
		if !ok || check.Status != wantStatus {
			t.Fatalf("%s check = %+v, want status %q", capability, check, wantStatus)
		}
	}
	if result.Complete {
		t.Fatal("setup without a Stop verifier must not report complete")
	}

	configPath := filepath.Join(home, ".codex", "config.toml")
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read generated Codex config: %v", err)
	}
	if !strings.Contains(string(config), `command = "/opt/homebrew/bin/engram"`) {
		t.Fatalf("generated config does not use stable executable path:\n%s", config)
	}
}

func TestInstallCodexReportsCanonicalCheckpointCapabilitiesWithoutSharedInstructions(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeCanonicalCodexActivationFixture(t, filepath.Join(marketplaceRoot, "plugin", "codex"))
	writeCanonicalCodexActivationFixture(t, installedPlugin)

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.5","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}
	activation, ok := result.Check("activation-cue")
	if !ok || activation.Status != CheckReady {
		t.Fatalf("activation-cue check = %+v, want ready for installed canonical skill and cue", activation)
	}
	verifier, ok := result.Check("verifier")
	if !ok || verifier.Status != CheckReady || !result.Complete {
		t.Fatalf("verifier check = %+v complete=%v, want ready for the installed canonical Stop verifier", verifier, result.Complete)
	}

	for _, shared := range []string{"AGENTS.md", "CLAUDE.md"} {
		if _, err := os.Stat(filepath.Join(home, shared)); !os.IsNotExist(err) {
			t.Fatalf("Codex setup created or modified shared instruction file %s: %v", shared, err)
		}
	}
}

func TestInstallCodexDoesNotReportActivationReadyForIncompleteCanonicalSkill(t *testing.T) {
	resetSetupSeams(t)
	useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	for _, root := range []string{filepath.Join(marketplaceRoot, "plugin", "codex"), installedPlugin} {
		writeCanonicalCodexActivationFixture(t, root)
		skillPath := filepath.Join(root, "skills", "memory", "SKILL.md")
		raw, err := os.ReadFile(skillPath)
		if err != nil {
			t.Fatalf("read incomplete skill fixture: %v", err)
		}
		incomplete := strings.Replace(string(raw), "## Finalize idempotently", "## Finish", 1)
		if err := os.WriteFile(skillPath, []byte(incomplete), 0o644); err != nil {
			t.Fatalf("write incomplete skill fixture: %v", err)
		}
	}

	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.5","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}
	activation, ok := result.Check("activation-cue")
	if !ok || activation.Status != CheckMissing {
		t.Fatalf("activation-cue check = %+v, want missing for incomplete rubric", activation)
	}
}

func writeCanonicalCodexActivationFixture(t *testing.T, destination string) {
	t.Helper()
	source := filepath.Join("..", "..", "plugin", "codex")
	paths := []string{
		".codex-plugin/plugin.json",
		".mcp.json",
		"hooks/hooks.json",
		"scripts/_checkpoint.sh",
		"scripts/session-start.sh",
		"scripts/post-compaction.sh",
		"scripts/stop.sh",
		"scripts/stop.ps1",
		"skills/memory/SKILL.md",
	}
	for _, relative := range paths {
		raw, err := os.ReadFile(filepath.Join(source, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read canonical Codex fixture %s: %v", relative, err)
		}
		target := filepath.Join(destination, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("create canonical Codex fixture directory: %v", err)
		}
		mode := os.FileMode(0o644)
		if strings.HasPrefix(relative, "scripts/") {
			mode = 0o755
		}
		if err := os.WriteFile(target, raw, mode); err != nil {
			t.Fatalf("write canonical Codex fixture %s: %v", relative, err)
		}
	}
}

func writeMarketplaceIdentity(t *testing.T, root, commit string) {
	t.Helper()
	gitDir := filepath.Join(root, ".git")
	if err := os.MkdirAll(gitDir, 0755); err != nil {
		t.Fatalf("create marketplace git metadata: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte(commit+"\n"), 0644); err != nil {
		t.Fatalf("write marketplace HEAD: %v", err)
	}
	gitConfig := "[remote \"origin\"]\n\turl = https://github.com/yersonargotev/engram.git\n"
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(gitConfig), 0644); err != nil {
		t.Fatalf("write marketplace git config: %v", err)
	}
	gitStatusFn = func(gotRoot string) ([]byte, error) {
		if gotRoot != root {
			return nil, fmt.Errorf("git status root = %q, want %q", gotRoot, root)
		}
		return nil, nil
	}
	gitResolveRefFn = func(gotRoot, _ string) ([]byte, error) {
		if gotRoot != root {
			return nil, fmt.Errorf("git ref root = %q, want %q", gotRoot, root)
		}
		return []byte(commit + "\n"), nil
	}
}

func TestVerifyCodexMarketplaceRootResolvesSymbolicHead(t *testing.T) {
	tests := []struct {
		name   string
		packed bool
	}{
		{name: "loose ref"},
		{name: "packed ref", packed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSetupSeams(t)
			root := t.TempDir()
			gitDir := filepath.Join(root, ".git")
			if err := os.MkdirAll(gitDir, 0755); err != nil {
				t.Fatalf("create git metadata: %v", err)
			}
			if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0644); err != nil {
				t.Fatalf("write symbolic HEAD: %v", err)
			}
			if tc.packed {
				packed := "# pack-refs with: peeled fully-peeled sorted\n" + testReleaseCommit + " refs/heads/main\n"
				if err := os.WriteFile(filepath.Join(gitDir, "packed-refs"), []byte(packed), 0644); err != nil {
					t.Fatalf("write packed refs: %v", err)
				}
			} else {
				refPath := filepath.Join(gitDir, "refs", "heads", "main")
				if err := os.MkdirAll(filepath.Dir(refPath), 0755); err != nil {
					t.Fatalf("create loose ref directory: %v", err)
				}
				if err := os.WriteFile(refPath, []byte(testReleaseCommit+"\n"), 0644); err != nil {
					t.Fatalf("write loose ref: %v", err)
				}
			}
			gitConfig := "[remote \"origin\"]\n\turl = https://github.com/yersonargotev/engram.git\n"
			if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(gitConfig), 0644); err != nil {
				t.Fatalf("write git config: %v", err)
			}

			identity, err := verifyCodexMarketplaceRoot(root, testReleaseCommit)
			if err != nil {
				t.Fatalf("verify symbolic marketplace HEAD: %v", err)
			}
			if identity.Commit != testReleaseCommit {
				t.Fatalf("commit = %q, want %q", identity.Commit, testReleaseCommit)
			}
		})
	}
}

func writeInstalledCodexPlugin(t *testing.T, root, marketplaceRoot string, includeStop bool) {
	t.Helper()
	writeCodexPluginFixture(t, root, includeStop)
	writeCodexPluginFixture(t, filepath.Join(marketplaceRoot, "plugin", "codex"), includeStop)
}

func TestVerifyInstalledCodexPluginRejectsStubStopHook(t *testing.T) {
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	marketplaceRoot := t.TempDir()
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, true)
	assets, err := snapshotCodexPluginTree(filepath.Join(marketplaceRoot, "plugin", "codex"))
	if err != nil {
		t.Fatalf("snapshot stub Codex plugin: %v", err)
	}
	output := []byte(fmt.Sprintf(`{"name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin))

	verified, err := verifyInstalledCodexPlugin(output, assets)
	if err != nil {
		t.Fatalf("verify installed stub plugin: %v", err)
	}
	if verified.VerifierReady {
		t.Fatal("stub Stop entry without the canonical command and executable asset reported verifier ready")
	}
}

func TestVerifyInstalledCodexStopVerifierRejectsModifiedLaunchers(t *testing.T) {
	root := t.TempDir()
	writeCanonicalCodexActivationFixture(t, root)
	hooksRaw, err := os.ReadFile(filepath.Join(root, "hooks", "hooks.json"))
	if err != nil {
		t.Fatalf("read canonical hooks: %v", err)
	}
	assets, err := snapshotCodexPluginTree(root)
	if err != nil {
		t.Fatalf("snapshot canonical plugin: %v", err)
	}
	if !verifyInstalledCodexStopVerifier(hooksRaw, assets) {
		t.Fatal("canonical Stop launchers did not report ready")
	}

	for _, relative := range []string{"scripts/stop.sh", "scripts/stop.ps1"} {
		t.Run(relative, func(t *testing.T) {
			modified := make(map[string]codexPluginTreeEntry, len(assets))
			for path, entry := range assets {
				modified[path] = entry
			}
			entry := modified[relative]
			entry.data = append(append([]byte(nil), entry.data...), []byte("# modified\n")...)
			modified[relative] = entry
			if verifyInstalledCodexStopVerifier(hooksRaw, modified) {
				t.Fatalf("modified %s reported verifier ready", relative)
			}
		})
	}
}

func writeCodexPluginFixture(t *testing.T, root string, includeStop bool) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, ".codex-plugin"), 0755); err != nil {
		t.Fatalf("create plugin manifest directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "hooks"), 0755); err != nil {
		t.Fatalf("create plugin hooks directory: %v", err)
	}
	manifest := `{"name":"engram","version":"0.1.3","repository":"https://github.com/yersonargotev/engram","hooks":"./hooks/hooks.json","mcpServers":"./.mcp.json"}`
	if err := os.WriteFile(filepath.Join(root, ".codex-plugin", "plugin.json"), []byte(manifest), 0644); err != nil {
		t.Fatalf("write plugin manifest: %v", err)
	}
	mcp := `{"mcpServers":{"engram":{"command":"engram","args":["mcp","--tools=agent"]}}}`
	if err := os.WriteFile(filepath.Join(root, ".mcp.json"), []byte(mcp), 0644); err != nil {
		t.Fatalf("write plugin MCP manifest: %v", err)
	}
	hooks := `{"hooks":{"SessionStart":[]}}`
	if includeStop {
		hooks = `{"hooks":{"SessionStart":[],"Stop":[{"matcher":"","hooks":[{"type":"command","command":"verify"}]}]}}`
	}
	if err := os.WriteFile(filepath.Join(root, "hooks", "hooks.json"), []byte(hooks), 0644); err != nil {
		t.Fatalf("write plugin hooks manifest: %v", err)
	}
}

func TestInstallCodexPreservesCustomInstructionsAfterPluginVerification(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }

	customInstructions := filepath.Join(home, "custom", "instructions.md")
	customCompact := filepath.Join(home, "custom", "compact.md")
	if err := os.MkdirAll(filepath.Dir(customInstructions), 0755); err != nil {
		t.Fatalf("create custom instruction directory: %v", err)
	}
	instructionBytes := []byte("custom instructions\r\n")
	compactBytes := []byte("custom compact prompt\n")
	if err := os.WriteFile(customInstructions, instructionBytes, 0644); err != nil {
		t.Fatalf("write custom instructions: %v", err)
	}
	if err := os.WriteFile(customCompact, compactBytes, 0644); err != nil {
		t.Fatalf("write custom compact prompt: %v", err)
	}

	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	customInstructionLine := fmt.Sprintf("model_instructions_file = %q", customInstructions)
	customCompactLine := fmt.Sprintf("experimental_compact_prompt_file = %q", customCompact)
	configBytes := []byte(customInstructionLine + "\r\n" + customCompactLine + "\r\nmodel = \"gpt-user\"\r\n")
	if err := os.WriteFile(configPath, configBytes, 0644); err != nil {
		t.Fatalf("write Codex config: %v", err)
	}

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex) failed: %v", err)
	}
	activation, ok := result.Check("activation-cue")
	if !ok || activation.Status != CheckMissing {
		t.Fatalf("activation-cue check = %+v, want missing until the canonical cue is installed", activation)
	}
	mcpCheck, ok := result.Check("mcp")
	if !ok || mcpCheck.Status != CheckReady {
		t.Fatalf("CRLF config MCP check = %+v, want ready", mcpCheck)
	}

	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read Codex config: %v", err)
	}
	if !strings.Contains(string(config), customInstructionLine) || !strings.Contains(string(config), customCompactLine) {
		t.Fatalf("custom instruction settings were replaced:\n%s", config)
	}
	if !strings.HasPrefix(string(config), string(configBytes)) {
		t.Fatalf("custom Codex config prefix changed byte-for-byte\ngot:  %q\nwant prefix: %q", config, configBytes)
	}
	if !strings.Contains(string(config), `[mcp_servers.engram]`) {
		t.Fatalf("verified setup did not add MCP registration:\n%s", config)
	}
	assertFileBytes(t, customInstructions, instructionBytes)
	assertFileBytes(t, customCompact, compactBytes)
	if _, err := os.Stat(filepath.Join(home, ".codex", "engram-instructions.md")); !os.IsNotExist(err) {
		t.Fatalf("custom activation should not create Engram legacy instructions: %v", err)
	}
}

func TestInstallCodexStableSetupRequiresReleaseIdentityBeforeMutation(t *testing.T) {
	tests := []struct {
		name    string
		options InstallOptions
	}{
		{name: "missing identity"},
		{
			name: "local pseudo-version is not a release tag",
			options: InstallOptions{
				Version: "1.20.1-0.20260827124451-9d4efe72321e+dirty",
				Commit:  "9d4efe72321ee2ee9088e02ed34fa780382c57cc",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetSetupSeams(t)
			home := useTestHome(t)
			lookPathFn = func(string) (string, error) {
				t.Fatal("stable identity validation must run before resolving Codex")
				return "", nil
			}
			runCommand = func(string, ...string) ([]byte, error) {
				t.Fatal("stable identity validation must run before external commands")
				return nil, nil
			}

			_, err := InstallWithOptions("codex", tc.options)
			if err == nil || !strings.Contains(err.Error(), "release identity") {
				t.Fatalf("InstallWithOptions(codex) error = %v, want release identity error", err)
			}
			if _, statErr := os.Stat(filepath.Join(home, ".codex")); !os.IsNotExist(statErr) {
				t.Fatalf("invalid stable setup mutated Codex home: %v", statErr)
			}
		})
	}
}

func TestInstallCodexDevelopmentModeExplicitlyUsesMain(t *testing.T) {
	resetSetupSeams(t)
	useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }
	marketplaceRoot := t.TempDir()
	developmentCommit := strings.Repeat("a", 40)
	writeMarketplaceIdentity(t, marketplaceRoot, developmentCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	var marketplaceCommand []string
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}) {
			marketplaceCommand = append([]string(nil), args...)
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		}
		if len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}) {
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		}
		return nil, fmt.Errorf("unexpected codex command: %v", args)
	}

	result, err := InstallWithOptions("codex", InstallOptions{Development: true, Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("InstallWithOptions(codex development) failed: %v", err)
	}
	if !slices.Contains(marketplaceCommand, "main") {
		t.Fatalf("development marketplace command = %v, want explicit main ref", marketplaceCommand)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckReady || !strings.Contains(plugin.Detail, developmentCommit) {
		t.Fatalf("development plugin check = %+v, want moving main commit %s accepted", plugin, developmentCommit)
	}
}

func TestInstallCodexIsolatedHomeAcceptance(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	commandLog := filepath.Join(t.TempDir(), "codex.log")
	fakeBin := t.TempDir()
	writeFakeCodexCLI(t, fakeBin, marketplaceRoot, installedPlugin)
	t.Setenv("FAKE_CODEX_LOG", commandLog)
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

	options := InstallOptions{Version: "2.2.1", Commit: testReleaseCommit}
	first, err := InstallWithOptions("codex", options)
	if err != nil {
		t.Fatalf("first isolated setup: %v", err)
	}
	if first.Files != 3 {
		t.Fatalf("first setup changed %d files, want 3", first.Files)
	}
	paths := []string{
		filepath.Join(home, ".codex", "config.toml"),
		filepath.Join(home, ".codex", "engram-instructions.md"),
		filepath.Join(home, ".codex", "engram-compact-prompt.md"),
	}
	baseline := readFiles(t, paths)
	config := string(baseline[paths[0]])
	if strings.Count(config, "[mcp_servers.engram]") != 1 {
		t.Fatalf("generated config has duplicate Engram MCP blocks:\n%s", config)
	}
	if !strings.Contains(config, `command = "/usr/local/bin/engram"`) {
		t.Fatalf("generated config lacks stable executable path:\n%s", config)
	}

	second, err := InstallWithOptions("codex", options)
	if err != nil {
		t.Fatalf("second isolated setup: %v", err)
	}
	for path, want := range baseline {
		assertFileBytes(t, path, want)
	}
	if second.Files != 0 {
		t.Fatalf("byte-stable rerun reported %d changed files despite identical outputs", second.Files)
	}

	logRaw, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatalf("read simulated Codex command log: %v", err)
	}
	logText := string(logRaw)
	if strings.Contains(logText, "--ref main") || strings.Count(logText, "--ref v2.2.1 --json") != 2 {
		t.Fatalf("simulated Codex commands did not use the immutable release twice:\n%s", logText)
	}

	interruptedHome := t.TempDir()
	userHomeDir = func() (string, error) { return interruptedHome, nil }
	realAtomicWrite := atomicWriteFileFn
	failConfigOnce := true
	atomicWriteFileFn = func(path string, data []byte, mode os.FileMode) error {
		if failConfigOnce && path == filepath.Join(interruptedHome, ".codex", "config.toml") {
			failConfigOnce = false
			return errors.New("simulated interruption before config publish")
		}
		return realAtomicWrite(path, data, mode)
	}
	if _, err := InstallWithOptions("codex", options); err == nil || !strings.Contains(err.Error(), "simulated interruption") {
		t.Fatalf("interrupted setup error = %v, want injected publish failure", err)
	}
	atomicWriteFileFn = realAtomicWrite
	if _, err := InstallWithOptions("codex", options); err != nil {
		t.Fatalf("rerun after interrupted setup: %v", err)
	}
	interruptedPaths := []string{
		filepath.Join(interruptedHome, ".codex", "config.toml"),
		filepath.Join(interruptedHome, ".codex", "engram-instructions.md"),
		filepath.Join(interruptedHome, ".codex", "engram-compact-prompt.md"),
	}
	for i, path := range interruptedPaths {
		want := baseline[paths[i]]
		if i == 0 {
			want = []byte(strings.ReplaceAll(string(want), home, interruptedHome))
		}
		assertFileBytes(t, path, want)
	}
}

func readFiles(t *testing.T, paths []string) map[string][]byte {
	t.Helper()
	result := make(map[string][]byte, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		result[path] = data
	}
	return result
}

func writeFakeCodexCLI(t *testing.T, dir, marketplaceRoot, installedPlugin string) {
	t.Helper()
	addJSON := fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)
	pluginJSON := fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)
	script := fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$*" >> "$FAKE_CODEX_LOG"
case "$1 $2 $3" in
  "plugin marketplace add") printf '%%s\n' '%s' ;;
  "plugin add engram@engram") printf '%%s\n' '%s' ;;
  *) printf 'unexpected command: %%s\n' "$*" >&2; exit 2 ;;
esac
`, addJSON, pluginJSON)
	path := filepath.Join(dir, "codex")
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatalf("write simulated Codex CLI: %v", err)
	}
}

func stubVerifiedCodexCLI(t *testing.T, includeStop bool) *[][]string {
	t.Helper()
	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, includeStop)
	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	commands := make([][]string, 0, 2)
	runCommand = func(name string, args ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, args...))
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}
	return &commands
}

func TestInstallCodexUpgradesOwnedMarketplaceToReleaseRef(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }
	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	config := "model = \"gpt-user\"\n\n[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.0\"\n\n[plugins.\"engram@engram\"]\nenabled = true\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("write existing Codex config: %v", err)
	}

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	lookPathFn = func(file string) (string, error) {
		if file == "codex" {
			return "/usr/local/bin/codex", nil
		}
		return "", errors.New("not found")
	}
	var commands [][]string
	runCommand = func(_ string, args ...string) ([]byte, error) {
		commands = append(commands, append([]string(nil), args...))
		switch {
		case slices.Equal(args, []string{"plugin", "list", "--json"}):
			return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installed":true,"enabled":true,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}],"available":[]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "upgrade", "engram"}):
			return []byte(fmt.Sprintf(`{"selectedMarketplaces":["engram"],"upgradedRoots":[%q],"errors":[]}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("upgrade owned marketplace: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckReady {
		t.Fatalf("plugin check = %+v, want ready", plugin)
	}
	for _, command := range commands {
		if len(command) >= 3 && slices.Equal(command[:3], []string{"plugin", "marketplace", "add"}) {
			t.Fatalf("owned marketplace upgrade used destructive/add path: %v", commands)
		}
	}
	updated, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read upgraded Codex config: %v", err)
	}
	if !strings.Contains(string(updated), `ref = "v2.2.1"`) || !strings.Contains(string(updated), `model = "gpt-user"`) {
		t.Fatalf("owned marketplace ref was not updated safely:\n%s", updated)
	}
}

func TestUpsertTopLevelCodexSettingsConvergesToCodexTableSpacing(t *testing.T) {
	input := "[marketplaces.engram]\nsource_type = \"git\"\n"
	first := upsertTopLevelTOMLString(input, "model_instructions_file", "/tmp/instructions.md")
	first = upsertTopLevelTOMLString(first, "experimental_compact_prompt_file", "/tmp/compact.md")
	second := upsertTopLevelTOMLString(first, "model_instructions_file", "/tmp/instructions.md")
	second = upsertTopLevelTOMLString(second, "experimental_compact_prompt_file", "/tmp/compact.md")

	wantPrefix := "model_instructions_file = \"/tmp/instructions.md\"\nexperimental_compact_prompt_file = \"/tmp/compact.md\"\n\n[marketplaces.engram]"
	if !strings.HasPrefix(first, wantPrefix) {
		t.Fatalf("first generated config does not separate top-level settings from Codex tables:\n%s", first)
	}
	if second != first {
		t.Fatalf("top-level setting upsert is not byte-stable\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func TestInstallCodexPreservesUnknownMarketplaceStateWithoutCommands(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	config := []byte("[marketplaces.engram]\r\nsource_type = \"git\"\r\nsource = \"https://github.com/user/custom-engram.git\"\r\nref = \"feature/custom\"\r\n\r\n[plugins.\"engram@engram\"]\r\nenabled = false\r\n")
	if err := os.WriteFile(configPath, config, 0644); err != nil {
		t.Fatalf("write custom marketplace config: %v", err)
	}
	lookPathFn = func(string) (string, error) {
		t.Fatal("unknown plugin state must be classified before resolving Codex")
		return "", nil
	}
	runCommand = func(string, ...string) ([]byte, error) {
		t.Fatal("unknown plugin state must not trigger Codex commands")
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("preserve unknown marketplace state: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckPreserved {
		t.Fatalf("plugin check = %+v, want preserved", plugin)
	}
	if !slices.Contains(result.Preserved, "marketplaces.engram") {
		t.Fatalf("preserved state = %v, want marketplaces.engram", result.Preserved)
	}
	assertFileBytes(t, configPath, config)
	if _, err := os.Stat(filepath.Join(home, ".codex", "engram-instructions.md")); !os.IsNotExist(err) {
		t.Fatalf("unknown plugin state triggered local setup: %v", err)
	}
}

func TestInstallCodexPreservesModifiedInstalledPluginBeforeMutation(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	config := []byte("[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.1\"\n\n[plugins.\"engram@engram\"]\nenabled = true\n")
	if err := os.WriteFile(configPath, config, 0644); err != nil {
		t.Fatalf("write Codex config: %v", err)
	}

	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	modifiedHooks := []byte(`{"hooks":{"SessionStart":[]},"user":"custom"}`)
	modifiedPath := filepath.Join(installedPlugin, "hooks", "hooks.json")
	if err := os.WriteFile(modifiedPath, modifiedHooks, 0644); err != nil {
		t.Fatalf("modify installed plugin: %v", err)
	}
	lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if slices.Equal(args, []string{"plugin", "list", "--json"}) {
			return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installed":true,"enabled":true,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
		}
		t.Fatalf("modified installed plugin must be preserved before command %v", args)
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("preserve modified installed plugin: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckPreserved {
		t.Fatalf("plugin check = %+v, want preserved", plugin)
	}
	assertFileBytes(t, configPath, config)
	assertFileBytes(t, modifiedPath, modifiedHooks)
}

func TestInstallCodexPreservesPluginWhenConfiguredRefDoesNotMatchCheckout(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	config := []byte("[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.1\"\n\n[plugins.\"engram@engram\"]\nenabled = true\n")
	if err := os.WriteFile(configPath, config, 0644); err != nil {
		t.Fatalf("write Codex config: %v", err)
	}
	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	configuredRefCommit := strings.Repeat("b", 40)
	gitResolveRefFn = func(string, string) ([]byte, error) { return []byte(configuredRefCommit + "\n"), nil }
	installedPlugin := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if slices.Equal(args, []string{"plugin", "list", "--json"}) {
			return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installed":true,"enabled":true,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
		}
		t.Fatalf("mismatched configured ref must be preserved before command %v", args)
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("preserve mismatched configured ref: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckPreserved || !strings.Contains(plugin.Detail, "does not match expected commit") {
		t.Fatalf("plugin check = %+v, want configured-ref mismatch preserved", plugin)
	}
	assertFileBytes(t, configPath, config)
}

func TestInstallCodexPreservesOrphanedPluginCacheWithoutConfigState(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	orphanPath := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "custom", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(orphanPath), 0755); err != nil {
		t.Fatalf("create orphaned plugin cache: %v", err)
	}
	orphanBytes := []byte("user-authored plugin bytes\r\n")
	if err := os.WriteFile(orphanPath, orphanBytes, 0644); err != nil {
		t.Fatalf("write orphaned plugin cache: %v", err)
	}
	lookPathFn = func(string) (string, error) {
		t.Fatal("orphaned plugin cache must be classified before resolving Codex")
		return "", nil
	}
	runCommand = func(string, ...string) ([]byte, error) {
		t.Fatal("orphaned plugin cache must not trigger Codex commands")
		return nil, nil
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("preserve orphaned plugin cache: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckPreserved {
		t.Fatalf("plugin check = %+v, want preserved", plugin)
	}
	assertFileBytes(t, orphanPath, orphanBytes)
}

func TestInstallCodexRejectsInstalledPluginThatDiffersFromVerifiedCheckout(t *testing.T) {
	resetSetupSeams(t)
	useTestHome(t)
	marketplaceRoot := t.TempDir()
	writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
	installedPlugin := filepath.Join(t.TempDir(), "engram", "0.1.3")
	writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
	if err := os.WriteFile(filepath.Join(installedPlugin, ".mcp.json"), []byte(`{"mcpServers":{}}`), 0644); err != nil {
		t.Fatalf("substitute installed MCP asset: %v", err)
	}
	lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }
	runCommand = func(_ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
			return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
		case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
			return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
		default:
			return nil, fmt.Errorf("unexpected codex command: %v", args)
		}
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("verify substituted installed plugin: %v", err)
	}
	plugin, ok := result.Check("plugin")
	if !ok || plugin.Status != CheckFailed || !strings.Contains(plugin.Detail, "does not match the verified marketplace checkout") {
		t.Fatalf("plugin check = %+v, want checkout mismatch", plugin)
	}
}

func TestInstallCodexReportsRefMutationWhenUpgradeFails(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	configPath := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		t.Fatalf("create Codex config directory: %v", err)
	}
	config := "[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.0\"\n"
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("write old marketplace config: %v", err)
	}
	lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }
	runCommand = func(_ string, args ...string) ([]byte, error) {
		if len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "upgrade", "engram"}) {
			return []byte("network unavailable"), errors.New("upgrade failed")
		}
		return nil, fmt.Errorf("unexpected codex command: %v", args)
	}

	result, err := InstallWithOptions("codex", InstallOptions{Version: "2.2.1", Commit: testReleaseCommit})
	if err != nil {
		t.Fatalf("failed upgrade result: %v", err)
	}
	if result.Files != 1 {
		t.Fatalf("failed upgrade reported %d changed files, want changed config", result.Files)
	}
	updated, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read failed-upgrade config: %v", err)
	}
	if !strings.Contains(string(updated), `ref = "v2.2.1"`) {
		t.Fatalf("test did not exercise ref mutation:\n%s", updated)
	}
}

func TestInstallCodexRecoversInterruptedOwnedMarketplaceTransition(t *testing.T) {
	for _, interruptedPhase := range []string{"marketplace upgrade", "plugin add"} {
		t.Run(interruptedPhase, func(t *testing.T) {
			resetSetupSeams(t)
			home := useTestHome(t)
			osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }
			configPath := filepath.Join(home, ".codex", "config.toml")
			if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
				t.Fatalf("create Codex config directory: %v", err)
			}
			initialConfig := []byte("model = \"gpt-user\"\n\n[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.0\"\n\n[plugins.\"engram@engram\"]\nenabled = true\n")
			if err := os.WriteFile(configPath, initialConfig, 0644); err != nil {
				t.Fatalf("write existing Codex config: %v", err)
			}

			oldCommit := strings.Repeat("a", 40)
			marketplaceRoot := t.TempDir()
			writeMarketplaceIdentity(t, marketplaceRoot, oldCommit)
			gitResolveRefFn = func(gotRoot, ref string) ([]byte, error) {
				if gotRoot != marketplaceRoot {
					return nil, fmt.Errorf("git ref root = %q, want %q", gotRoot, marketplaceRoot)
				}
				switch ref {
				case "v2.2.0":
					return []byte(oldCommit + "\n"), nil
				case "v2.2.1":
					return []byte(testReleaseCommit + "\n"), nil
				default:
					return nil, fmt.Errorf("unexpected ref %q", ref)
				}
			}
			installedPlugin := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.3")
			writeInstalledCodexPlugin(t, installedPlugin, marketplaceRoot, false)
			lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }

			upgradeCalls := 0
			pluginAddCalls := 0
			runCommand = func(_ string, args ...string) ([]byte, error) {
				switch {
				case slices.Equal(args, []string{"plugin", "list", "--json"}):
					return []byte(fmt.Sprintf(`{"installed":[{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installed":true,"enabled":true,"source":{"source":"local","path":%q},"marketplaceSource":{"sourceType":"git","source":"https://github.com/yersonargotev/engram.git"}}]}`, filepath.Join(marketplaceRoot, "plugin", "codex"))), nil
				case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "upgrade", "engram"}):
					upgradeCalls++
					if interruptedPhase == "marketplace upgrade" && upgradeCalls == 1 {
						return []byte("simulated interruption"), errors.New("interrupted")
					}
					if err := os.WriteFile(filepath.Join(marketplaceRoot, ".git", "HEAD"), []byte(testReleaseCommit+"\n"), 0644); err != nil {
						t.Fatalf("advance simulated marketplace: %v", err)
					}
					return []byte(fmt.Sprintf(`{"selectedMarketplaces":["engram"],"upgradedRoots":[%q],"errors":[]}`, marketplaceRoot)), nil
				case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
					pluginAddCalls++
					if interruptedPhase == "plugin add" && pluginAddCalls == 1 {
						return []byte("simulated interruption"), errors.New("interrupted")
					}
					return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
				default:
					return nil, fmt.Errorf("unexpected codex command: %v", args)
				}
			}

			options := InstallOptions{Version: "2.2.1", Commit: testReleaseCommit}
			first, err := InstallWithOptions("codex", options)
			if err != nil {
				t.Fatalf("interrupted setup: %v", err)
			}
			plugin, ok := first.Check("plugin")
			if !ok || plugin.Status != CheckFailed {
				t.Fatalf("interrupted plugin check = %+v, want failed", plugin)
			}
			assertFileBytes(t, configPath, initialConfig)
			if _, err := os.Stat(codexSetupTransactionPath(configPath)); err != nil {
				t.Fatalf("interrupted setup did not retain recovery transaction: %v", err)
			}

			second, err := InstallWithOptions("codex", options)
			if err != nil {
				t.Fatalf("rerun after %s interruption: %v", interruptedPhase, err)
			}
			plugin, ok = second.Check("plugin")
			if !ok || plugin.Status != CheckReady {
				t.Fatalf("recovered plugin check = %+v, want ready", plugin)
			}
			updated, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatalf("read recovered config: %v", err)
			}
			if !strings.Contains(string(updated), `ref = "v2.2.1"`) || !strings.Contains(string(updated), `model = "gpt-user"`) {
				t.Fatalf("recovered config lost identity or custom bytes:\n%s", updated)
			}
			if _, err := os.Stat(codexSetupTransactionPath(configPath)); !os.IsNotExist(err) {
				t.Fatalf("completed setup left recovery transaction behind: %v", err)
			}
		})
	}
}

func TestInstallCodexRecoversAttributedPartialFreshPluginInstall(t *testing.T) {
	for _, customPartial := range []bool{false, true} {
		name := "generated partial"
		if customPartial {
			name = "modified partial"
		}
		t.Run(name, func(t *testing.T) {
			resetSetupSeams(t)
			home := useTestHome(t)
			osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }
			configPath := filepath.Join(home, ".codex", "config.toml")
			marketplaceRoot := t.TempDir()
			writeMarketplaceIdentity(t, marketplaceRoot, testReleaseCommit)
			writeCodexPluginFixture(t, filepath.Join(marketplaceRoot, "plugin", "codex"), false)
			installedPlugin := filepath.Join(home, ".codex", "plugins", "cache", "engram", "engram", "0.1.3")
			lookPathFn = func(string) (string, error) { return "/usr/local/bin/codex", nil }

			pluginAddCalls := 0
			runCommand = func(_ string, args ...string) ([]byte, error) {
				switch {
				case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "add", codexMarketplace}):
					if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
						t.Fatalf("create simulated Codex config: %v", err)
					}
					config := []byte("[marketplaces.engram]\nsource_type = \"git\"\nsource = \"https://github.com/yersonargotev/engram.git\"\nref = \"v2.2.1\"\n")
					if err := os.WriteFile(configPath, config, 0644); err != nil {
						t.Fatalf("write simulated marketplace config: %v", err)
					}
					return []byte(fmt.Sprintf(`{"marketplaceName":"engram","installedRoot":%q,"alreadyAdded":false}`, marketplaceRoot)), nil
				case len(args) >= 4 && slices.Equal(args[:4], []string{"plugin", "marketplace", "upgrade", "engram"}):
					return []byte(fmt.Sprintf(`{"selectedMarketplaces":["engram"],"upgradedRoots":[%q],"errors":[]}`, marketplaceRoot)), nil
				case len(args) >= 3 && slices.Equal(args[:3], []string{"plugin", "add", "engram@engram"}):
					pluginAddCalls++
					if pluginAddCalls == 1 {
						if err := os.MkdirAll(filepath.Dir(filepath.Join(installedPlugin, ".mcp.json")), 0755); err != nil {
							t.Fatalf("create partial plugin cache: %v", err)
						}
						mcp, err := os.ReadFile(filepath.Join(marketplaceRoot, "plugin", "codex", ".mcp.json"))
						if err != nil {
							t.Fatalf("read verified MCP asset: %v", err)
						}
						if err := os.WriteFile(filepath.Join(installedPlugin, ".mcp.json"), mcp, 0644); err != nil {
							t.Fatalf("write partial plugin cache: %v", err)
						}
						return []byte("simulated interruption"), errors.New("interrupted")
					}
					writeCodexPluginFixture(t, installedPlugin, false)
					config, err := os.ReadFile(configPath)
					if err != nil {
						t.Fatalf("read simulated Codex config: %v", err)
					}
					config = append(config, []byte("\n[plugins.\"engram@engram\"]\nenabled = true\n")...)
					if err := os.WriteFile(configPath, config, 0644); err != nil {
						t.Fatalf("write simulated plugin config: %v", err)
					}
					return []byte(fmt.Sprintf(`{"pluginId":"engram@engram","name":"engram","marketplaceName":"engram","version":"0.1.3","installedPath":%q}`, installedPlugin)), nil
				default:
					return nil, fmt.Errorf("unexpected codex command: %v", args)
				}
			}

			options := InstallOptions{Version: "2.2.1", Commit: testReleaseCommit}
			first, err := InstallWithOptions("codex", options)
			if err != nil {
				t.Fatalf("interrupted fresh setup: %v", err)
			}
			plugin, ok := first.Check("plugin")
			if !ok || plugin.Status != CheckFailed {
				t.Fatalf("interrupted plugin check = %+v, want failed", plugin)
			}
			if _, err := os.Stat(codexSetupTransactionPath(configPath)); err != nil {
				t.Fatalf("fresh interruption did not retain recovery transaction: %v", err)
			}
			if customPartial {
				customPath := filepath.Join(installedPlugin, "custom-user-file")
				if err := os.WriteFile(customPath, []byte("keep me\n"), 0644); err != nil {
					t.Fatalf("customize partial cache: %v", err)
				}
				second, err := InstallWithOptions("codex", options)
				if err != nil {
					t.Fatalf("classify modified partial cache: %v", err)
				}
				plugin, ok = second.Check("plugin")
				if !ok || plugin.Status != CheckPreserved || pluginAddCalls != 1 {
					t.Fatalf("modified partial check = %+v, plugin add calls = %d; want preserved without retry", plugin, pluginAddCalls)
				}
				assertFileBytes(t, customPath, []byte("keep me\n"))
				return
			}

			second, err := InstallWithOptions("codex", options)
			if err != nil {
				t.Fatalf("recover generated partial cache: %v", err)
			}
			plugin, ok = second.Check("plugin")
			if !ok || plugin.Status != CheckReady || pluginAddCalls != 2 {
				t.Fatalf("recovered partial check = %+v, plugin add calls = %d; want ready after one retry", plugin, pluginAddCalls)
			}
			if _, err := os.Stat(codexSetupTransactionPath(configPath)); !os.IsNotExist(err) {
				t.Fatalf("completed fresh setup left recovery transaction behind: %v", err)
			}
		})
	}
}

func TestInstallCodexDoesNotAdoptOrphanedLegacyFile(t *testing.T) {
	resetSetupSeams(t)
	home := useTestHome(t)
	osExecutable = func() (string, error) { return "/usr/local/bin/engram", nil }
	orphanPath := filepath.Join(home, ".codex", "engram-instructions.md")
	if err := os.MkdirAll(filepath.Dir(orphanPath), 0755); err != nil {
		t.Fatalf("create Codex directory: %v", err)
	}
	if err := os.WriteFile(orphanPath, []byte(memoryProtocolMarkdown), 0644); err != nil {
		t.Fatalf("write orphaned legacy file: %v", err)
	}
	stubVerifiedCodexCLI(t, false)

	result, err := InstallWithOptions("codex", InstallOptions{Development: true})
	if err != nil {
		t.Fatalf("setup with orphaned legacy file: %v", err)
	}
	if !slices.Contains(result.Preserved, "model_instructions_file") {
		t.Fatalf("preserved state = %v, want orphaned model instructions", result.Preserved)
	}
	config, err := os.ReadFile(filepath.Join(home, ".codex", "config.toml"))
	if err != nil {
		t.Fatalf("read Codex config: %v", err)
	}
	if strings.Contains(string(config), "model_instructions_file") {
		t.Fatalf("orphaned legacy file was adopted into config:\n%s", config)
	}
	assertFileBytes(t, orphanPath, []byte(memoryProtocolMarkdown))
}

func TestVerifyCodexMarketplaceAssetsRejectsDirtyTree(t *testing.T) {
	resetSetupSeams(t)
	gitStatusFn = func(string) ([]byte, error) {
		return []byte(" M plugin/codex/hooks/hooks.json\n"), nil
	}
	if err := verifyCodexMarketplaceAssets("/tmp/marketplace"); err == nil || !strings.Contains(err.Error(), "differs from verified commit") {
		t.Fatalf("dirty marketplace verification error = %v", err)
	}
}
