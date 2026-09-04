package plugin_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	agentPluginRoot          = "plugin/engram"
	agentPluginSchemaURL     = "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
	agentPluginMCPSchemaURL  = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
	agentPluginResolvedBin   = "./bin/engram"
	agentPluginSkillDirName  = "engram-memory"
	editorialMemorySkillFile = "skills/engram-memory/SKILL.md"
)

var agentPluginNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,62}[a-z0-9])?$`)

func agentPluginDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(repoRoot(t), filepath.FromSlash(agentPluginRoot))
}

func TestAgentPluginManifestConformsToV1(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(agentPluginDir(t), "plugin.json"))
	if err != nil {
		t.Fatalf("Agent Plugin manifest: %v", err)
	}

	var manifest map[string]any
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("plugin.json is not JSON: %v", err)
	}

	if got, ok := manifest["$schema"].(string); !ok || got != agentPluginSchemaURL {
		t.Errorf("plugin.json $schema = %v, want %q", manifest["$schema"], agentPluginSchemaURL)
	}

	name, _ := manifest["name"].(string)
	if name != "engram" {
		t.Errorf("plugin.json name = %q, want %q", name, "engram")
	}
	if strings.Contains(name, "--") || strings.Contains(name, "..") || !agentPluginNamePattern.MatchString(name) {
		t.Errorf("plugin.json name %q is not a valid Agent Plugins 1.0 name", name)
	}

	allowed := map[string]struct{}{
		"$schema":     {},
		"name":        {},
		"version":     {},
		"description": {},
		"author":      {},
		"homepage":    {},
		"repository":  {},
		"license":     {},
		"keywords":    {},
		"extensions":  {},
	}
	for key := range manifest {
		if _, ok := allowed[key]; !ok {
			t.Errorf("plugin.json has non-portable field %q", key)
		}
	}
}

func TestAgentPluginMCPLaunchesAgentProfileWithResolvedBinary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(agentPluginDir(t), "mcp.json"))
	if err != nil {
		t.Fatalf("Agent Plugin MCP config: %v", err)
	}

	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("mcp.json is not JSON: %v", err)
	}
	if len(cfg) != 2 {
		t.Errorf("mcp.json top-level fields = %d, want 2 ($schema and mcpServers)", len(cfg))
	}
	if got, ok := cfg["$schema"].(string); !ok || got != agentPluginMCPSchemaURL {
		t.Errorf("mcp.json $schema = %v, want %q", cfg["$schema"], agentPluginMCPSchemaURL)
	}

	servers, ok := cfg["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("mcp.json mcpServers must be an object")
	}
	server, ok := servers["engram"].(map[string]any)
	if !ok {
		t.Fatal("mcp.json must declare the engram server")
	}
	if got, _ := server["type"].(string); got != "stdio" {
		t.Errorf("engram MCP type = %q, want %q", got, "stdio")
	}
	command, _ := server["command"].(string)
	if command != agentPluginResolvedBin {
		t.Errorf("engram MCP command = %q, want resolved plugin-relative path %q", command, agentPluginResolvedBin)
	}
	if !strings.HasPrefix(command, "./") {
		t.Errorf("engram MCP command %q is not a plugin-relative resolved path", command)
	}

	args, ok := server["args"].([]any)
	if !ok {
		t.Fatal("engram MCP args must be an array")
	}
	gotArgs := make([]string, len(args))
	for i, arg := range args {
		gotArgs[i], _ = arg.(string)
	}
	wantArgs := []string{"mcp", "--tools=agent"}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Errorf("engram MCP args = %q, want %q", gotArgs, wantArgs)
	}
}

func TestAgentPluginSkillIsEditorialNotAFork(t *testing.T) {
	root := repoRoot(t)
	skillPath := filepath.Join(agentPluginDir(t), "skills", agentPluginSkillDirName, "SKILL.md")
	packaged, err := os.ReadFile(skillPath)
	if err != nil {
		t.Fatalf("packaged Agent Plugin skill: %v", err)
	}
	if dir := filepath.Base(filepath.Dir(skillPath)); dir != agentPluginSkillDirName {
		t.Fatalf("skill directory %q does not match %q", dir, agentPluginSkillDirName)
	}

	name := packagedSkillName(t, string(packaged))
	if name != agentPluginSkillDirName {
		t.Fatalf("packaged skill name %q does not match directory %q", name, agentPluginSkillDirName)
	}

	editorial, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(editorialMemorySkillFile)))
	if err != nil {
		t.Fatalf("editorial skill: %v", err)
	}
	if sha256Hex(packaged) != sha256Hex(editorial) {
		t.Fatal("packaged Agent Plugin skill is a fork; it must match skills/engram-memory/SKILL.md")
	}
}

func TestAgentPluginOmitsHooksAndRules(t *testing.T) {
	forbidden := []string{
		"hooks",
		"hooks.json",
		"rules",
		".cursor-plugin",
		"commands",
		"agents",
	}
	err := filepath.WalkDir(agentPluginDir(t), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(agentPluginDir(t), path)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		base := filepath.Base(path)
		for _, name := range forbidden {
			if base == name || rel == name {
				t.Errorf("portable Agent Plugin includes %s; hooks and rules stay out of this package", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk Agent Plugin package: %v", err)
	}
}

func packagedSkillName(t *testing.T, content string) string {
	t.Helper()
	if !strings.HasPrefix(content, "---\n") {
		t.Fatal("packaged Memory skill is missing YAML frontmatter")
	}
	body := content[len("---\n"):]
	end := strings.Index(body, "\n---")
	if end == -1 {
		t.Fatal("packaged Memory skill frontmatter is not closed")
	}
	for _, line := range strings.Split(body[:end], "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && strings.TrimSpace(key) == "name" {
			return strings.Trim(strings.TrimSpace(value), `"'`)
		}
	}
	t.Fatal("packaged Memory skill frontmatter is missing name")
	return ""
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
