package cloud

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestClientConfigRoundTripAndEnvironmentResolution(t *testing.T) {
	dir := t.TempDir()
	if _, err := SaveClientServer(dir, " https://cloud.example.test/path "); err != nil {
		t.Fatalf("save server: %v", err)
	}
	if err := saveClientConfigForTest(dir, ClientConfig{ServerURL: "https://cloud.example.test/path", Token: "persisted-token"}); err != nil {
		t.Fatalf("save token: %v", err)
	}

	t.Setenv("ENGRAM_CLOUD_SERVER", "https://env.example.test")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "env-token")
	got, err := ResolveClientConfig(dir)
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if got.ServerURL != "https://env.example.test" || got.Token != "env-token" {
		t.Fatalf("unexpected resolved config: %+v", got)
	}
}

func TestSaveClientServerPreservesToken(t *testing.T) {
	dir := t.TempDir()
	if err := saveClientConfigForTest(dir, ClientConfig{ServerURL: "https://old.example.test", Token: "keep-me"}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	if _, err := SaveClientServer(dir, "http://new.example.test"); err != nil {
		t.Fatalf("save server: %v", err)
	}
	got, err := LoadClientConfig(dir)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.ServerURL != "http://new.example.test" || got.Token != "keep-me" {
		t.Fatalf("token was not preserved: %+v", got)
	}
}

func TestValidateServerURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{"http", " https://cloud.example.test/path ", "https://cloud.example.test/path", true},
		{"query", "https://cloud.example.test/path?x=1", "", false},
		{"empty query", "https://cloud.example.test/path?", "", false},
		{"fragment", "https://cloud.example.test/path#x", "", false},
		{"userinfo", "https://review-user:review-password@cloud.example.test", "", false},
		{"scheme", "ftp://cloud.example.test", "", false},
		{"host", "https:///path", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateServerURL(tt.raw)
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("ValidateServerURL(%q) = %q, %v; want %q, ok=%v", tt.raw, got, err, tt.want, tt.ok)
			}
		})
	}
}

func TestLoadClientConfigMissing(t *testing.T) {
	got, err := LoadClientConfig(t.TempDir())
	if err != nil || got != nil {
		t.Fatalf("missing config = %#v, %v; want nil, nil", got, err)
	}
}

func TestClientConfigRequiresAbsoluteDataDir(t *testing.T) {
	if _, err := LoadClientConfig(""); err == nil {
		t.Fatal("empty data directory should be rejected")
	}
	if err := SaveClientConfig("relative", ClientConfig{}); err == nil {
		t.Fatal("relative data directory should be rejected")
	}
}

func TestResolveClientConfigSupportsEnvironmentWithoutDataDir(t *testing.T) {
	t.Setenv("ENGRAM_CLOUD_SERVER", "https://env.example.test")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "env-token")
	got, err := ResolveClientConfig("")
	if err != nil {
		t.Fatalf("resolve environment-only config: %v", err)
	}
	if got.ServerURL != "https://env.example.test" || got.Token != "env-token" {
		t.Fatalf("environment-only config = %+v", got)
	}
}

func TestSaveClientConfigUsesPrivatePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	dir := t.TempDir()
	if err := SaveClientConfig(dir, ClientConfig{Token: "secret"}); err != nil {
		t.Fatalf("save config: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, "cloud.json"))
	if err != nil {
		t.Fatalf("stat config: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("cloud.json permissions = %o, want 600", got)
	}
}

func TestSaveClientConfigFallsBackWhenRenameCannotReplaceExistingFile(t *testing.T) {
	dir := t.TempDir()
	if err := SaveClientConfig(dir, ClientConfig{ServerURL: "https://old.example.test", Token: "keep"}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	originalRename := renameClientConfigFile
	first := true
	renameClientConfigFile = func(oldPath, newPath string) error {
		if first {
			first = false
			return errors.New("destination exists")
		}
		return os.Rename(oldPath, newPath)
	}
	t.Cleanup(func() { renameClientConfigFile = originalRename })

	if err := SaveClientConfig(dir, ClientConfig{ServerURL: "https://new.example.test", Token: "keep"}); err != nil {
		t.Fatalf("replace existing config: %v", err)
	}
	got, err := LoadClientConfig(dir)
	if err != nil {
		t.Fatalf("load replacement: %v", err)
	}
	if got.ServerURL != "https://new.example.test" || got.Token != "keep" {
		t.Fatalf("replacement config = %+v", got)
	}
}

func saveClientConfigForTest(dir string, cfg ClientConfig) error {
	b := []byte(`{"server_url":"` + cfg.ServerURL + `","token":"` + cfg.Token + `"}`)
	return os.WriteFile(filepath.Join(dir, "cloud.json"), b, 0o644)
}
