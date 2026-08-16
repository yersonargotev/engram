package cloud

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// ClientConfig contains the persisted client-side cloud endpoint and bearer
// token. The JSON field names are part of the on-disk compatibility contract.
type ClientConfig struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token"`
}

var renameClientConfigFile = os.Rename

// LoadClientConfig reads <dataDir>/cloud.json. A missing file is equivalent to
// an unconfigured client and returns (nil, nil).
func LoadClientConfig(dataDir string) (*ClientConfig, error) {
	path, err := clientConfigPath(dataDir)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cfg ClientConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ResolveClientConfig overlays non-empty environment variables on the
// persisted client configuration. Environment values always take precedence.
func ResolveClientConfig(dataDir string) (*ClientConfig, error) {
	var cfg *ClientConfig
	if strings.TrimSpace(dataDir) != "" {
		var err error
		cfg, err = LoadClientConfig(dataDir)
		if err != nil {
			return nil, err
		}
	}
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	if value := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_SERVER")); value != "" {
		cfg.ServerURL = value
	}
	if value := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN")); value != "" {
		cfg.Token = value
	}
	return cfg, nil
}

// SaveClientServer updates the persisted endpoint while preserving any token
// already stored in cloud.json. The replacement is written in the same
// directory and renamed into place so readers never observe a partial JSON
// document.
func SaveClientServer(dataDir, rawURL string) (*ClientConfig, error) {
	serverURL, err := ValidateServerURL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := LoadClientConfig(dataDir)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &ClientConfig{}
	}
	cfg.ServerURL = serverURL
	if err := SaveClientConfig(dataDir, *cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// SaveClientConfig persists a complete client configuration using the same
// safe replacement semantics as SaveClientServer.
func SaveClientConfig(dataDir string, cfg ClientConfig) error {
	path, err := clientConfigPath(dataDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(dataDir, ".cloud.json-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceClientConfigFile(tmpName, path); err != nil {
		return fmt.Errorf("replace cloud config: %w", err)
	}
	return nil
}

// replaceClientConfigFile uses a direct atomic rename where the platform
// supports replacing an existing destination. If that fails because a target
// already exists (notably on Windows), it stages the previous file under the
// temp file's unique name and rolls back if installing the replacement fails.
func replaceClientConfigFile(tmpName, path string) error {
	if err := renameClientConfigFile(tmpName, path); err == nil {
		return nil
	} else if _, statErr := os.Stat(path); statErr != nil {
		return err
	}

	backupName := tmpName + ".previous"
	if err := renameClientConfigFile(path, backupName); err != nil {
		return err
	}
	if err := renameClientConfigFile(tmpName, path); err != nil {
		_ = renameClientConfigFile(backupName, path)
		return err
	}
	_ = os.Remove(backupName)
	return nil
}

func clientConfigPath(dataDir string) (string, error) {
	dataDir = strings.TrimSpace(dataDir)
	if dataDir == "" {
		return "", fmt.Errorf("cloud client data directory is required")
	}
	if !filepath.IsAbs(dataDir) {
		return "", fmt.Errorf("cloud client data directory must be absolute")
	}
	return filepath.Join(dataDir, "cloud.json"), nil
}

// ValidateServerURL accepts only absolute HTTP(S) URLs with a host and no
// query or fragment, matching the CLI cloud endpoint validation contract.
func ValidateServerURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "#") {
		return "", fmt.Errorf("fragment is not allowed")
	}
	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("scheme must be http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" || strings.TrimSpace(parsed.Hostname()) == "" {
		return "", fmt.Errorf("host is required")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("userinfo is not allowed")
	}
	if parsed.ForceQuery || strings.TrimSpace(parsed.RawQuery) != "" {
		return "", fmt.Errorf("query is not allowed")
	}
	if strings.TrimSpace(parsed.Fragment) != "" {
		return "", fmt.Errorf("fragment is not allowed")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
