package recallstudy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func WritePrivateJSON(path string, value any) error {
	return writeJSON(path, value, 0o600)
}

func WriteSharedJSON(path string, value any) error {
	return writeJSON(path, value, 0o644)
}

func writeJSON(path string, value any, mode os.FileMode) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("Recall study artifact path is required")
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".recall-study-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	remove := true
	defer func() {
		_ = temporary.Close()
		if remove {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	remove = false
	return nil
}
