package recallstudy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("multiple JSON values are not allowed")
	}
	return nil
}

func readStrictJSON(path string, maximum int64, destination any) error {
	raw, err := readBoundedFile(path, maximum)
	if err != nil {
		return err
	}
	return decodeStrictJSON(raw, destination)
}
