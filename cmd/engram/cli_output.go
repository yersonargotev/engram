package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type cliErrorEnvelope struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeCLIJSON(value any) error {
	b, err := jsonMarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func failCLI(jsonMode bool, code, message string, details map[string]any) {
	if jsonMode {
		b, err := json.Marshal(cliErrorEnvelope{Code: code, Message: message, Details: details})
		if err == nil {
			fmt.Fprintln(os.Stderr, string(b))
		} else {
			fmt.Fprintf(os.Stderr, `{"code":"internal_error","message":%q}`+"\n", err.Error())
		}
	} else {
		fmt.Fprintf(os.Stderr, "error: %s\n", message)
	}
	exitFunc(1)
}
