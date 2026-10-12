package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/diagnostic"
	"github.com/yersonargotev/engram/internal/store"
)

func isSessionIdentityRecoveryCommand(args []string) bool {
	if len(args) < 2 || args[0] != "doctor" || args[1] != "repair" {
		return false
	}
	for _, arg := range args[2:] {
		if arg == "--source-id" || arg == "--canonical-id" {
			return true
		}
	}
	return false
}

func cmdSessionIdentityRecovery(cfg store.Config, request store.SessionIdentityRecoveryRequest, mode diagnostic.RepairMode, token string, yes, jsonMode bool) {
	fail := func(err error) {
		code := "identity_recovery_failed"
		var rejected *store.SessionIdentityRecoveryError
		if errors.As(err, &rejected) {
			code = rejected.Code
		}
		failCLI(jsonMode, code, err.Error(), nil)
	}
	if mode == diagnostic.RepairModeApply && token == "" {
		failCLI(jsonMode, "preview_required", "--apply requires --preview-token from the reviewed plan", nil)
		return
	}
	if mode != diagnostic.RepairModeApply && (token != "" || yes) {
		failCLI(jsonMode, "invalid_recovery_arguments", "--preview-token and --yes apply only to --apply", nil)
		return
	}
	s, err := store.OpenSessionIdentityRecovery(cfg)
	if err != nil {
		fail(err)
		return
	}
	defer s.Close()
	plan, err := s.PreviewSessionIdentityRecovery(request)
	if err != nil {
		fail(err)
		return
	}
	if mode != diagnostic.RepairModeApply {
		writeDoctorRepairJSON(plan)
		return
	}
	if plan.PreviewToken != token {
		failCLI(jsonMode, "stale_preview", "targets changed; obtain and review a new preview", nil)
		return
	}
	if !yes {
		if jsonMode {
			failCLI(true, "confirmation_required", "identity recovery requires --yes in JSON mode", map[string]any{"preview": plan})
			return
		}
		writeDoctorRepairJSON(plan)
		fmt.Fprint(os.Stderr, "Apply this session identity recovery? [y/N] ")
		var answer string
		_, _ = scanInputLine(&answer)
		answer = strings.ToLower(strings.TrimSpace(answer))
		if answer != "y" && answer != "yes" {
			fmt.Fprintln(os.Stderr, "Aborted.")
			return
		}
	}
	result, err := s.ApplySessionIdentityRecovery(request, token)
	if err != nil {
		fail(err)
		return
	}
	writeDoctorRepairJSON(result)
}
