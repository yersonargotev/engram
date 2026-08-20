package main

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func cmdAdmissionShadow(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(3, jsonMode, admissionFlagOptions{Project: true, Session: true})
	if !proceed {
		return
	}
	project := flags.Project
	sessionID := flags.SessionID
	jsonMode = flags.JSONMode
	if project == "" || sessionID == "" {
		failCLI(jsonMode, "invalid_arguments", "admission shadow requires --project and --session", nil)
		return
	}
	project, _ = store.NormalizeProject(project)
	memoryStore := openAdmissionProjectStore(cfg, project, jsonMode)
	if memoryStore == nil {
		return
	}
	defer memoryStore.Close()

	result, err := memoryops.New(memoryStore).RunAdmissionShadow(memoryops.AdmissionShadowInput{Project: project, SessionID: sessionID})
	if err != nil {
		failAdmissionSessionOperation(jsonMode, err, sessionID, project, "admission_shadow_failed")
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	renderAdmissionShadow(result)
}

func cmdAdmissionReview(cfg store.Config, jsonMode bool) {
	if len(os.Args) < 4 {
		failCLI(jsonMode, "invalid_arguments", "admission review requires list or mark", nil)
		return
	}
	switch os.Args[3] {
	case "list":
		cmdAdmissionReviewList(cfg, jsonMode)
	case "mark":
		cmdAdmissionReviewMark(cfg, jsonMode)
	case "help", "--help", "-h":
		printAdmissionUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown admission review command %q", os.Args[3]), nil)
	}
}

func cmdAdmissionReviewList(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(4, jsonMode, admissionFlagOptions{Project: true})
	if !proceed {
		return
	}
	project := flags.Project
	jsonMode = flags.JSONMode
	if project == "" {
		failCLI(jsonMode, "invalid_arguments", "admission review list requires --project", nil)
		return
	}
	project, _ = store.NormalizeProject(project)
	memoryStore := openAdmissionProjectStore(cfg, project, jsonMode)
	if memoryStore == nil {
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).ListAdmissionReviews(memoryops.AdmissionReviewListInput{Project: project})
	if err != nil {
		failCLI(jsonMode, "admission_review_list_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	renderAdmissionReviewList(result)
}

func cmdAdmissionReviewMark(cfg store.Config, jsonMode bool) {
	if len(os.Args) >= 5 && isAdmissionHelpArgument(os.Args[4]) {
		printAdmissionUsage()
		return
	}
	if len(os.Args) < 5 || strings.HasPrefix(os.Args[4], "--") || strings.TrimSpace(os.Args[4]) == "" {
		failCLI(jsonMode, "invalid_arguments", "admission review mark requires PROPOSAL_ID", nil)
		return
	}
	proposalID := strings.TrimSpace(os.Args[4])
	verdict := ""
	note := ""
	var unsupported *bool
	var privacyLeak *bool
	unsupportedSet := false
	unsupportedCleared := false
	privacyLeakSet := false
	privacyLeakCleared := false
	seenVerdict := false
	seenNote := false
	for index := 5; index < len(os.Args); index++ {
		switch os.Args[index] {
		case "--json":
			jsonMode = true
		case "--verdict":
			if seenVerdict || index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--verdict requires one value", nil)
				return
			}
			seenVerdict = true
			verdict = strings.ToLower(strings.TrimSpace(os.Args[index+1]))
			index++
		case "--note":
			if seenNote || index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
				failCLI(jsonMode, "invalid_arguments", "--note requires one value", nil)
				return
			}
			seenNote = true
			note = os.Args[index+1]
			index++
		case "--unsupported":
			if unsupportedCleared {
				failCLI(jsonMode, "invalid_arguments", "--unsupported cannot be combined with --clear-unsupported", nil)
				return
			}
			unsupportedSet = true
			value := true
			unsupported = &value
		case "--clear-unsupported":
			if unsupportedSet {
				failCLI(jsonMode, "invalid_arguments", "--clear-unsupported cannot be combined with --unsupported", nil)
				return
			}
			unsupportedCleared = true
			value := false
			unsupported = &value
		case "--privacy-leak":
			if privacyLeakCleared {
				failCLI(jsonMode, "invalid_arguments", "--privacy-leak cannot be combined with --clear-privacy-leak", nil)
				return
			}
			privacyLeakSet = true
			value := true
			privacyLeak = &value
		case "--clear-privacy-leak":
			if privacyLeakSet {
				failCLI(jsonMode, "invalid_arguments", "--clear-privacy-leak cannot be combined with --privacy-leak", nil)
				return
			}
			privacyLeakCleared = true
			value := false
			privacyLeak = &value
		case "help", "--help", "-h":
			printAdmissionUsage()
			return
		default:
			admissionUnknownArgument(jsonMode, os.Args[index])
			return
		}
	}
	if verdict != string(memoryops.AdmissionAdmit) && verdict != string(memoryops.AdmissionReview) && verdict != string(memoryops.AdmissionReject) {
		failCLI(jsonMode, "invalid_arguments", "admission review mark requires --verdict admit|review|reject", nil)
		return
	}
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).MarkAdmissionReview(memoryops.AdmissionReviewMarkInput{
		ProposalID:  proposalID,
		Verdict:     memoryops.AdmissionRecommendation(verdict),
		Note:        note,
		Unsupported: unsupported,
		PrivacyLeak: privacyLeak,
	})
	if err != nil {
		if errors.Is(err, store.ErrAdmissionShadowProposalNotFound) {
			failCLI(jsonMode, "unknown_admission_proposal", err.Error(), map[string]any{"proposal_id": proposalID})
			return
		}
		failCLI(jsonMode, "admission_review_mark_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	if result.AlreadyRecorded {
		fmt.Printf("Admission correction already recorded: %s\n", result.Review.ID)
	} else {
		fmt.Printf("Admission correction recorded: %s\n", result.Review.ID)
	}
}

func cmdAdmissionMetrics(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(3, jsonMode, admissionFlagOptions{Project: true})
	if !proceed {
		return
	}
	project := flags.Project
	jsonMode = flags.JSONMode
	if project == "" {
		failCLI(jsonMode, "invalid_arguments", "admission metrics requires --project", nil)
		return
	}
	project, _ = store.NormalizeProject(project)
	memoryStore := openAdmissionProjectStore(cfg, project, jsonMode)
	if memoryStore == nil {
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).AdmissionMetrics(memoryops.AdmissionMetricsInput{Project: project})
	if err != nil {
		failCLI(jsonMode, "admission_metrics_failed", err.Error(), nil)
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	renderAdmissionMetrics(result)
}

func isAdmissionHelpArgument(argument string) bool {
	switch strings.ToLower(strings.TrimSpace(argument)) {
	case "help", "--help", "-h":
		return true
	default:
		return false
	}
}

func openAdmissionProjectStore(cfg store.Config, project string, jsonMode bool) *store.Store {
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return nil
	}
	exists, err := memoryStore.ProjectExists(project)
	if err != nil {
		memoryStore.Close()
		failCLI(jsonMode, "project_resolution_failed", err.Error(), nil)
		return nil
	}
	if !exists {
		available, listErr := memoryStore.ListProjectNames()
		memoryStore.Close()
		if listErr != nil {
			failCLI(jsonMode, "project_resolution_failed", listErr.Error(), nil)
			return nil
		}
		failCLI(jsonMode, "unknown_project", fmt.Sprintf("unknown project: %s", project), map[string]any{"available_projects": available})
		return nil
	}
	return memoryStore
}

func failAdmissionSessionOperation(jsonMode bool, err error, sessionID, project, fallbackCode string) {
	if errors.Is(err, memoryops.ErrAdmissionSessionNotFound) {
		failCLI(jsonMode, "unknown_session", err.Error(), map[string]any{"session_id": sessionID})
		return
	}
	if errors.Is(err, memoryops.ErrAdmissionSessionProjectMismatch) {
		details := map[string]any{"session_id": sessionID, "requested_project": project}
		var mismatch *memoryops.AdmissionSessionProjectMismatchError
		if errors.As(err, &mismatch) {
			details["session_project"] = mismatch.SessionProject
		}
		failCLI(jsonMode, "session_project_mismatch", err.Error(), details)
		return
	}
	failCLI(jsonMode, fallbackCode, err.Error(), nil)
}

func admissionUnknownArgument(jsonMode bool, argument string) {
	if strings.HasPrefix(argument, "--") {
		failCLI(jsonMode, "unknown_flag", fmt.Sprintf("unknown flag %s", argument), nil)
		return
	}
	failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unexpected admission argument %q", argument), nil)
}

func renderAdmissionShadow(result *memoryops.AdmissionShadowResult) {
	fmt.Printf("Admission shadow run %s (retained locally; no Memories were written)\n", result.Run.ID)
	fmt.Printf("Project: %s\n", result.Run.Project)
	fmt.Printf("Session: %s\n", result.Run.SessionID)
	fmt.Printf("Evidence: %d items, %d bytes\n", result.Run.IncludedItems, result.Run.IncludedContentBytes)
	for index, proposal := range result.Proposals {
		fmt.Printf("\n%d. [%s] %s\n", index+1, proposal.Recommendation, proposal.Title)
		fmt.Printf("   Proposal: %s\n", proposal.ID)
	}
	if len(result.Proposals) == 0 {
		fmt.Println("No Memory proposals were retained.")
	}
}

func renderAdmissionReviewList(result *memoryops.AdmissionReviewListResult) {
	fmt.Printf("Admission review queue for %s\n", result.Project)
	if len(result.Proposals) == 0 {
		fmt.Println("No pending Memory proposals.")
		return
	}
	for _, run := range result.Runs {
		fmt.Printf("Run %s: session %s; evidence %s; generator %s; Policy: %s; %d evidence item(s)\n",
			run.ID, run.SessionID, run.EvidenceVersion, run.GeneratorVersion,
			run.PolicyVersion, run.IncludedItems)
		if len(run.DiagnosticCodes) > 0 {
			fmt.Printf("  Diagnostics: %s\n", strings.Join(run.DiagnosticCodes, ", "))
		}
	}
	for index, proposal := range result.Proposals {
		fmt.Printf("\n%d. [%s] %s\n", index+1, proposal.Recommendation, proposal.Title)
		fmt.Printf("   Content: %s\n", proposal.Content)
		fmt.Printf("   Category: %s; protected: %t; type: %s; scope: %s\n",
			proposal.Category, proposal.Protected, proposal.Type, proposal.Scope)
		fmt.Printf("   Proposal reasons: %s\n", strings.Join(proposal.ProposalReasonCodes, ", "))
		fmt.Printf("   Assessment reasons: %s\n", strings.Join(proposal.AssessmentReasonCodes, ", "))
		fmt.Printf("   Evidence: %s\n", strings.Join(proposal.EvidenceRefs, ", "))
		fmt.Printf("   Proposal: %s; run: %s\n", proposal.ID, proposal.RunID)
	}
}

func renderAdmissionMetrics(result *memoryops.AdmissionMetricsResult) {
	fmt.Printf("Admission metrics for %s\n", result.Project)
	fmt.Printf("Runs: %d; proposals: %d; review events: %d; reviewed: %d; pending: %d\n",
		result.RunCount, result.ProposalCount, result.ReviewCount,
		result.ReviewedProposalCount, result.PendingProposalCount)
	fmt.Printf("Agreement: %d; disagreement: %d\n", result.AgreementCount, result.DisagreementCount)
	fmt.Printf("Protected false rejects: %d\n", result.ProtectedFalseRejectCount)
	fmt.Printf("Unsupported: %d; privacy leaks: %d\n", result.UnsupportedCount, result.PrivacyLeakCount)
	fmt.Printf("Reason-coded proposals: %d/%d\n", result.ReasonCodedProposalCount, result.ProposalCount)
	renderAdmissionMetricMap("Policy versions", result.ByPolicyVersion)
	renderAdmissionMetricMap("Recommendations", result.ByRecommendation)
	renderAdmissionMetricMap("Categories", result.ByCategory)
	renderAdmissionMetricMap("Human verdicts", result.ByHumanVerdict)
	renderAdmissionMetricMap("Reason codes", result.ByReasonCode)
	renderAdmissionMetricMap("Protected false rejects by category", result.ProtectedFalseRejectsByCategory)
	renderAdmissionMetricMap("Protected false rejects by reason code", result.ProtectedFalseRejectsByReasonCode)
	fmt.Printf("Automatic reject gate blocked: %t\n", result.AutomaticRejectGateBlocked)
	fmt.Printf("Automatic promotion gate blocked: %t\n", result.AutomaticPromotionGateBlocked)
}

func renderAdmissionMetricMap(label string, values map[string]int) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	fmt.Printf("%s: %s\n", label, strings.Join(parts, ", "))
}
