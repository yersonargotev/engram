package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/memoryops"
	"github.com/yersonargotev/engram/internal/store"
)

func cmdAdmissionStudy(cfg store.Config, jsonMode bool) {
	if len(os.Args) < 4 {
		failCLI(jsonMode, "invalid_arguments", "admission study requires freeze or cleanup", nil)
		return
	}
	switch os.Args[3] {
	case "freeze":
		cmdAdmissionStudyFreeze(cfg, jsonMode)
	case "cleanup":
		cmdAdmissionStudyCleanup(cfg, jsonMode)
	case "help", "--help", "-h":
		printAdmissionUsage()
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown admission study command %q", os.Args[3]), nil)
	}
}

func cmdAdmissionStudyFreeze(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(4, jsonMode, admissionFlagOptions{Input: true})
	if !proceed {
		return
	}
	if flags.InputPath == "" {
		failCLI(flags.JSONMode, "invalid_arguments", "admission study freeze requires --input", nil)
		return
	}
	contract, err := readAdmissionStudyContract(flags.InputPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_study_contract", err.Error(), nil)
		return
	}
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(flags.JSONMode, "store_error", err.Error(), nil)
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).FreezeAdmissionStudy(memoryops.AdmissionStudyFreezeInput{Contract: contract})
	if err != nil {
		if errors.Is(err, store.ErrAdmissionStudyContractChanged) {
			failCLI(flags.JSONMode, "study_contract_changed", err.Error(), nil)
			return
		}
		failCLI(flags.JSONMode, "invalid_study_contract", err.Error(), nil)
		return
	}
	if flags.JSONMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	fmt.Printf("Admission study %s/%s frozen (%s)\n", result.Study.Contract.StudyID, result.Study.Contract.StudyVersion, result.Study.ContractHash)
}

func cmdAdmissionStudyCleanup(cfg store.Config, jsonMode bool) {
	flags, proceed := parseAdmissionFlags(4, jsonMode, admissionFlagOptions{StudySelector: true, Yes: true})
	if !proceed {
		return
	}
	if flags.StudyID == "" || flags.StudyVersion == "" {
		failCLI(flags.JSONMode, "invalid_arguments", "admission study cleanup requires --study and --study-version", nil)
		return
	}
	if !flags.Yes {
		failCLI(flags.JSONMode, "confirmation_required", "admission study cleanup requires --yes", nil)
		return
	}
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(flags.JSONMode, "store_error", err.Error(), nil)
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).CleanupAdmissionStudy(memoryops.AdmissionStudyCleanupInput{
		StudyID: flags.StudyID, StudyVersion: flags.StudyVersion,
	})
	if err != nil {
		failAdmissionStudyOperation(flags.JSONMode, err, "admission_study_cleanup_failed")
		return
	}
	if flags.JSONMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	fmt.Printf("Admission study %s/%s removed: %d run(s), %d proposal(s), %d review(s), %d omission(s)\n",
		result.StudyID, result.StudyVersion, result.RunCount, result.ProposalCount, result.ReviewCount, result.OmissionCount)
}

func cmdAdmissionOmission(cfg store.Config, jsonMode bool) {
	if len(os.Args) >= 5 && isAdmissionHelpArgument(os.Args[4]) {
		printAdmissionUsage()
		return
	}
	if len(os.Args) < 5 || os.Args[3] != "record" || strings.HasPrefix(os.Args[4], "--") {
		failCLI(jsonMode, "invalid_arguments", "admission omission requires record RUN_ID", nil)
		return
	}
	runID := strings.TrimSpace(os.Args[4])
	values := map[string]string{}
	for index := 5; index < len(os.Args); index++ {
		argument := os.Args[index]
		if argument == "--json" {
			jsonMode = true
			continue
		}
		if argument != "--reviewer" && argument != "--category" && argument != "--reason-code" && argument != "--annotation" {
			admissionUnknownArgument(jsonMode, argument)
			return
		}
		if _, seen := values[argument]; seen || index+1 >= len(os.Args) || strings.HasPrefix(os.Args[index+1], "--") {
			failCLI(jsonMode, "invalid_arguments", argument+" requires one value", nil)
			return
		}
		values[argument] = os.Args[index+1]
		index++
	}
	if runID == "" || values["--reviewer"] == "" || values["--category"] == "" || values["--reason-code"] == "" || values["--annotation"] == "" {
		failCLI(jsonMode, "invalid_arguments", "admission omission record requires --reviewer, --category, --reason-code, and --annotation", nil)
		return
	}
	memoryStore, err := storeNew(cfg)
	if err != nil {
		failCLI(jsonMode, "store_error", err.Error(), nil)
		return
	}
	defer memoryStore.Close()
	result, err := memoryops.New(memoryStore).RecordAdmissionStudyOmission(memoryops.AdmissionStudyOmissionInput{
		RunID: runID, ReviewerID: values["--reviewer"], Category: values["--category"],
		ReasonCode: values["--reason-code"], Annotation: values["--annotation"],
	})
	if err != nil {
		failAdmissionStudyOperation(jsonMode, err, "admission_study_omission_failed")
		return
	}
	if jsonMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	fmt.Printf("Admission omission recorded: %s\n", result.Omission.ID)
}

func readAdmissionStudyContract(path string) (store.AdmissionStudyContract, error) {
	var reader io.Reader
	var file *os.File
	if path == "-" {
		reader = os.Stdin
	} else {
		opened, err := os.Open(path)
		if err != nil {
			return store.AdmissionStudyContract{}, fmt.Errorf("open admission study contract: %w", err)
		}
		file = opened
		defer file.Close()
		reader = file
	}
	encoded, err := io.ReadAll(io.LimitReader(reader, maxAdmissionInputBytes+1))
	if err != nil {
		return store.AdmissionStudyContract{}, fmt.Errorf("read admission study contract: %w", err)
	}
	if len(encoded) > maxAdmissionInputBytes {
		return store.AdmissionStudyContract{}, fmt.Errorf("admission study contract JSON exceeds %d bytes", maxAdmissionInputBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var contract store.AdmissionStudyContract
	if err := decoder.Decode(&contract); err != nil {
		return store.AdmissionStudyContract{}, fmt.Errorf("decode admission study contract: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return store.AdmissionStudyContract{}, fmt.Errorf("decode admission study contract: multiple JSON values are not allowed")
	}
	return contract, nil
}

func failAdmissionStudyOperation(jsonMode bool, err error, fallbackCode string) {
	switch {
	case errors.Is(err, store.ErrAdmissionStudyNotFound):
		failCLI(jsonMode, "unknown_admission_study", err.Error(), nil)
	case errors.Is(err, store.ErrAdmissionShadowRunNotFound):
		failCLI(jsonMode, "unknown_admission_run", err.Error(), nil)
	case errors.Is(err, store.ErrAdmissionStudyMetadataMismatch), errors.Is(err, store.ErrAdmissionStudyContractChanged):
		failCLI(jsonMode, "invalid_study_metadata", err.Error(), nil)
	default:
		failCLI(jsonMode, fallbackCode, err.Error(), nil)
	}
}

func renderAdmissionStudyReviewList(result *memoryops.AdmissionStudyReviewListResult) {
	fmt.Printf("Admission study review queue for %s/%s (%s)\n", result.StudyID, result.StudyVersion, result.ReviewerID)
	for _, proposal := range result.Proposals {
		fmt.Printf("[%s] %s\n  Proposal: %s; run: %s\n", proposal.Recommendation, proposal.Title, proposal.ID, proposal.RunID)
	}
	if len(result.Proposals) == 0 {
		fmt.Println("No pending Memory proposals for this reviewer.")
	}
}

func renderAdmissionStudyMetrics(result *memoryops.AdmissionStudyMetricsResult) {
	fmt.Printf("Admission study metrics for %s/%s\n", result.StudyID, result.StudyVersion)
	fmt.Printf("Runs: %d; proposals: %d; review events: %d; omissions: %d\n",
		result.Counts.RunCount, result.Counts.ProposalCount, result.Counts.ReviewEventCount, result.Counts.OmissionCount)
	fmt.Printf("Sample sufficient: %t; go: %t; automatic admission enabled: %t\n",
		result.Sufficiency.Sufficient, result.Gates.Go, result.AutomaticAdmissionEnabled)
}
