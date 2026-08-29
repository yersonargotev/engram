package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/yersonargotev/engram/internal/activationstudy"
)

type activationStudyFlags struct {
	ContractPath string
	HashPath     string
	SourceRepo   string
	UserSkill    string
	AuthFile     string
	CodexBinary  string
	EventsPath   string
	OutputPath   string
	MarkdownPath string
	JSONMode     bool
}

func cmdActivationStudy() {
	jsonMode := hasArg("--json")
	if len(os.Args) < 3 {
		failCLI(jsonMode, "invalid_arguments", "activation-study requires verify, run, or analyze", nil)
		return
	}
	subcommand := strings.ToLower(strings.TrimSpace(os.Args[2]))
	switch subcommand {
	case "help", "--help", "-h":
		printActivationStudyUsage()
	case "verify":
		flags, proceed := parseActivationStudyFlags(subcommand, os.Args[3:], true)
		if proceed {
			cmdActivationStudyVerify(flags)
		}
	case "run":
		flags, proceed := parseActivationStudyFlags(subcommand, os.Args[3:], true)
		if proceed {
			cmdActivationStudyRun(flags)
		}
	case "analyze":
		flags, proceed := parseActivationStudyFlags(subcommand, os.Args[3:], false)
		if proceed {
			cmdActivationStudyAnalyze(flags)
		}
	default:
		failCLI(jsonMode, "invalid_arguments", fmt.Sprintf("unknown activation-study command %q", os.Args[2]), nil)
	}
}

func parseActivationStudyFlags(subcommand string, args []string, runtime bool) (activationStudyFlags, bool) {
	flags := activationStudyFlags{}
	set := flag.NewFlagSet("activation-study "+subcommand, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&flags.ContractPath, "contract", "", "frozen contract JSON")
	set.StringVar(&flags.HashPath, "contract-hash", "", "contract SHA-256 sidecar")
	set.BoolVar(&flags.JSONMode, "json", false, "emit JSON")
	if runtime {
		set.StringVar(&flags.SourceRepo, "source-repo", "", "Git source repository")
		set.StringVar(&flags.UserSkill, "user-skill", "", "frozen user skill directory")
		set.StringVar(&flags.AuthFile, "auth-file", "", "Codex auth file copied into disposable homes")
		set.StringVar(&flags.CodexBinary, "codex", "codex", "Codex executable")
	}
	if subcommand == "run" {
		set.StringVar(&flags.OutputPath, "output", "", "bounded event-set JSON output")
	}
	if subcommand == "analyze" {
		set.StringVar(&flags.EventsPath, "events", "", "bounded event-set JSON")
		set.StringVar(&flags.OutputPath, "output", "", "aggregate report JSON output")
		set.StringVar(&flags.MarkdownPath, "markdown-output", "", "aggregate Markdown report output")
	}
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			printActivationStudyUsage()
			return flags, false
		}
		code := "invalid_arguments"
		message := err.Error()
		if strings.Contains(err.Error(), "flag provided but not defined") {
			code = "unknown_flag"
			name := strings.TrimSpace(strings.TrimPrefix(err.Error(), "flag provided but not defined:"))
			message = "unknown activation-study flag " + "-" + name
		}
		failCLI(hasArg("--json"), code, message, nil)
		return flags, false
	}
	if set.NArg() > 0 {
		failCLI(flags.JSONMode, "invalid_arguments", fmt.Sprintf("unexpected activation-study argument %q", set.Arg(0)), nil)
		return flags, false
	}
	if flags.ContractPath == "" || flags.HashPath == "" {
		failCLI(flags.JSONMode, "invalid_arguments", "activation-study requires --contract and --contract-hash", nil)
		return flags, false
	}
	if runtime && (flags.SourceRepo == "" || flags.UserSkill == "" || flags.AuthFile == "") {
		failCLI(flags.JSONMode, "invalid_arguments", "activation-study verify/run requires --source-repo, --user-skill, and --auth-file", nil)
		return flags, false
	}
	if subcommand == "run" && flags.OutputPath == "" {
		failCLI(flags.JSONMode, "invalid_arguments", "activation-study run requires --output", nil)
		return flags, false
	}
	if subcommand == "analyze" && flags.EventsPath == "" {
		failCLI(flags.JSONMode, "invalid_arguments", "activation-study analyze requires --events", nil)
		return flags, false
	}
	return flags, true
}

func cmdActivationStudyVerify(flags activationStudyFlags) {
	study := loadActivationStudy(flags)
	report, err := study.Verify(context.Background(), activationRunOptions(flags))
	if err != nil {
		failCLI(flags.JSONMode, "activation_verification_failed", err.Error(), nil)
		return
	}
	if flags.JSONMode {
		if err := writeCLIJSON(report); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	fmt.Printf("Activation study %s/%s verified (%d fixtures; cleanup verified)\n", study.Contract.StudyID, study.Contract.StudyVersion, len(report.Fixtures))
}

func cmdActivationStudyRun(flags activationStudyFlags) {
	study := loadActivationStudy(flags)
	events, err := study.Run(context.Background(), activationRunOptions(flags))
	if err != nil {
		failCLI(flags.JSONMode, "activation_run_failed", err.Error(), nil)
		return
	}
	if err := activationstudy.WriteJSON(flags.OutputPath, events); err != nil {
		failCLI(flags.JSONMode, "output_error", err.Error(), nil)
		return
	}
	result := map[string]any{"study_id": events.StudyID, "study_version": events.StudyVersion, "contract_sha256": events.ContractSHA256, "records": len(events.Records), "cleanup_verified": events.Verification.CleanupVerified}
	if flags.JSONMode {
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
		return
	}
	fmt.Printf("Activation study %s/%s retained %d bounded cells; raw evidence removed\n", events.StudyID, events.StudyVersion, len(events.Records))
}

func cmdActivationStudyAnalyze(flags activationStudyFlags) {
	study := loadActivationStudy(flags)
	events, err := activationstudy.ReadEventSet(flags.EventsPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_event_set", err.Error(), nil)
		return
	}
	report, err := study.Analyze(events)
	if err != nil {
		failCLI(flags.JSONMode, "activation_analysis_failed", err.Error(), nil)
		return
	}
	if flags.OutputPath != "" {
		if err := activationstudy.WriteJSON(flags.OutputPath, report); err != nil {
			failCLI(flags.JSONMode, "output_error", err.Error(), nil)
			return
		}
	}
	if flags.MarkdownPath != "" {
		if err := activationstudy.WriteMarkdown(flags.MarkdownPath, report); err != nil {
			failCLI(flags.JSONMode, "output_error", err.Error(), nil)
			return
		}
	}
	if flags.OutputPath == "" && flags.MarkdownPath == "" {
		if flags.JSONMode {
			if err := writeCLIJSON(report); err != nil {
				failCLI(true, "output_error", err.Error(), nil)
			}
			return
		}
		fmt.Print(activationstudy.RenderMarkdown(report))
		return
	}
	if flags.JSONMode {
		result := map[string]any{"study_id": report.StudyID, "study_version": report.StudyVersion, "contract_sha256": report.ContractSHA256, "retained": report.SampleSize.Retained, "integration_failures": report.SampleSize.IntegrationFailures}
		if err := writeCLIJSON(result); err != nil {
			failCLI(true, "output_error", err.Error(), nil)
		}
	}
}

func loadActivationStudy(flags activationStudyFlags) *activationstudy.Study {
	study, err := activationstudy.Load(flags.ContractPath, flags.HashPath)
	if err != nil {
		failCLI(flags.JSONMode, "invalid_activation_contract", err.Error(), nil)
		return nil
	}
	return study
}

func activationRunOptions(flags activationStudyFlags) activationstudy.RunOptions {
	return activationstudy.RunOptions{
		SourceRepo: flags.SourceRepo, UserSkill: flags.UserSkill, AuthFile: flags.AuthFile, CodexBinary: flags.CodexBinary, OutputPath: flags.OutputPath,
	}
}

func printActivationStudyUsage() {
	fmt.Println(`usage: engram activation-study verify|run|analyze [options]

  verify   --contract FILE --contract-hash FILE --source-repo DIR
           --user-skill DIR --auth-file FILE [--codex PATH] [--json]
  run      --contract FILE --contract-hash FILE --source-repo DIR
           --user-skill DIR --auth-file FILE --output FILE [--codex PATH] [--json]
  analyze  --contract FILE --contract-hash FILE --events FILE
           [--output FILE] [--markdown-output FILE] [--json]`)
}
