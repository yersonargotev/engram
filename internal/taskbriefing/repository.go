package taskbriefing

import (
	"context"
	"io"
	"os/exec"
	"sort"
	"strings"
	"time"

	projectpkg "github.com/yersonargotev/engram/internal/project"
)

var (
	gitCommandOutput = runGitCommand
	gitTermsOutput   = runGitTermsCommand
)

type BaseSource string

const (
	BaseSourceExplicit      BaseSource = "explicit"
	BaseSourceUpstream      BaseSource = "upstream"
	BaseSourceRemoteDefault BaseSource = "remote_default"
)

type BaseResolution struct {
	Ref    string     `json:"ref"`
	Source BaseSource `json:"source"`
}

func inspectRepository(workingDirectory, explicitBase, selectedProject string) (string, RepositorySignals) {
	root, err := gitCommandOutput(workingDirectory, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", RepositorySignals{GitFailures: []SignalType{SignalBranch}}
	}
	root = strings.TrimSpace(root)
	detected := projectpkg.DetectProjectFull(root)
	repositoryProject := strings.TrimSpace(detected.Project)
	if repositoryProject != "" && !strings.EqualFold(repositoryProject, strings.TrimSpace(selectedProject)) {
		return repositoryProject, RepositorySignals{}
	}

	repository := RepositorySignals{}
	branch, err := gitCommandOutput(root, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		repository.GitFailures = append(repository.GitFailures, SignalBranch)
	} else {
		repository.Branch = strings.TrimSpace(branch)
	}

	base := resolveBranchBase(root, explicitBase, repository.Branch)
	affectedPathBase := "HEAD"
	if base == nil {
		repository.BaseUnresolved = true
	} else if mergeBase, err := gitCommandOutput(root, "merge-base", "HEAD", base.Ref); err != nil || strings.TrimSpace(mergeBase) == "" {
		repository.BaseUnresolved = true
	} else {
		repository.BaseResolution = base
		rangeSpec := strings.TrimSpace(mergeBase) + "..HEAD"
		affectedPathBase = strings.TrimSpace(mergeBase)
		acquireRepositoryTerms(&repository, root, SignalBranchDiff, CalibratedDefaults.DiffTermLimit, func(value string) {
			repository.BranchDiff = value
		}, "diff", "--no-ext-diff", "--no-textconv", "--unified=0", rangeSpec, "--")
		acquireRepositoryTerms(&repository, root, SignalCommitSubject, CalibratedDefaults.CommitTermLimit, func(value string) {
			repository.CommitSubjects = []string{value}
		}, "log", "--format=%s%x00", rangeSpec, "--")
	}

	acquireRepositoryTerms(&repository, root, SignalStagedDiff, CalibratedDefaults.DiffTermLimit, func(value string) {
		repository.StagedDiff = value
	}, "diff", "--cached", "--no-ext-diff", "--no-textconv", "--unified=0", "--")
	acquireRepositoryTerms(&repository, root, SignalUnstagedDiff, CalibratedDefaults.DiffTermLimit, func(value string) {
		repository.UnstagedDiff = value
	}, "diff", "--no-ext-diff", "--no-textconv", "--unified=0", "--")
	acquireRepositoryTerms(&repository, root, SignalAffectedPath, CalibratedDefaults.PathTermLimit, func(value string) {
		repository.AffectedPaths = []string{value}
	}, "diff", "--name-only", "-z", affectedPathBase, "--")
	acquireRepositoryTerms(&repository, root, SignalUntrackedPath, CalibratedDefaults.UntrackedTermLimit, func(value string) {
		repository.UntrackedPaths = []string{value}
	}, "ls-files", "--others", "--exclude-standard", "-z")
	return repositoryProject, repository
}

func acquireRepositoryTerms(repository *RepositorySignals, root string, signal SignalType, limit int, set func(string), args ...string) {
	value, omittedTerms, countComplete, err := gitTermsOutput(root, limit, args...)
	if err != nil {
		repository.GitFailures = append(repository.GitFailures, signal)
		return
	}
	set(value)
	if omittedTerms > 0 || !countComplete {
		analyzedTerms := len(normalizeTerms(value, limit))
		repository.AcquisitionTruncations = append(repository.AcquisitionTruncations, InputTruncation{
			Signal: signal, OmittedTerms: omittedTerms, TotalTerms: analyzedTerms + omittedTerms, AnalyzedTerms: analyzedTerms, CountComplete: countComplete,
		})
	}
}

func resolveBranchBase(root, explicitBase, currentBranch string) *BaseResolution {
	if ref := strings.TrimSpace(explicitBase); ref != "" {
		if gitRefExists(root, ref) {
			return &BaseResolution{Ref: ref, Source: BaseSourceExplicit}
		}
		return nil
	}
	if upstream, err := gitCommandOutput(root, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"); err == nil {
		if ref := strings.TrimSpace(upstream); ref != "" && !isSameBranchUpstream(root, ref, currentBranch) && gitRefExists(root, ref) {
			return &BaseResolution{Ref: ref, Source: BaseSourceUpstream}
		}
	}
	for _, remote := range repositoryRemotes(root) {
		defaultRef, err := gitCommandOutput(root, "symbolic-ref", "--quiet", "--short", "refs/remotes/"+remote+"/HEAD")
		if err != nil {
			continue
		}
		if ref := strings.TrimSpace(defaultRef); ref != "" && gitRefExists(root, ref) {
			return &BaseResolution{Ref: ref, Source: BaseSourceRemoteDefault}
		}
	}
	return nil
}

func isSameBranchUpstream(root, upstream, currentBranch string) bool {
	currentBranch = strings.TrimSpace(currentBranch)
	if currentBranch == "" {
		return false
	}
	remoteOutput, err := gitCommandOutput(root, "config", "--get", "branch."+currentBranch+".remote")
	if err != nil {
		return false
	}
	remote := strings.TrimSpace(remoteOutput)
	if remote == "." {
		return upstream == currentBranch
	}
	trackedBranch, found := strings.CutPrefix(upstream, remote+"/")
	return found && trackedBranch == currentBranch
}

func repositoryRemotes(root string) []string {
	output, err := gitCommandOutput(root, "remote")
	if err != nil {
		return nil
	}
	remotes := strings.Fields(output)
	sort.Strings(remotes)
	for index, remote := range remotes {
		if remote == "origin" && index > 0 {
			copy(remotes[1:index+1], remotes[:index])
			remotes[0] = remote
			break
		}
	}
	return remotes
}

func gitRefExists(root, ref string) bool {
	_, err := gitCommandOutput(root, "rev-parse", "--verify", ref+"^{commit}")
	return err == nil
}

func runGitCommand(directory string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	commandArgs := append([]string{"-C", directory}, args...)
	output, err := exec.CommandContext(ctx, "git", commandArgs...).Output()
	return string(output), err
}

func runGitTermsCommand(directory string, termLimit int, args ...string) (string, int, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	commandArgs := append([]string{"-C", directory}, args...)
	command := exec.CommandContext(ctx, "git", commandArgs...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return "", 0, false, err
	}
	if err := command.Start(); err != nil {
		return "", 0, false, err
	}

	terms, omitted, complete, scanErr := readBoundedGitTerms(stdout, termLimit)
	if scanErr != nil {
		cancel()
		_ = command.Wait()
		return "", 0, false, scanErr
	}
	if !complete {
		cancel()
		_ = command.Wait()
		return terms, omitted, false, nil
	}
	if err := command.Wait(); err != nil {
		return "", 0, false, err
	}
	return terms, omitted, true, nil
}

func readBoundedGitTerms(input io.Reader, termLimit int) (string, int, bool, error) {
	terms, omitted, complete, err := collectTerms(input, termLimit, CalibratedDefaults.GitInputByteLimit)
	return strings.Join(terms, " "), omitted, complete, err
}
