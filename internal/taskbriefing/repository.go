package taskbriefing

import (
	"bufio"
	"context"
	"os/exec"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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
	if base == nil {
		repository.BaseUnresolved = true
		return repositoryProject, repository
	}
	mergeBase, err := gitCommandOutput(root, "merge-base", "HEAD", base.Ref)
	if err != nil || strings.TrimSpace(mergeBase) == "" {
		repository.BaseUnresolved = true
		return repositoryProject, repository
	}
	repository.BaseResolution = base
	rangeSpec := strings.TrimSpace(mergeBase) + "..HEAD"

	if diff, omittedTerms, commandErr := gitTermsOutput(root, CalibratedDefaults.DiffTermLimit, "diff", "--no-ext-diff", "--no-textconv", "--unified=0", rangeSpec, "--"); commandErr != nil {
		repository.GitFailures = append(repository.GitFailures, SignalBranchDiff)
	} else {
		repository.BranchDiff = diff
		if omittedTerms > 0 {
			repository.AcquisitionTruncations = append(repository.AcquisitionTruncations, InputTruncation{Signal: SignalBranchDiff, OmittedTerms: omittedTerms})
		}
	}
	if paths, omittedTerms, commandErr := gitTermsOutput(root, CalibratedDefaults.PathTermLimit, "diff", "--name-only", "-z", rangeSpec, "--"); commandErr != nil {
		repository.GitFailures = append(repository.GitFailures, SignalAffectedPath)
	} else {
		repository.AffectedPaths = []string{paths}
		if omittedTerms > 0 {
			repository.AcquisitionTruncations = append(repository.AcquisitionTruncations, InputTruncation{Signal: SignalAffectedPath, OmittedTerms: omittedTerms})
		}
	}
	if subjects, omittedTerms, commandErr := gitTermsOutput(root, CalibratedDefaults.CommitTermLimit, "log", "--format=%s%x00", rangeSpec, "--"); commandErr != nil {
		repository.GitFailures = append(repository.GitFailures, SignalCommitSubject)
	} else {
		repository.CommitSubjects = []string{subjects}
		if omittedTerms > 0 {
			repository.AcquisitionTruncations = append(repository.AcquisitionTruncations, InputTruncation{Signal: SignalCommitSubject, OmittedTerms: omittedTerms})
		}
	}
	return repositoryProject, repository
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

func runGitTermsCommand(directory string, termLimit int, args ...string) (string, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	commandArgs := append([]string{"-C", directory}, args...)
	command := exec.CommandContext(ctx, "git", commandArgs...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return "", 0, err
	}
	if err := command.Start(); err != nil {
		return "", 0, err
	}

	retained := make([]string, 0, termLimit)
	seen := make(map[string]struct{}, termLimit)
	omitted := 0
	scanner := bufio.NewScanner(stdout)
	scanner.Split(scanAlphanumericToken)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for scanner.Scan() {
		term := strings.ToLower(scanner.Text())
		if shouldIgnoreTerm(term) {
			continue
		}
		if _, exists := seen[term]; exists {
			continue
		}
		if len(retained) < termLimit {
			seen[term] = struct{}{}
			retained = append(retained, term)
			continue
		}
		omitted++
	}
	if scanErr := scanner.Err(); scanErr != nil {
		cancel()
		_ = command.Wait()
		return "", 0, scanErr
	}
	if err := command.Wait(); err != nil {
		return "", 0, err
	}
	return strings.Join(retained, " "), omitted, nil
}

func scanAlphanumericToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0
	for start < len(data) {
		r, size := utf8.DecodeRune(data[start:])
		if r == utf8.RuneError && size == 1 && !atEOF && !utf8.FullRune(data[start:]) {
			return 0, nil, nil
		}
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			break
		}
		start += size
	}
	for end := start; end < len(data); {
		r, size := utf8.DecodeRune(data[end:])
		if r == utf8.RuneError && size == 1 && !atEOF && !utf8.FullRune(data[end:]) {
			return 0, nil, nil
		}
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			return end + size, data[start:end], nil
		}
		end += size
	}
	if atEOF && start < len(data) {
		return len(data), data[start:], nil
	}
	if start > 0 {
		return start, nil, nil
	}
	return 0, nil, nil
}
