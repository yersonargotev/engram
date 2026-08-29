package project

import (
	"errors"
	"testing"
)

func TestIdentityPolicyClassifiesEveryProjectSource(t *testing.T) {
	tests := []struct {
		source        string
		strength      IdentityStrength
		implicitWrite bool
	}{
		{SourceConfig, IdentityStrengthStrong, true},
		{SourceGitRemote, IdentityStrengthStrong, true},
		{SourceCLIExplicit, IdentityStrengthExplicit, false},
		{SourceEnvironment, IdentityStrengthExplicit, false},
		{SourceExplicitOverride, IdentityStrengthExplicit, false},
		{SourceUserSelectedAfterAmbiguousProject, IdentityStrengthExplicit, false},
		{SourceRequestBody, IdentityStrengthExplicit, false},
		{SourceSessionProject, IdentityStrengthExplicit, false},
		{SourceProcessOverride, IdentityStrengthExplicit, false},
		{SourceGitRoot, IdentityStrengthWeak, false},
		{SourceGitChild, IdentityStrengthWeak, false},
		{SourceDirBasename, IdentityStrengthWeak, false},
		{SourceAmbiguous, IdentityStrengthUnresolved, false},
		{SourceAllProjects, IdentityStrengthAggregate, false},
		{SourcePersonalScope, IdentityStrengthAggregate, false},
	}

	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			policy := ClassifyIdentitySource(test.source)
			if policy.Strength != test.strength || policy.AllowsImplicitWrite != test.implicitWrite {
				t.Fatalf("ClassifyIdentitySource(%q) = %+v, want strength=%q implicit_write=%v", test.source, policy, test.strength, test.implicitWrite)
			}
		})
	}
}

func TestIdentityPolicyRejectsUnknownSources(t *testing.T) {
	policy := ClassifyIdentitySource("future_source")
	if policy.Strength != IdentityStrengthUnknown || policy.AllowsImplicitWrite {
		t.Fatalf("unknown source policy = %+v, want unknown and write denied", policy)
	}
}

func TestIdentityPolicyForResultDeniesErroredStrongSource(t *testing.T) {
	policy := IdentityPolicyForResult(DetectionResult{Source: SourceConfig, Error: ErrInvalidConfig})
	if policy.Strength != IdentityStrengthStrong || policy.AllowsImplicitWrite {
		t.Fatalf("invalid config result policy = %+v, want strong source with write denied", policy)
	}
}

func TestRequireImplicitWriteAuthority(t *testing.T) {
	for _, source := range []string{SourceConfig, SourceGitRemote} {
		if err := RequireImplicitWriteAuthority(DetectionResult{Project: "engram", Source: source, Path: "/repo"}); err != nil {
			t.Fatalf("strong %s identity rejected: %v", source, err)
		}
	}

	for _, source := range []string{SourceGitRoot, SourceGitChild, SourceDirBasename} {
		weak := DetectionResult{Project: "tmp", Source: source, Path: "/tmp"}
		err := RequireImplicitWriteAuthority(weak)
		var authorityErr *WriteAuthorityError
		if !errors.As(err, &authorityErr) {
			t.Fatalf("weak %s identity error = %T %v, want *WriteAuthorityError", source, err, err)
		}
		if authorityErr.Code != WriteAuthorityErrorCode || authorityErr.Project != weak.Project ||
			authorityErr.Source != weak.Source || authorityErr.Path != weak.Path ||
			authorityErr.Strength != IdentityStrengthWeak || authorityErr.SafeNextAction != ExplicitProjectSafeNextAction {
			t.Fatalf("weak %s identity error = %+v", source, authorityErr)
		}
	}
}

func TestRequireImplicitWriteAuthorityPreservesDetectionError(t *testing.T) {
	err := RequireImplicitWriteAuthority(DetectionResult{Source: SourceAmbiguous, Error: ErrAmbiguousProject})
	if !errors.Is(err, ErrAmbiguousProject) {
		t.Fatalf("detection error = %v, want ErrAmbiguousProject", err)
	}
}
