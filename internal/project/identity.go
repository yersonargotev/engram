package project

import "fmt"

// IdentityStrength describes how confidently a project source identifies one
// project. It is discovery metadata; write authority is stated separately by
// IdentityPolicy.AllowsImplicitWrite.
type IdentityStrength string

const (
	IdentityStrengthStrong     IdentityStrength = "strong"
	IdentityStrengthExplicit   IdentityStrength = "explicit"
	IdentityStrengthWeak       IdentityStrength = "weak"
	IdentityStrengthUnresolved IdentityStrength = "unresolved"
	IdentityStrengthAggregate  IdentityStrength = "aggregate"
	IdentityStrengthUnknown    IdentityStrength = "unknown"
)

const (
	// WriteAuthorityErrorCode is the stable machine-readable code for an
	// implicit write rejected because project identity evidence is insufficient.
	WriteAuthorityErrorCode = "weak_project_identity"
	// ExplicitProjectSafeNextAction is the transport-neutral recovery action.
	ExplicitProjectSafeNextAction = "provide an explicit project name and retry the write"
)

// IdentityPolicy is the deterministic project identity policy for one source.
// AllowsImplicitWrite is true only when automatic detection is authoritative
// enough to select a project for mutation without caller-supplied authority.
type IdentityPolicy struct {
	Strength            IdentityStrength
	AllowsImplicitWrite bool
}

// ClassifyIdentitySource classifies every project source exposed by this
// package. Unknown future sources fail closed for implicit writes.
func ClassifyIdentitySource(source string) IdentityPolicy {
	switch source {
	case SourceConfig, SourceGitRemote:
		return IdentityPolicy{Strength: IdentityStrengthStrong, AllowsImplicitWrite: true}
	case SourceCLIExplicit, SourceEnvironment, SourceExplicitOverride, SourceUserSelectedAfterAmbiguousProject,
		SourceRequestBody, SourceSessionProject, SourceProcessOverride:
		return IdentityPolicy{Strength: IdentityStrengthExplicit}
	case SourceGitRoot, SourceGitChild, SourceDirBasename:
		return IdentityPolicy{Strength: IdentityStrengthWeak}
	case SourceAmbiguous:
		return IdentityPolicy{Strength: IdentityStrengthUnresolved}
	case SourceAllProjects, SourcePersonalScope:
		return IdentityPolicy{Strength: IdentityStrengthAggregate}
	default:
		return IdentityPolicy{Strength: IdentityStrengthUnknown}
	}
}

// IdentityPolicyForResult applies source classification to one concrete
// detection result. Detection errors retain their source strength for
// diagnostics but never advertise implicit write authority.
func IdentityPolicyForResult(result DetectionResult) IdentityPolicy {
	policy := ClassifyIdentitySource(result.Source)
	if result.Error != nil {
		policy.AllowsImplicitWrite = false
	}
	return policy
}

// WriteAuthorityError carries the complete, non-sensitive identity evidence a
// caller needs to recover from a rejected implicit write.
type WriteAuthorityError struct {
	Code           string
	Project        string
	Source         string
	Path           string
	Strength       IdentityStrength
	SafeNextAction string
}

func (e *WriteAuthorityError) Error() string {
	return fmt.Sprintf(
		"project %q was detected from %s with %s identity strength; %s",
		e.Project, e.Source, e.Strength, e.SafeNextAction,
	)
}

// RequireImplicitWriteAuthority rejects detection results that are useful for
// discovery but are not authoritative enough to select a write target. Caller-
// supplied explicit sources use their established validation paths instead.
func RequireImplicitWriteAuthority(result DetectionResult) error {
	if result.Error != nil {
		return result.Error
	}
	policy := ClassifyIdentitySource(result.Source)
	if policy.AllowsImplicitWrite {
		return nil
	}
	return &WriteAuthorityError{
		Code:           WriteAuthorityErrorCode,
		Project:        result.Project,
		Source:         result.Source,
		Path:           result.Path,
		Strength:       policy.Strength,
		SafeNextAction: ExplicitProjectSafeNextAction,
	}
}
