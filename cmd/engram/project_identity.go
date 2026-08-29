package main

import (
	"errors"

	"github.com/yersonargotev/engram/internal/project"
)

func projectIdentityDetails(result project.DetectionResult) map[string]any {
	policy := project.IdentityPolicyForResult(result)
	return map[string]any{
		"project":                result.Project,
		"project_source":         result.Source,
		"project_path":           result.Path,
		"project_strength":       policy.Strength,
		"implicit_write_allowed": policy.AllowsImplicitWrite,
		"safe_next_action":       project.ExplicitProjectSafeNextAction,
	}
}

func failProjectResolution(result project.DetectionResult, err error) {
	details := projectIdentityDetails(result)
	code := "project_detection_failed"
	var authorityErr *project.WriteAuthorityError
	if errors.As(err, &authorityErr) {
		code = authorityErr.Code
	} else if errors.Is(err, project.ErrAmbiguousProject) {
		code = "ambiguous_project"
		details["available_projects"] = result.AvailableProjects
	}
	failCLI(true, code, err.Error(), details)
}
