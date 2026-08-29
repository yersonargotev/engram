package main

import "github.com/yersonargotev/engram/internal/project"

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
