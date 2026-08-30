package workflow

import "github.com/kelos-dev/open-actions/internal/expression"

// ExpressionSite identifies a workflow or action metadata field with a
// distinct GitHub Actions context contract.
type ExpressionSite uint8

const (
	ExpressionWorkflowConcurrency ExpressionSite = iota
	ExpressionWorkflowEnvironment
	ExpressionJobCondition
	ExpressionJobStrategy
	ExpressionJobConfiguration
	ExpressionJobEnvironment
	ExpressionJobOutput
	ExpressionStep
	ExpressionStepCondition
	ExpressionActionInputDefault
	ExpressionCompositeStep
	ExpressionCompositeCondition
	ExpressionCompositeOutput
)

// ExpressionAvailability returns the contexts and special functions available
// at an expression site. open_actions is an Open Actions extension.
func ExpressionAvailability(site ExpressionSite) expression.Availability {
	switch site {
	case ExpressionWorkflowConcurrency:
		return expression.NewAvailability("github", "inputs", "vars")
	case ExpressionWorkflowEnvironment:
		return expression.NewAvailability("github", "open_actions", "secrets", "inputs", "vars")
	case ExpressionJobCondition:
		return expression.NewAvailability("github", "open_actions", "needs", "vars", "inputs").WithStatusFunctions()
	case ExpressionJobStrategy:
		return expression.NewAvailability("github", "open_actions", "needs", "vars", "inputs")
	case ExpressionJobConfiguration:
		return expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "vars", "inputs")
	case ExpressionJobEnvironment:
		return expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "vars", "secrets", "inputs")
	case ExpressionJobOutput:
		return expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "secrets", "steps", "inputs")
	case ExpressionStep:
		return expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "secrets", "steps", "inputs").WithHashFiles()
	case ExpressionStepCondition:
		return expression.NewAvailability("github", "open_actions", "needs", "strategy", "matrix", "job", "runner", "env", "vars", "steps", "inputs").WithStatusFunctions().WithHashFiles()
	case ExpressionActionInputDefault:
		return expression.NewAvailability("github", "open_actions", "strategy", "matrix", "job", "runner").WithHashFiles()
	case ExpressionCompositeStep:
		return expression.NewAvailability("github", "open_actions", "strategy", "matrix", "job", "runner", "env", "inputs", "steps").WithHashFiles()
	case ExpressionCompositeCondition:
		return expression.NewAvailability("github", "open_actions", "strategy", "matrix", "job", "runner", "env", "inputs", "steps").WithStatusFunctions().WithHashFiles()
	case ExpressionCompositeOutput:
		return expression.NewAvailability("github", "open_actions", "strategy", "matrix", "job", "runner", "env", "inputs", "steps")
	default:
		return expression.NewAvailability()
	}
}
