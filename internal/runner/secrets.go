package runner

import (
	"fmt"
	"strings"

	"github.com/kelos-dev/open-actions/internal/expression"
)

func (e *Executor) registerJobSecretMasks(plan *Plan) error {
	registerSecret := func(name, value string) {
		if !strings.EqualFold(name, actionsStepDebugName) && !strings.EqualFold(name, actionsRunnerDebugName) {
			e.masker.addSecret(value)
		}
	}
	register := func(input string) error {
		program, err := expression.Parse(input)
		if err != nil {
			return fmt.Errorf("collect secret references for job %q: %w", plan.JobID, err)
		}
		names, all := program.ContextReferences("secrets")
		if all {
			for name, value := range e.secrets {
				registerSecret(name, value)
			}
		} else {
			for _, name := range names {
				if value, found := stringMapValue(e.secrets, name); found {
					registerSecret(name, value)
				}
			}
		}
		return nil
	}
	var inputs []string
	for _, values := range []map[string]string{plan.Env, plan.Outputs} {
		for _, input := range values {
			inputs = append(inputs, input)
		}
	}
	for _, step := range plan.Steps {
		inputs = append(inputs, step.Name, step.Run, step.WorkingDirectory)
		if input, ok := step.ContinueOnError.(string); ok {
			inputs = append(inputs, input)
		}
		for _, values := range []map[string]string{step.Env, step.With} {
			for _, input := range values {
				inputs = append(inputs, input)
			}
		}
	}
	for _, input := range inputs {
		if err := register(input); err != nil {
			return err
		}
	}
	return nil
}
