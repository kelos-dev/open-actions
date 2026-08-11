package runner

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxOutputCommandFileBytes = 1 << 20

type commandFiles struct {
	environment string
	output      string
	path        string
	state       string
	summary     string
}

type commandUpdates struct {
	environment map[string]string
	outputs     map[string]string
	paths       []string
	state       map[string]string
}

func newCommandFiles(directory, prefix string) (commandFiles, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return commandFiles{}, fmt.Errorf("create command file directory: %w", err)
	}
	files := commandFiles{
		environment: filepath.Join(directory, prefix+"-environment"),
		output:      filepath.Join(directory, prefix+"-output"),
		path:        filepath.Join(directory, prefix+"-path"),
		state:       filepath.Join(directory, prefix+"-state"),
		summary:     filepath.Join(directory, prefix+"-summary"),
	}
	for _, path := range []string{files.environment, files.output, files.path, files.state, files.summary} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			return commandFiles{}, fmt.Errorf("create command file: %w", err)
		}
	}
	return files, nil
}

func (f commandFiles) read() (commandUpdates, error) {
	environment, err := readKeyValueFile(f.environment)
	if err != nil {
		return commandUpdates{}, fmt.Errorf("read GITHUB_ENV: %w", err)
	}
	state, err := readKeyValueFile(f.state)
	if err != nil {
		return commandUpdates{}, fmt.Errorf("read GITHUB_STATE: %w", err)
	}
	outputInfo, err := os.Stat(f.output)
	if err != nil {
		return commandUpdates{}, fmt.Errorf("read GITHUB_OUTPUT: %w", err)
	}
	if outputInfo.Size() > maxOutputCommandFileBytes {
		return commandUpdates{}, fmt.Errorf("read GITHUB_OUTPUT: file exceeds %d bytes", maxOutputCommandFileBytes)
	}
	outputs, err := readKeyValueFile(f.output)
	if err != nil {
		return commandUpdates{}, fmt.Errorf("read GITHUB_OUTPUT: %w", err)
	}
	pathData, err := os.ReadFile(f.path)
	if err != nil {
		return commandUpdates{}, fmt.Errorf("read GITHUB_PATH: %w", err)
	}
	paths := []string{}
	for _, value := range strings.Split(strings.ReplaceAll(string(pathData), "\r\n", "\n"), "\n") {
		if value != "" {
			paths = append(paths, value)
		}
	}
	return commandUpdates{environment: environment, outputs: outputs, paths: paths, state: state}, nil
}

func (f commandFiles) environmentVariables(environment []string) []string {
	environment = setEnvironment(environment, "GITHUB_ENV", f.environment)
	environment = setEnvironment(environment, "GITHUB_OUTPUT", f.output)
	environment = setEnvironment(environment, "GITHUB_PATH", f.path)
	environment = setEnvironment(environment, "GITHUB_STATE", f.state)
	return setEnvironment(environment, "GITHUB_STEP_SUMMARY", f.summary)
}

func readKeyValueFile(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	result := map[string]string{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			continue
		}
		assignment := strings.IndexByte(line, '=')
		heredoc := strings.Index(line, "<<")
		if heredoc >= 0 && (assignment < 0 || heredoc < assignment) {
			name := line[:heredoc]
			delimiter := line[heredoc+2:]
			if name == "" || delimiter == "" {
				return nil, fmt.Errorf("invalid multiline command %q", line)
			}
			values := []string{}
			closed := false
			for scanner.Scan() {
				value := strings.TrimSuffix(scanner.Text(), "\r")
				if value == delimiter {
					closed = true
					break
				}
				values = append(values, value)
			}
			if !closed {
				return nil, fmt.Errorf("multiline command %q is not terminated", name)
			}
			result[name] = strings.Join(values, "\n")
			continue
		}
		if assignment < 1 {
			return nil, fmt.Errorf("invalid command %q", line)
		}
		name := line[:assignment]
		value := line[assignment+1:]
		result[name] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
