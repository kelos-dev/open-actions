package actionref

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode"
)

type Reference struct {
	Owner      string
	Repository string
	Path       string
	Ref        string
}

func Parse(value string) (Reference, error) {
	at := strings.LastIndex(value, "@")
	if at <= 0 || at == len(value)-1 {
		return Reference{}, fmt.Errorf("action reference %q must have the form owner/repository[/path]@ref", value)
	}
	location, ref := value[:at], value[at+1:]
	parts := strings.Split(location, "/")
	if len(parts) < 2 || !componentPattern.MatchString(parts[0]) || !componentPattern.MatchString(parts[1]) || parts[0] == "." || parts[0] == ".." || parts[1] == "." || parts[1] == ".." {
		return Reference{}, fmt.Errorf("action reference %q must identify a GitHub owner and repository", value)
	}
	if strings.HasPrefix(ref, "-") || strings.HasPrefix(ref, "/") || strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") || strings.HasSuffix(strings.ToLower(ref), ".lock") || strings.Contains(ref, "//") || strings.Contains(ref, "@{") || strings.Contains(ref, "..") || strings.ContainsAny(ref, "\\~^:?*[") {
		return Reference{}, fmt.Errorf("action reference %q contains an unsafe Git ref", value)
	}
	for _, character := range ref {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return Reference{}, fmt.Errorf("action reference %q contains an unsafe Git ref", value)
		}
	}
	subpath := strings.Join(parts[2:], "/")
	if subpath != "" {
		clean := path.Clean(subpath)
		if clean != subpath || clean == "." || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") {
			return Reference{}, fmt.Errorf("action reference %q contains an unsafe action path", value)
		}
	}
	return Reference{Owner: parts[0], Repository: parts[1], Path: subpath, Ref: ref}, nil
}

var componentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
