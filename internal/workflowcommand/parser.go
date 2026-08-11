package workflowcommand

import (
	"strings"
	"unicode"
)

// Command is a decoded GitHub Actions workflow command.
type Command struct {
	Name       string
	Properties map[string]string
	Value      string
}

// Parse decodes double-colon and bracket-form workflow commands.
func Parse(line string) (Command, bool) {
	if command, found := parseDoubleColon(strings.TrimLeftFunc(line, unicode.IsSpace)); found {
		return command, true
	}
	marker := strings.Index(line, "##[")
	if marker < 0 {
		return Command{}, false
	}
	return parseBracket(line[marker:])
}

// IsResume reports whether line resumes the matching stop-commands token.
func IsResume(line, token string) bool {
	command, found := Parse(line)
	return found && command.Name == strings.ToLower(token) && len(command.Properties) == 0 && command.Value == ""
}

func parseDoubleColon(line string) (Command, bool) {
	if !strings.HasPrefix(line, "::") {
		return Command{}, false
	}
	separator := strings.Index(line[2:], "::")
	if separator < 0 {
		return Command{}, false
	}
	separator += 2
	name, properties := parseHeader(line[2:separator], ",", unescapeDoubleColonProperty)
	if name == "" {
		return Command{}, false
	}
	return Command{Name: name, Properties: properties, Value: unescapeDoubleColonData(line[separator+2:])}, true
}

func parseBracket(line string) (Command, bool) {
	close := strings.IndexByte(line[3:], ']')
	if close < 0 {
		return Command{}, false
	}
	close += 3
	name, properties := parseHeader(line[3:close], ";", unescapeBracketValue)
	if name == "" {
		return Command{}, false
	}
	return Command{Name: name, Properties: properties, Value: unescapeBracketValue(line[close+1:])}, true
}

func parseHeader(header, delimiter string, unescape func(string) string) (string, map[string]string) {
	name, propertyList, _ := strings.Cut(header, " ")
	properties := map[string]string{}
	for _, property := range strings.Split(propertyList, delimiter) {
		key, value, found := strings.Cut(property, "=")
		if found && key != "" {
			properties[strings.ToLower(key)] = unescape(value)
		}
	}
	return strings.ToLower(name), properties
}

func unescapeDoubleColonData(value string) string {
	value = replaceEscape(value, "%0D", "\r")
	value = replaceEscape(value, "%0A", "\n")
	return replaceEscape(value, "%25", "%")
}

func unescapeDoubleColonProperty(value string) string {
	value = replaceEscape(value, "%3A", ":")
	value = replaceEscape(value, "%2C", ",")
	return unescapeDoubleColonData(value)
}

func unescapeBracketValue(value string) string {
	value = replaceEscape(value, "%3B", ";")
	value = replaceEscape(value, "%0D", "\r")
	value = replaceEscape(value, "%0A", "\n")
	value = replaceEscape(value, "%5D", "]")
	return replaceEscape(value, "%25", "%")
}

func replaceEscape(value, encoded, decoded string) string {
	value = strings.ReplaceAll(value, encoded, decoded)
	return strings.ReplaceAll(value, strings.ToLower(encoded), decoded)
}
