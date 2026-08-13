package expression

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxExpressionNodes   = 256
	maxExpressionNesting = 64
)

type tokenKind uint8

const (
	endToken tokenKind = iota
	identifierToken
	numberToken
	stringToken
	leftParenToken
	rightParenToken
	leftBracketToken
	rightBracketToken
	dotToken
	commaToken
	wildcardToken
	notToken
	equalToken
	notEqualToken
	lessToken
	lessEqualToken
	greaterToken
	greaterEqualToken
	andToken
	orToken
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

type lexer struct {
	input string
	pos   int
}

func (l *lexer) next() (token, error) {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
	if l.pos == len(l.input) {
		return token{kind: endToken, pos: l.pos}, nil
	}
	start := l.pos
	switch l.input[l.pos] {
	case '(':
		l.pos++
		return token{kind: leftParenToken, text: "(", pos: start}, nil
	case ')':
		l.pos++
		return token{kind: rightParenToken, text: ")", pos: start}, nil
	case '[':
		l.pos++
		return token{kind: leftBracketToken, text: "[", pos: start}, nil
	case ']':
		l.pos++
		return token{kind: rightBracketToken, text: "]", pos: start}, nil
	case '.':
		l.pos++
		return token{kind: dotToken, text: ".", pos: start}, nil
	case ',':
		l.pos++
		return token{kind: commaToken, text: ",", pos: start}, nil
	case '*':
		l.pos++
		return token{kind: wildcardToken, text: "*", pos: start}, nil
	case '!':
		l.pos++
		if l.consume('=') {
			return token{kind: notEqualToken, text: "!=", pos: start}, nil
		}
		return token{kind: notToken, text: "!", pos: start}, nil
	case '=':
		l.pos++
		if l.consume('=') {
			return token{kind: equalToken, text: "==", pos: start}, nil
		}
		return token{}, fmt.Errorf("unsupported character %q at column %d", l.input[start], start+1)
	case '<':
		l.pos++
		if l.consume('=') {
			return token{kind: lessEqualToken, text: "<=", pos: start}, nil
		}
		return token{kind: lessToken, text: "<", pos: start}, nil
	case '>':
		l.pos++
		if l.consume('=') {
			return token{kind: greaterEqualToken, text: ">=", pos: start}, nil
		}
		return token{kind: greaterToken, text: ">", pos: start}, nil
	case '&':
		l.pos++
		if l.consume('&') {
			return token{kind: andToken, text: "&&", pos: start}, nil
		}
		return token{}, fmt.Errorf("unsupported character %q at column %d", l.input[start], start+1)
	case '|':
		l.pos++
		if l.consume('|') {
			return token{kind: orToken, text: "||", pos: start}, nil
		}
		return token{}, fmt.Errorf("unsupported character %q at column %d", l.input[start], start+1)
	case '\'':
		return l.stringToken()
	}
	if identifierStart(l.input[l.pos]) {
		l.pos++
		for l.pos < len(l.input) && identifierPart(l.input[l.pos]) {
			l.pos++
		}
		return token{kind: identifierToken, text: l.input[start:l.pos], pos: start}, nil
	}
	if asciiDigit(l.input[l.pos]) || (l.input[l.pos] == '-' && l.pos+1 < len(l.input) && asciiDigit(l.input[l.pos+1])) {
		return l.numberToken()
	}
	return token{}, fmt.Errorf("unsupported character %q at column %d", l.input[start], start+1)
}

func (l *lexer) consume(expected byte) bool {
	if l.pos < len(l.input) && l.input[l.pos] == expected {
		l.pos++
		return true
	}
	return false
}

func (l *lexer) stringToken() (token, error) {
	start := l.pos
	l.pos++
	var result strings.Builder
	for l.pos < len(l.input) {
		if l.input[l.pos] != '\'' {
			result.WriteByte(l.input[l.pos])
			l.pos++
			continue
		}
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
			result.WriteByte('\'')
			l.pos += 2
			continue
		}
		l.pos++
		return token{kind: stringToken, text: result.String(), pos: start}, nil
	}
	return token{}, fmt.Errorf("unterminated string at column %d", start+1)
}

func (l *lexer) numberToken() (token, error) {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	if l.pos+1 < len(l.input) && l.input[l.pos] == '0' && (l.input[l.pos+1] == 'x' || l.input[l.pos+1] == 'X') {
		l.pos += 2
		digits := l.pos
		for l.pos < len(l.input) && asciiHex(l.input[l.pos]) {
			l.pos++
		}
		if digits == l.pos {
			return token{}, fmt.Errorf("invalid hexadecimal number at column %d", start+1)
		}
		return token{kind: numberToken, text: l.input[start:l.pos], pos: start}, nil
	}
	for l.pos < len(l.input) && asciiDigit(l.input[l.pos]) {
		l.pos++
	}
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && asciiDigit(l.input[l.pos]) {
			l.pos++
		}
	}
	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		l.pos++
		if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
			l.pos++
		}
		digits := l.pos
		for l.pos < len(l.input) && asciiDigit(l.input[l.pos]) {
			l.pos++
		}
		if digits == l.pos {
			return token{}, fmt.Errorf("invalid exponent at column %d", start+1)
		}
	}
	text := l.input[start:l.pos]
	if _, err := strconv.ParseFloat(text, 64); err != nil {
		return token{}, fmt.Errorf("invalid number at column %d", start+1)
	}
	return token{kind: numberToken, text: text, pos: start}, nil
}

func identifierStart(character byte) bool {
	return character == '_' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z'
}

func identifierPart(character byte) bool {
	return identifierStart(character) || asciiDigit(character) || character == '-'
}

func asciiDigit(character byte) bool {
	return character >= '0' && character <= '9'
}

func asciiHex(character byte) bool {
	return asciiDigit(character) || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F'
}

type parser struct {
	lexer   lexer
	current token
	nodes   int
	nesting int
}

func parseExpression(input string) (node, error) {
	p := &parser{lexer: lexer{input: input}}
	if err := p.advance(); err != nil {
		return nil, err
	}
	result, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.current.kind != endToken {
		return nil, fmt.Errorf("unexpected token %q at column %d", p.current.text, p.current.pos+1)
	}
	return result, nil
}

func (p *parser) advance() error {
	next, err := p.lexer.next()
	if err != nil {
		return err
	}
	p.current = next
	return nil
}

func (p *parser) addNode(result node) (node, error) {
	p.nodes++
	if p.nodes > maxExpressionNodes {
		return nil, fmt.Errorf("expression exceeds %d nodes", maxExpressionNodes)
	}
	return result, nil
}

func (p *parser) parseNested(parse func() (node, error)) (node, error) {
	p.nesting++
	if p.nesting > maxExpressionNesting {
		return nil, fmt.Errorf("expression exceeds %d nesting levels", maxExpressionNesting)
	}
	defer func() { p.nesting-- }()
	return parse()
}

func (p *parser) parseOr() (node, error) {
	return p.parseBinary(p.parseAnd, orToken)
}

func (p *parser) parseAnd() (node, error) {
	return p.parseBinary(p.parseComparison, andToken)
}

func (p *parser) parseComparison() (node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.current.kind == equalToken || p.current.kind == notEqualToken || p.current.kind == lessToken || p.current.kind == lessEqualToken || p.current.kind == greaterToken || p.current.kind == greaterEqualToken {
		operator := p.current.kind
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left, err = p.addNode(binaryNode{operator: operator, left: left, right: right})
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *parser) parseBinary(next func() (node, error), operator tokenKind) (node, error) {
	left, err := next()
	if err != nil {
		return nil, err
	}
	for p.current.kind == operator {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := next()
		if err != nil {
			return nil, err
		}
		left, err = p.addNode(binaryNode{operator: operator, left: left, right: right})
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.current.kind == notToken {
		if err := p.advance(); err != nil {
			return nil, err
		}
		operand, err := p.parseNested(p.parseUnary)
		if err != nil {
			return nil, err
		}
		return p.addNode(unaryNode{operand: operand})
	}
	return p.parsePostfix()
}

func (p *parser) parsePostfix() (node, error) {
	result, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		switch p.current.kind {
		case dotToken:
			if err := p.advance(); err != nil {
				return nil, err
			}
			if p.current.kind == wildcardToken {
				wildcard, err := p.addNode(wildcardNode{})
				if err != nil {
					return nil, err
				}
				if err := p.advance(); err != nil {
					return nil, err
				}
				result, err = p.addNode(indexNode{target: result, index: wildcard})
				if err != nil {
					return nil, err
				}
				continue
			}
			if p.current.kind != identifierToken {
				return nil, fmt.Errorf("expected property name at column %d", p.current.pos+1)
			}
			name := p.current.text
			if err := p.advance(); err != nil {
				return nil, err
			}
			result, err = p.addNode(propertyNode{target: result, name: name})
			if err != nil {
				return nil, err
			}
		case leftBracketToken:
			if err := p.advance(); err != nil {
				return nil, err
			}
			var index node
			if p.current.kind == wildcardToken {
				index, err = p.addNode(wildcardNode{})
				if err != nil {
					return nil, err
				}
				if err := p.advance(); err != nil {
					return nil, err
				}
			} else {
				index, err = p.parseNested(p.parseOr)
				if err != nil {
					return nil, err
				}
			}
			if p.current.kind != rightBracketToken {
				return nil, fmt.Errorf("expected ] at column %d", p.current.pos+1)
			}
			if err := p.advance(); err != nil {
				return nil, err
			}
			result, err = p.addNode(indexNode{target: result, index: index})
			if err != nil {
				return nil, err
			}
		default:
			return result, nil
		}
	}
}

func (p *parser) parsePrimary() (node, error) {
	current := p.current
	switch current.kind {
	case identifierToken:
		if err := p.advance(); err != nil {
			return nil, err
		}
		switch strings.ToLower(current.text) {
		case "true":
			return p.addNode(literalNode{value: value{kind: boolKind, boolean: true}})
		case "false":
			return p.addNode(literalNode{value: value{kind: boolKind}})
		case "null":
			return p.addNode(literalNode{value: value{kind: nullKind}})
		}
		if p.current.kind == leftParenToken {
			return p.parseCall(current.text)
		}
		return p.addNode(contextNode{name: current.text})
	case numberToken:
		if err := p.advance(); err != nil {
			return nil, err
		}
		number, err := parseNumber(current.text)
		if err != nil {
			return nil, err
		}
		return p.addNode(literalNode{value: value{kind: numberKind, number: number}})
	case stringToken:
		if err := p.advance(); err != nil {
			return nil, err
		}
		return p.addNode(literalNode{value: value{kind: stringKind, text: current.text}})
	case leftParenToken:
		if err := p.advance(); err != nil {
			return nil, err
		}
		result, err := p.parseNested(p.parseOr)
		if err != nil {
			return nil, err
		}
		if p.current.kind != rightParenToken {
			return nil, fmt.Errorf("expected ) at column %d", p.current.pos+1)
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected expression at column %d", current.pos+1)
	}
}

func (p *parser) parseCall(name string) (node, error) {
	if err := p.advance(); err != nil {
		return nil, err
	}
	var arguments []node
	if p.current.kind != rightParenToken {
		for {
			argument, err := p.parseNested(p.parseOr)
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, argument)
			if p.current.kind != commaToken {
				break
			}
			if err := p.advance(); err != nil {
				return nil, err
			}
		}
	}
	if p.current.kind != rightParenToken {
		return nil, fmt.Errorf("expected ) at column %d", p.current.pos+1)
	}
	if err := p.advance(); err != nil {
		return nil, err
	}
	return p.addNode(callNode{name: name, arguments: arguments})
}

func parseNumber(text string) (float64, error) {
	negative := strings.HasPrefix(text, "-")
	unsigned := strings.TrimPrefix(text, "-")
	if strings.HasPrefix(unsigned, "0x") || strings.HasPrefix(unsigned, "0X") {
		integer, err := strconv.ParseUint(unsigned[2:], 16, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid hexadecimal number")
		}
		result := float64(integer)
		if negative {
			result = -result
		}
		return result, nil
	}
	if !jsonNumberPattern.MatchString(text) {
		return 0, fmt.Errorf("invalid number")
	}
	return strconv.ParseFloat(text, 64)
}
