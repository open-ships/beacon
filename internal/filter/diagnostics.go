package filter

import (
	"strings"
	"unicode/utf8"
)

// Diagnostic is one CEL compile error tied to a byte range in one expression.
// Line is 1-based; Column and EndColumn are 0-based byte columns. CEL reports
// an error point rather than a span, so Diagnose expands that point to the
// smallest surrounding token that can usefully be highlighted by an editor.
type Diagnostic struct {
	Expression int
	Line       int
	Column     int
	EndColumn  int
	Message    string
}

// Diagnose compiles every expression and returns every parse/type-check error
// instead of stopping at the first invalid filter. An error is reserved for an
// environment construction failure; invalid CEL is represented by diagnostics.
func Diagnose(exprs []string) ([]Diagnostic, error) {
	env, err := newEnvironment()
	if err != nil {
		return nil, err
	}

	var diagnostics []Diagnostic
	seen := make(map[[4]int]struct{})
	for expressionIndex, expr := range exprs {
		_, issues := env.Compile(expr)
		for _, issue := range issues.Errors() {
			line := issue.Location.Line()
			column := issue.Location.Column()
			if line < 1 {
				line = 1
			}
			if column < 0 {
				column = 0
			}
			start, end := diagnosticTokenRange(expr, line, column)
			key := [4]int{expressionIndex, line, start, end}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			diagnostics = append(diagnostics, Diagnostic{
				Expression: expressionIndex,
				Line:       line,
				Column:     start,
				EndColumn:  end,
				Message:    issue.Message,
			})
		}
	}
	return diagnostics, nil
}

func diagnosticTokenRange(expr string, line, column int) (int, int) {
	lines := strings.Split(expr, "\n")
	if line < 1 || line > len(lines) {
		return 0, firstRuneEnd(expr, 0)
	}
	text := lines[line-1]
	if text == "" {
		return 0, 0
	}
	if column > len(text) {
		column = len(text)
	}

	// EOF diagnostics have no character at their reported point. Highlight
	// the preceding token (usually a dangling operator) instead.
	if column == len(text) {
		end := column
		for end > 0 && asciiSpace(text[end-1]) {
			end--
		}
		if end == 0 {
			return 0, firstRuneEnd(text, 0)
		}
		start := previousTokenStart(text, end)
		return start, end
	}

	start := column
	for start < len(text) && asciiSpace(text[start]) {
		start++
	}
	if start == len(text) {
		end := start
		for end > 0 && asciiSpace(text[end-1]) {
			end--
		}
		return previousTokenStart(text, end), end
	}
	return start, nextTokenEnd(text, start)
}

func previousTokenStart(text string, end int) int {
	start := end - 1
	kind := tokenKind(text[start])
	if kind == tokenOther {
		_, size := utf8.DecodeLastRuneInString(text[:end])
		return end - size
	}
	for start > 0 && tokenKind(text[start-1]) == kind {
		start--
	}
	return start
}

func nextTokenEnd(text string, start int) int {
	kind := tokenKind(text[start])
	if kind == tokenOther {
		return firstRuneEnd(text, start)
	}
	end := start + 1
	for end < len(text) && (tokenKind(text[end]) == kind || kind == tokenWord && asciiDigit(text[end])) {
		end++
	}
	return end
}

func firstRuneEnd(text string, start int) int {
	if start >= len(text) {
		return start
	}
	_, size := utf8.DecodeRuneInString(text[start:])
	return start + size
}

type diagnosticTokenKind uint8

const (
	tokenOther diagnosticTokenKind = iota
	tokenWord
	tokenNumber
	tokenOperator
)

func tokenKind(b byte) diagnosticTokenKind {
	switch {
	case b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'):
		return tokenWord
	case (b >= '0' && b <= '9') || b == '.':
		return tokenNumber
	case strings.ContainsRune("=!<>&|+-*/%?", rune(b)):
		return tokenOperator
	default:
		return tokenOther
	}
}

func asciiSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r'
}

func asciiDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
