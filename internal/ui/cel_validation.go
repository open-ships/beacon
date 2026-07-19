package ui

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/open-ships/beacon/internal/filter"
)

type celValidationResponse struct {
	Valid       bool                      `json:"valid"`
	Diagnostics []celValidationDiagnostic `json:"diagnostics"`
}

type celValidationDiagnostic struct {
	Start   int    `json:"start"`
	End     int    `json:"end"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
	Message string `json:"message"`
}

type filterSourceLine struct {
	expression     string
	physicalLine   int
	leadingBytes   int
	raw            string
	lineStartUTF16 int
}

func acceptsJSON(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "application/json")
}

func renderCELValidationJSON(w http.ResponseWriter, log *slog.Logger, text string) {
	response, err := buildCELValidationResponse(text)
	if err != nil {
		log.Error("ui: CEL validation failed", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(response)
}

func buildCELValidationResponse(text string) (celValidationResponse, error) {
	lines := filterSourceLines(text)
	expressions := make([]string, len(lines))
	for i, line := range lines {
		expressions[i] = line.expression
	}
	diagnostics, err := filter.Diagnose(expressions)
	if err != nil {
		return celValidationResponse{}, err
	}

	response := celValidationResponse{
		Valid:       len(diagnostics) == 0,
		Diagnostics: make([]celValidationDiagnostic, 0, len(diagnostics)),
	}
	for _, diagnostic := range diagnostics {
		if diagnostic.Expression < 0 || diagnostic.Expression >= len(lines) {
			continue
		}
		line := lines[diagnostic.Expression]
		startByte := line.leadingBytes + diagnostic.Column
		endByte := line.leadingBytes + diagnostic.EndColumn
		start := line.lineStartUTF16 + utf16PrefixLength(line.raw, startByte)
		end := line.lineStartUTF16 + utf16PrefixLength(line.raw, endByte)
		response.Diagnostics = append(response.Diagnostics, celValidationDiagnostic{
			Start:   start,
			End:     end,
			Line:    line.physicalLine + diagnostic.Line - 1,
			Column:  utf8PrefixLength(line.raw, startByte) + 1,
			Message: diagnostic.Message,
		})
	}
	return response, nil
}

func filterSourceLines(text string) []filterSourceLine {
	rawLines := strings.Split(text, "\n")
	lines := make([]filterSourceLine, 0, len(rawLines))
	lineStartUTF16 := 0
	for i, raw := range rawLines {
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" {
			lines = append(lines, filterSourceLine{
				expression:     trimmed,
				physicalLine:   i + 1,
				leadingBytes:   strings.Index(raw, trimmed),
				raw:            raw,
				lineStartUTF16: lineStartUTF16,
			})
		}
		lineStartUTF16 += utf16Length(raw)
		if i < len(rawLines)-1 {
			lineStartUTF16++ // textarea newlines are one UTF-16 code unit.
		}
	}
	return lines
}

func utf16PrefixLength(s string, byteOffset int) int {
	byteOffset = runeBoundary(s, byteOffset)
	return utf16Length(s[:byteOffset])
}

func utf8PrefixLength(s string, byteOffset int) int {
	byteOffset = runeBoundary(s, byteOffset)
	return utf8.RuneCountInString(s[:byteOffset])
}

func utf16Length(s string) int {
	return len(utf16.Encode([]rune(s)))
}

func runeBoundary(s string, byteOffset int) int {
	if byteOffset < 0 {
		return 0
	}
	if byteOffset > len(s) {
		return len(s)
	}
	for byteOffset > 0 && byteOffset < len(s) && !utf8.RuneStart(s[byteOffset]) {
		byteOffset--
	}
	return byteOffset
}
