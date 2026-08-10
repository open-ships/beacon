package filter

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
	"github.com/open-ships/beacon/internal/n2kcatalog"
)

func env(pgnNum uint32, source uint8, payload string) *msg.Envelope {
	return &msg.Envelope{PGN: pgnNum, Source: source, Dest: 255, Priority: 2,
		Timestamp: time.Now(), Payload: json.RawMessage(payload)}
}

func TestPlainIntLiterals(t *testing.T) {
	c, err := Compile([]string{"msg.pgn == 127250"})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := c.Match(env(127250, 1, `{}`))
	if err != nil || !ok {
		t.Fatalf("match = %v, %v; want true", ok, err)
	}
	ok, _ = c.Match(env(127251, 1, `{}`))
	if ok {
		t.Fatal("wrong PGN matched")
	}
}

func TestAndSemanticsAcrossExprs(t *testing.T) {
	c, err := Compile([]string{"msg.pgn in [127250, 128259]", "msg.source != 42"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(127250, 42, `{}`)); ok {
		t.Fatal("second expr should have rejected")
	}
	if ok, _ := c.Match(env(128259, 7, `{}`)); !ok {
		t.Fatal("both exprs should pass")
	}
}

func TestPayloadField(t *testing.T) {
	c, err := Compile([]string{"double(msg.payload.speed) > 2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(128259, 1, `{"speed": 3.5}`)); !ok {
		t.Fatal("payload threshold should pass")
	}
	if ok, _ := c.Match(env(128259, 1, `{"speed": 1.0}`)); ok {
		t.Fatal("payload threshold should reject")
	}
}

func TestPhysicalAndProvenanceFields(t *testing.T) {
	c, err := Compile([]string{`msg.ingress == "socketcan:can0"`, `msg.device_name_hex == "FEDCBA9876543210"`, `msg.physical.speed.value > 2.0`})
	if err != nil {
		t.Fatal(err)
	}
	e := env(128259, 1, `{}`)
	e.Ingress = "socketcan:can0"
	e.DeviceNameHex = "FEDCBA9876543210"
	e.Physical = map[string]n2kcatalog.PhysicalField{"speed": {Value: 2.5, Unit: "m/s"}}
	if ok, err := c.Match(e); err != nil || !ok {
		t.Fatalf("match = %v, %v", ok, err)
	}
}

func TestEvalErrorReturnsError(t *testing.T) {
	c, err := Compile([]string{"double(msg.payload.missing) > 1.0"})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := c.Match(env(128259, 1, `{}`))
	if ok || err == nil {
		t.Fatalf("missing field: match=%v err=%v; want false + error", ok, err)
	}
}

func TestCompileErrorRejected(t *testing.T) {
	if _, err := Compile([]string{"msg.pgn =="}); err == nil {
		t.Fatal("invalid CEL accepted")
	}
}

func TestCompileRejectsNonBooleanResult(t *testing.T) {
	for _, expr := range []string{`1`, `"heading"`, `msg.payload`, `[msg.pgn]`} {
		t.Run(expr, func(t *testing.T) {
			if _, err := Compile([]string{expr}); err == nil {
				t.Fatalf("non-boolean filter %q accepted", expr)
			} else if got := err.Error(); !strings.Contains(got, "must return bool") {
				t.Fatalf("error = %q, want boolean-result explanation", got)
			}
		})
	}
}

func TestOptionalPayloadFieldPresenceGuard(t *testing.T) {
	c, err := Compile([]string{`!has(msg.payload.heading) || double(msg.payload.heading) > 1.0`})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "absent passes", payload: `{}`, want: true},
		{name: "present passes comparison", payload: `{"heading": 2}`, want: true},
		{name: "present fails comparison", payload: `{"heading": 0.5}`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.Match(env(127250, 1, tc.payload))
			if err != nil || got != tc.want {
				t.Fatalf("Match() = %v, %v; want %v, nil", got, err, tc.want)
			}
		})
	}
}

func TestEmptyChainMatchesAll(t *testing.T) {
	c, err := Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(1, 1, `{}`)); !ok {
		t.Fatal("empty chain must pass everything")
	}
}

func TestDiagnoseReturnsOffendingTokenRanges(t *testing.T) {
	exprs := []string{"msg.pgn == 127250", "msg.source == @", "msg.priority ==", "unknown2 == true"}
	diagnostics, err := Diagnose(exprs)
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnostics) != 3 {
		t.Fatalf("diagnostics = %+v, want three errors", diagnostics)
	}

	at := diagnostics[0]
	if at.Expression != 1 || at.Line != 1 || exprs[1][at.Column:at.EndColumn] != "@" {
		t.Fatalf("unexpected-character diagnostic = %+v, range %q", at, exprs[1][at.Column:at.EndColumn])
	}
	dangling := diagnostics[1]
	if dangling.Expression != 2 || exprs[2][dangling.Column:dangling.EndColumn] != "==" {
		t.Fatalf("EOF diagnostic = %+v, range %q; want dangling operator", dangling, exprs[2][dangling.Column:dangling.EndColumn])
	}
	unknown := diagnostics[2]
	if unknown.Expression != 3 || exprs[3][unknown.Column:unknown.EndColumn] != "unknown2" {
		t.Fatalf("unknown-identifier diagnostic = %+v, range %q", unknown, exprs[3][unknown.Column:unknown.EndColumn])
	}
}

func TestDiagnoseRejectsNonBooleanResult(t *testing.T) {
	diagnostics, err := Diagnose([]string{"msg.pgn", "msg.pgn == 127250"})
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnostics) != 1 {
		t.Fatalf("diagnostics = %+v, want one non-boolean result", diagnostics)
	}
	if got := diagnostics[0]; got.Expression != 0 || got.Column != 0 || got.EndColumn != len("msg.pgn") ||
		!strings.Contains(got.Message, "must return bool") {
		t.Fatalf("non-boolean diagnostic = %+v", got)
	}
}
