package filter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/open-ships/beacon/internal/msg"
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

func TestEmptyChainMatchesAll(t *testing.T) {
	c, err := Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Match(env(1, 1, `{}`)); !ok {
		t.Fatal("empty chain must pass everything")
	}
}
