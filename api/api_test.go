package api

import (
	"encoding/json"
	"testing"
)

func TestErrorBodyWire(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(ErrorBody{Error: Error{Code: CodeClaimLost, Message: "m", Details: map[string]any{}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"error":{"code":"vcs_claim_lost","message":"m","details":{}}}`; string(b) != want {
		t.Errorf("got %s, want %s", b, want)
	}
	if CodeClaimLost.Status() != 409 || Code("x").Status() != 500 {
		t.Error("status mapping")
	}
}
