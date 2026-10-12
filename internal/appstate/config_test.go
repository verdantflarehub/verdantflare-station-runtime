package appstate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateObserverConfiguration(t *testing.T) {
	if config, err := LoadConfig("", nil, nil); err != nil || config != nil {
		t.Fatal("disabled observer enabled")
	}
	path := filepath.Join(t.TempDir(), "observer.json")
	token := strings.Repeat("t", 40)
	raw := `{"schemaVersion":1,"namespaces":["apps"],"coreOrigin":"http://core.invalid","tokenEnv":"TEST_COLLECTOR_TOKEN"}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if config, err := LoadConfig(path, func(string) string { return token }, nil); err != nil || config == nil {
		t.Fatal("valid config refused", err)
	}
	if _, err := LoadConfig(path, func(string) string { return token }, []string{token}); err == nil {
		t.Fatal("instance credential reused")
	}
	for _, bad := range []string{strings.Replace(raw, `["apps"]`, `["apps","apps"]`, 1), strings.Replace(raw, `["apps"]`, `null`, 1), strings.Replace(raw, `http://core.invalid`, `http://user:password@core.invalid`, 1), strings.Replace(raw, `http://core.invalid`, `http://core.invalid/path`, 1), strings.Replace(raw, `http://core.invalid`, `http://core.invalid?`, 1)} {
		if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path, func(string) string { return token }, nil); err == nil {
			t.Fatal("invalid registry accepted")
		}
	}
}
