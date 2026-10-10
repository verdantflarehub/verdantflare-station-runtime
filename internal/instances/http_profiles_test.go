package instances

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestProfilesAreAuthenticatedPublicFieldsOnly(t *testing.T) {
	h := Handler{Token: "private-test-token", Service: &Service{Profiles: map[string]HelperProfile{
		"standard": {StorageProfile: StorageProfile{ID: "standard", Namespace: "private-ns", NodeName: "private-node", Claims: []string{"private-pvc"}, Bytes: 1024}, Image: "private-image", MaxFileBytes: 512},
	}}}
	for _, tc := range []struct {
		method, path, token string
		duplicate           bool
		status              int
	}{
		{"GET", "/internal/v1/instances/profiles", "", false, 401},
		{"GET", "/internal/v1/instances/profiles", h.Token, true, 401},
		{"GET", "/internal/v1/instances/profiles?target=other", h.Token, false, 400},
		{"POST", "/internal/v1/instances/profiles", h.Token, false, 404},
		{"GET", "/internal/v1/instances/profiles", h.Token, false, 200},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		if tc.duplicate {
			r.Header.Add("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
		if w.Code == 200 {
			var value struct {
				Profiles []map[string]any `json:"profiles"`
			}
			if json.Unmarshal(w.Body.Bytes(), &value) != nil || len(value.Profiles) != 1 {
				t.Fatal("missing profiles")
			}
			p := value.Profiles[0]
			if len(p) != 3 || p["profile_id"] != "standard" || p["storage_reserved_bytes"] != float64(1024) || p["max_file_bytes"] != float64(512) {
				t.Fatal("private data or false capacity in profile response")
			}
		}
	}
}
