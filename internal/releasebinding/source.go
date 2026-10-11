// Package releasebinding verifies a controlled signed package/Chart mapping.
package releasebinding

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

var ErrUntrusted = errors.New("TRUSTED_RELEASE_UNAVAILABLE")
var digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var ident = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
var version = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type Binding struct {
	ReleaseRef     string `json:"releaseRef"`
	AppID          string `json:"appId"`
	AppVersion     string `json:"appVersion"`
	PackageDigest  string `json:"packageDigest"`
	ChartDigest    string `json:"chartDigest"`
	TemplateDigest string `json:"templateDigest"`
}

func (b Binding) Valid() bool {
	return ident.MatchString(b.ReleaseRef) && ident.MatchString(b.AppID) && version.MatchString(b.AppVersion) && digest.MatchString(b.PackageDigest) && digest.MatchString(b.ChartDigest) && digest.MatchString(b.TemplateDigest)
}

type SignedRelease struct {
	SchemaVersion int     `json:"schemaVersion"`
	Binding       Binding `json:"binding"`
	NotBefore     string  `json:"notBefore"`
	ExpiresAt     string  `json:"expiresAt"`
}
type registryRelease struct {
	ReleaseRef string `json:"releaseRef"`
	Payload    string `json:"payload"`
	Signature  string `json:"signature"`
	KeyID      string `json:"keyId"`
	PackageRef string `json:"packageRef"`
	ChartRef   string `json:"chartRef"`
}
type registry struct {
	SchemaVersion int               `json:"schemaVersion"`
	Keys          map[string]string `json:"keys"`
	Releases      []registryRelease `json:"releases"`
}
type Source struct {
	Path string
	Now  func() time.Time
}

func (s *Source) Resolve(ref string) (Binding, error) {
	if s == nil || s.Path == "" || !ident.MatchString(ref) {
		return Binding{}, ErrUntrusted
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return Binding{}, ErrUntrusted
	}
	defer root.Close()
	raw, err := read(root, filepath.Base(s.Path), 2<<20)
	if err != nil {
		return Binding{}, err
	}
	var cfg registry
	if strictjson.Decode(bytes.NewReader(raw), 2<<20, &cfg) != nil {
		return Binding{}, ErrUntrusted
	}
	for _, entry := range cfg.Releases {
		if entry.ReleaseRef == ref {
			payload, err := base64.StdEncoding.Strict().DecodeString(entry.Payload)
			if err != nil {
				return Binding{}, ErrUntrusted
			}
			var release SignedRelease
			if strictjson.Decode(bytes.NewReader(payload), 64<<10, &release) != nil || release.Binding.ReleaseRef != ref || s.Verify(release.Binding) != nil {
				return Binding{}, ErrUntrusted
			}
			return release.Binding, nil
		}
	}
	return Binding{}, ErrUntrusted
}
func read(root *os.Root, ref string, maximum int64) ([]byte, error) {
	if !fs.ValidPath(ref) || ref == "." || strings.ContainsAny(ref, "\\:") {
		return nil, ErrUntrusted
	}
	f, err := root.Open(ref)
	if err != nil {
		return nil, ErrUntrusted
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		return nil, ErrUntrusted
	}
	b, err := io.ReadAll(io.LimitReader(f, maximum+1))
	if err != nil || int64(len(b)) > maximum {
		return nil, ErrUntrusted
	}
	return b, nil
}
func sum(raw []byte) string { h := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(h[:]) }

// Verify reloads controlled key/release configuration, so a withdrawal or expiry
// blocks the next mutation. Neither a request nor a signed payload selects URLs.
func (s *Source) Verify(want Binding) error {
	if s == nil || s.Path == "" || !want.Valid() {
		return ErrUntrusted
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return ErrUntrusted
	}
	defer root.Close()
	raw, err := read(root, filepath.Base(s.Path), 2<<20)
	if err != nil {
		return err
	}
	var cfg registry
	if strictjson.Decode(bytes.NewReader(raw), 2<<20, &cfg) != nil || cfg.SchemaVersion != 1 || cfg.Keys == nil || cfg.Releases == nil || len(cfg.Keys) > 128 || len(cfg.Releases) > 128 {
		return ErrUntrusted
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	seen := map[string]bool{}
	var candidate *registryRelease
	for i := range cfg.Releases {
		r := &cfg.Releases[i]
		if !ident.MatchString(r.ReleaseRef) || seen[r.ReleaseRef] {
			return ErrUntrusted
		}
		seen[r.ReleaseRef] = true
		if r.ReleaseRef == want.ReleaseRef {
			candidate = r
		}
	}
	if candidate == nil {
		return ErrUntrusted
	}
	r := candidate
	pub, err := base64.StdEncoding.Strict().DecodeString(cfg.Keys[r.KeyID])
	if err != nil || len(pub) != ed25519.PublicKeySize || !ident.MatchString(r.KeyID) {
		return ErrUntrusted
	}
	payload, err := base64.StdEncoding.Strict().DecodeString(r.Payload)
	if err != nil || len(payload) > 64<<10 {
		return ErrUntrusted
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(r.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(ed25519.PublicKey(pub), append([]byte("verdantflare.preinstalled.release.v1\n"), payload...), signature) {
		return ErrUntrusted
	}
	var release SignedRelease
	if strictjson.Decode(bytes.NewReader(payload), 64<<10, &release) != nil || release.SchemaVersion != 1 || release.Binding != want {
		return ErrUntrusted
	}
	from, e1 := time.Parse(time.RFC3339, release.NotBefore)
	until, e2 := time.Parse(time.RFC3339, release.ExpiresAt)
	if e1 != nil || e2 != nil || !until.After(from) || now.Before(from) || !now.Before(until) {
		return ErrUntrusted
	}
	manifest, e1 := read(root, r.PackageRef, 4<<20)
	chart, e2 := read(root, r.ChartRef, 32<<20)
	if e1 != nil || e2 != nil || sum(manifest) != want.PackageDigest || sum(chart) != want.ChartDigest {
		return ErrUntrusted
	}
	return nil
}
