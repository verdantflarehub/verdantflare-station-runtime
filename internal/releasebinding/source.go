// Package releasebinding verifies a controlled package/Chart digest mapping.
package releasebinding

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

// SchemaVersion 2 carries digests only; the mounted registry is the trust root.
const SchemaVersion = 2

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

type Entry struct {
	Binding
	PackageRef string `json:"packageRef"`
	ChartRef   string `json:"chartRef"`
}
type Registry struct {
	SchemaVersion int     `json:"schemaVersion"`
	Releases      []Entry `json:"releases"`
}
type Source struct {
	Path string
}

func (s *Source) Resolve(ref string) (Binding, error) {
	if !ident.MatchString(ref) {
		return Binding{}, ErrUntrusted
	}
	root, cfg, err := s.load()
	if err != nil {
		return Binding{}, err
	}
	defer root.Close()
	entry, err := find(root, cfg, ref)
	if err != nil {
		return Binding{}, err
	}
	return entry.Binding, nil
}

// Verify reloads the controlled registry, so a withdrawal blocks the next
// mutation. Neither a request nor a registry entry selects URLs.
func (s *Source) Verify(want Binding) error {
	if !want.Valid() {
		return ErrUntrusted
	}
	root, cfg, err := s.load()
	if err != nil {
		return err
	}
	defer root.Close()
	entry, err := find(root, cfg, want.ReleaseRef)
	if err != nil || entry.Binding != want {
		return ErrUntrusted
	}
	return nil
}

func (s *Source) load() (*os.Root, Registry, error) {
	var cfg Registry
	if s == nil || s.Path == "" {
		return nil, cfg, ErrUntrusted
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return nil, cfg, ErrUntrusted
	}
	raw, err := read(root, filepath.Base(s.Path), 2<<20)
	if err != nil || strictjson.Decode(bytes.NewReader(raw), 2<<20, &cfg) != nil || cfg.SchemaVersion != SchemaVersion || cfg.Releases == nil || len(cfg.Releases) > 128 {
		root.Close()
		return nil, cfg, ErrUntrusted
	}
	return root, cfg, nil
}

// find rejects duplicate references and checks the mounted bytes against the
// recorded digests before any entry is returned.
func find(root *os.Root, cfg Registry, ref string) (Entry, error) {
	seen := map[string]bool{}
	var candidate *Entry
	for i := range cfg.Releases {
		r := &cfg.Releases[i]
		if !r.Binding.Valid() || seen[r.ReleaseRef] {
			return Entry{}, ErrUntrusted
		}
		seen[r.ReleaseRef] = true
		if r.ReleaseRef == ref {
			candidate = r
		}
	}
	if candidate == nil {
		return Entry{}, ErrUntrusted
	}
	manifest, e1 := read(root, candidate.PackageRef, 4<<20)
	chart, e2 := read(root, candidate.ChartRef, 32<<20)
	if e1 != nil || e2 != nil || Sum(manifest) != candidate.PackageDigest || Sum(chart) != candidate.ChartDigest {
		return Entry{}, ErrUntrusted
	}
	return *candidate, nil
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
func Sum(raw []byte) string { h := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(h[:]) }
