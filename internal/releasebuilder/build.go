// Package releasebuilder prepares controlled digest release bundles offline. It
// does not configure a running Core or Runtime.
package releasebuilder

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/executor"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebinding"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
)

var ErrInvalid = errors.New("invalid or unavailable release preparation input")
var ErrOutput = errors.New("release output must be a new writable directory; existing content is never overwritten")
var identifier = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

type Release struct {
	ReleaseRef string `json:"releaseRef"`
	AppID      string `json:"appId"`
	AppVersion string `json:"appVersion"`
	PackageRef string `json:"packageRef"`
	ChartRef   string `json:"chartRef"`
}
type Config struct {
	SchemaVersion int       `json:"schemaVersion"`
	Releases      []Release `json:"releases"`
}
type Options struct{ Root, Targets, Config, Output string }
type Summary struct {
	RegistryFile string                   `json:"registryFile"`
	Releases     []releasebinding.Binding `json:"releases"`
}
type material struct {
	entry                    releasebinding.Entry
	packageBytes, chartBytes []byte
}

func read(root *os.Root, ref string, limit int64) ([]byte, error) {
	if !fs.ValidPath(ref) || ref == "." || strings.ContainsAny(ref, "\\:") {
		return nil, ErrInvalid
	}
	f, err := root.Open(ref)
	if err != nil {
		return nil, ErrInvalid
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, ErrInvalid
	}
	return raw, nil
}
func digest(raw []byte) string { return releasebinding.Sum(raw) }

// Build writes a digest-only registry next to copies of the original package
// and Chart bytes. Errors intentionally omit paths and input contents.
func Build(options Options) (Summary, error) {
	out := Summary{RegistryFile: "registry.json", Releases: []releasebinding.Binding{}}
	if options.Root == "" || options.Targets == "" || options.Config == "" || options.Output == "" {
		return out, ErrInvalid
	}
	root, err := os.OpenRoot(options.Root)
	if err != nil {
		return out, ErrInvalid
	}
	defer root.Close()
	raw, err := read(root, options.Config, 2<<20)
	if err != nil {
		return out, err
	}
	var cfg Config
	if strictjson.Decode(bytes.NewReader(raw), 2<<20, &cfg) != nil || cfg.SchemaVersion != releasebinding.SchemaVersion || len(cfg.Releases) == 0 || len(cfg.Releases) > 128 {
		return out, ErrInvalid
	}
	// Validate the target registry path through the same root before the existing
	// loader reads it. Manifest/template references are root-confined by loader.
	if _, err = read(root, options.Targets, 2<<20); err != nil {
		return out, err
	}
	targets, err := executor.LoadRegistry(options.Root, filepath.Join(options.Root, filepath.FromSlash(options.Targets)))
	if err != nil {
		return out, ErrInvalid
	}
	materials := make([]material, 0, len(cfg.Releases))
	seenRef, seenApp := map[string]bool{}, map[string]bool{}
	for _, entry := range cfg.Releases {
		if !identifier.MatchString(entry.ReleaseRef) || seenRef[entry.ReleaseRef] || seenApp[entry.AppID] {
			return out, ErrInvalid
		}
		seenRef[entry.ReleaseRef], seenApp[entry.AppID] = true, true
		target, ok := targets[entry.AppID+"@"+entry.AppVersion]
		if !ok || executor.ValidatePreinstalledTarget(target) != nil {
			return out, ErrInvalid
		}
		packageBytes, err := read(root, entry.PackageRef, 4<<20)
		if err != nil {
			return out, err
		}
		if verifyProvenance(root, packageBytes, target) != nil {
			return out, ErrInvalid
		}
		chartBytes, err := read(root, entry.ChartRef, 32<<20)
		if err != nil {
			return out, err
		}
		binding := releasebinding.Binding{ReleaseRef: entry.ReleaseRef, AppID: entry.AppID, AppVersion: entry.AppVersion, PackageDigest: digest(packageBytes), ChartDigest: digest(chartBytes), TemplateDigest: "sha256:" + hex.EncodeToString(target.Hash[:])}
		if !binding.Valid() {
			return out, ErrInvalid
		}
		registration := releasebinding.Entry{Binding: binding, PackageRef: entry.ReleaseRef + "/package.json", ChartRef: entry.ReleaseRef + "/chart.tgz"}
		materials = append(materials, material{registration, packageBytes, chartBytes})
		out.Releases = append(out.Releases, binding)
	}
	registration := releasebinding.Registry{SchemaVersion: releasebinding.SchemaVersion, Releases: []releasebinding.Entry{}}
	for _, item := range materials {
		registration.Releases = append(registration.Releases, item.entry)
	}
	raw, err = json.MarshalIndent(registration, "", "  ")
	if err != nil || len(raw) > 2<<20 {
		return out, ErrInvalid
	}
	if err = os.Mkdir(options.Output, 0700); err != nil {
		return out, ErrOutput
	}
	outputRoot, err := os.OpenRoot(options.Output)
	if err != nil {
		return out, ErrOutput
	}
	defer outputRoot.Close()
	write := func(name string, content []byte) error {
		file, err := outputRoot.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return ErrOutput
		}
		_, writeErr := file.Write(content)
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			return ErrOutput
		}
		return nil
	}
	for _, item := range materials {
		if err = outputRoot.Mkdir(item.entry.ReleaseRef, 0700); err != nil {
			return out, ErrOutput
		}
		if write(item.entry.PackageRef, item.packageBytes) != nil || write(item.entry.ChartRef, item.chartBytes) != nil {
			return out, ErrOutput
		}
	}
	// No usable registry exists until all immutable materials have been written.
	if err = write(out.RegistryFile, raw); err != nil {
		return out, err
	}
	consumer := &releasebinding.Source{Path: filepath.Join(options.Output, out.RegistryFile)}
	for _, binding := range out.Releases {
		if consumer.Verify(binding) != nil {
			return out, ErrOutput
		}
	}
	return out, nil
}
