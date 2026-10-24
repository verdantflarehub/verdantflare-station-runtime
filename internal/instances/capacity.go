package instances

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"slices"

	"k8s.io/apimachinery/pkg/util/validation"
)

// CapacityClaims is separate operator policy. It never rewrites a pinned
// execution profile, an accepted request, or an existing workspace binding.
type CapacityClaims map[string][]string

type capacityEntry struct {
	ProfileID   string   `json:"profile_id"`
	ProfileHash string   `json:"profile_hash"`
	Claims      []string `json:"claims"`
}

func LoadCapacityClaims(path string, current, history map[string]HelperProfile) (CapacityClaims, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, ErrInvalid
	}
	var entries []capacityEntry
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&entries) != nil || d.Decode(new(any)) != io.EOF || len(entries) == 0 {
		return nil, ErrInvalid
	}
	known := map[string]HelperProfile{}
	occupied := map[string]bool{}
	for _, profiles := range []map[string]HelperProfile{current, history} {
		for _, p := range profiles {
			known[hex.EncodeToString(p.Hash[:])] = p
			for _, claim := range p.Claims {
				occupied[p.Namespace+"/"+claim] = true
			}
		}
	}
	out := CapacityClaims{}
	for _, e := range entries {
		hash, err := hex.DecodeString(e.ProfileHash)
		p, ok := known[e.ProfileHash]
		if err != nil || len(hash) != 32 || hex.EncodeToString(hash) != e.ProfileHash || !ok || p.ID != e.ProfileID || len(e.Claims) == 0 || out[e.ProfileHash] != nil {
			return nil, ErrInvalid
		}
		for _, claim := range e.Claims {
			key := p.Namespace + "/" + claim
			if len(validation.IsDNS1123Subdomain(claim)) != 0 || occupied[key] {
				return nil, ErrInvalid
			}
			occupied[key] = true
		}
		out[e.ProfileHash] = slices.Clone(e.Claims)
	}
	return out, nil
}

func (s *Service) withCapacity(p HelperProfile) HelperProfile {
	if extra := s.CapacityClaims[hex.EncodeToString(p.Hash[:])]; len(extra) > 0 {
		p.Claims = append(slices.Clone(p.Claims), extra...)
	}
	return p
}
