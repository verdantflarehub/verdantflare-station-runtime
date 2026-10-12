package appstate

import (
	"os"
	"regexp"
	"slices"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Config struct {
	Namespaces    []string
	Core          *Client
	HealthTargets []HealthTarget
}

func LoadConfig(path string, lookup func(string) string, otherTokens []string) (*Config, error) {
	if path == "" {
		return nil, nil
	}
	if lookup == nil {
		return nil, ErrInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrInvalid
	}
	defer f.Close()
	var input struct {
		SchemaVersion int            `json:"schemaVersion"`
		Namespaces    []string       `json:"namespaces"`
		CoreOrigin    string         `json:"coreOrigin"`
		TokenEnv      string         `json:"tokenEnv"`
		HealthTargets []HealthTarget `json:"healthTargets,omitempty"`
	}
	if strictjson.Decode(f, 65536, &input) != nil || input.SchemaVersion != 1 || len(input.Namespaces) == 0 || len(input.Namespaces) > 128 || !regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`).MatchString(input.TokenEnv) {
		return nil, ErrInvalid
	}
	seen := map[string]bool{}
	for _, ns := range input.Namespaces {
		if len(validation.IsDNS1123Label(ns)) != 0 || seen[ns] {
			return nil, ErrInvalid
		}
		seen[ns] = true
	}
	token := lookup(input.TokenEnv)
	if slices.Contains(otherTokens, token) {
		return nil, ErrInvalid
	}
	client, err := NewClient(input.CoreOrigin, token)
	if err != nil {
		return nil, err
	}
	slices.Sort(input.Namespaces)
	if !validHealthTargets(input.HealthTargets, input.Namespaces) {
		return nil, ErrInvalid
	}
	return &Config{Namespaces: input.Namespaces, Core: client, HealthTargets: input.HealthTargets}, nil
}

// Watch uses a separate client with per-call contexts; the executor's short
// HTTP timeout must not truncate every long-lived Kubernetes watch stream.
func NewKubernetes(path, contextName string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error
	if path != "" {
		if contextName == "" {
			return nil, ErrInvalid
		}
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: path}, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, ErrInvalid
	}
	cfg.Timeout = 0
	cfg.QPS = 20
	cfg.Burst = 40
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, ErrInvalid
	}
	return client, nil
}
