package executor

import (
	"crypto/sha256"
	"errors"
	"regexp"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-runtime/api/runtimev1"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

const installationKey = "station.verdantflare.com/installation-id"

type InstallationBinding struct {
	Container string `json:"container"`
	Env       string `json:"env"`
	Source    string `json:"source"`
}

var installationEnvName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)

func validInstallationBindings(t Target) bool {
	if len(t.InstallationBindings) > 32 {
		return false
	}
	seen := map[string]bool{}
	for _, binding := range t.InstallationBindings {
		if !installationEnvName.MatchString(binding.Env) || seen[binding.Container+":"+binding.Env] {
			return false
		}
		switch binding.Source {
		case "installation_id", "station_id", "organization_id", "app_id", "app_version":
		default:
			return false
		}
		found := false
		if t.Deployment == nil {
			return false
		}
		for _, container := range t.Deployment.Spec.Template.Spec.Containers {
			if container.Name != binding.Container {
				continue
			}
			found = true
			for _, variable := range container.Env {
				if variable.Name == binding.Env {
					return false
				}
			}
		}
		if !found {
			return false
		}
		seen[binding.Container+":"+binding.Env] = true
	}
	return true
}

// Bind only a private copy of the registered template. Public requests cannot
// select an environment name, source, container or substitute a value.
func bindInstallation(t Target, in *pb.AppRequest) (Target, error) {
	if !validInstallationBindings(t) {
		return Target{}, errors.New("invalid installation bindings")
	}
	if len(t.InstallationBindings) == 0 {
		return t, nil
	}
	id, err := uuid.Parse(in.InstallationId)
	if err != nil || id.Version() != 7 || id.String() != in.InstallationId {
		return Target{}, errors.New("registered application requires an installation identity")
	}
	values := map[string]string{"installation_id": in.InstallationId, "station_id": in.StationId,
		"organization_id": in.OrganizationId, "app_id": in.AppId, "app_version": in.AppVersion}
	t.InstallationID = in.InstallationId
	t.Deployment = t.Deployment.DeepCopy()
	for _, binding := range t.InstallationBindings {
		for i := range t.Deployment.Spec.Template.Spec.Containers {
			container := &t.Deployment.Spec.Template.Spec.Containers[i]
			if container.Name == binding.Container {
				container.Env = append(container.Env, corev1.EnvVar{Name: binding.Env, Value: values[binding.Source]})
			}
		}
	}
	return t, nil
}

func operationHash(in *pb.AppRequest, installationBound bool) [32]byte {
	normalized := proto.Clone(in).(*pb.AppRequest)
	normalized.RequestId = ""
	if !installationBound {
		normalized.InstallationId = ""
	}
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	return sha256.Sum256(raw)
}
