package executor

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/postrender"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metameta "k8s.io/apimachinery/pkg/api/meta"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"sigs.k8s.io/yaml"
)

// ChartLifecycle is deliberately separate from the legacy Deployment driver.
// An OCI operation must never fall back to creating only part of its Chart.
type ChartLifecycle interface {
	Ensure(context.Context, Target, string, string, string, string) (bool, error)
	Uninstall(context.Context, Target, string, string) (bool, error)
}

type HelmLifecycle struct {
	Getter genericclioptions.RESTClientGetter
}

func (h *HelmLifecycle) configuration(namespace string) (*action.Configuration, error) {
	if h == nil || h.Getter == nil {
		return nil, errors.New("Helm execution is unavailable")
	}
	cfg := new(action.Configuration)
	if err := cfg.Init(h.Getter, namespace, "secret", func(string, ...interface{}) {}); err != nil {
		return nil, err
	}
	return cfg, nil
}

func chartValues(t Target, station, org string) map[string]interface{} {
	return map[string]interface{}{
		"appId":        t.AppID,
		"appVersion":   t.Version,
		"namespace":    t.Deployment.Namespace,
		"workloadName": t.Deployment.Name,
		"station": map[string]interface{}{
			"stationId":      station,
			"organizationId": org,
			"installationId": t.InstallationID,
			"templateHash":   hex.EncodeToString(t.Hash[:]),
			"packageDigest":  t.OCIChart.PackageDigest,
		},
	}
}

// A local Helm render checks the exact downloaded package, including .Files,
// before any API mutation. The manifest also provides a bounded migration
// inventory for installations previously recorded by a synthetic Helm Secret.
func renderChart(t Target, vals map[string]interface{}, operation string) (string, error) {
	chart, err := loader.LoadArchive(bytes.NewReader(t.OCIChart.Package))
	if err != nil {
		return "", err
	}
	if chart.Metadata == nil || chart.Metadata.Name != t.AppID || chart.Metadata.Version != t.OCIChart.ChartVersion {
		return "", errors.New("verified Chart identity changed")
	}
	install := action.NewInstall(new(action.Configuration))
	install.ClientOnly = true
	install.DryRun = true
	install.DisableHooks = true
	install.ReleaseName = t.AppID
	install.Namespace = t.Deployment.Namespace
	install.PostRenderer = &stationPostRenderer{target: t, station: vals["station"].(map[string]interface{}), operation: operation}
	rel, err := install.Run(chart, vals)
	if err != nil {
		return "", err
	}
	// Helm keeps hooks outside rel.Manifest and does not pass them through the
	// post-renderer. Reject the package rather than silently ignoring resources
	// that would bypass the installation namespace and ownership checks.
	if len(rel.Hooks) != 0 {
		return "", errors.New("Chart contains unsupported Helm hooks")
	}
	if err := checkChartManifest(t, rel.Manifest, vals); err != nil {
		return "", err
	}
	return rel.Manifest, nil
}

func checkChartManifest(t Target, manifest string, vals map[string]interface{}) error {
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	found := false
	allowedKinds := map[string]bool{"Deployment": true, "Service": true, "ConfigMap": true, "NetworkPolicy": true, "PersistentVolumeClaim": true}
	for {
		var raw map[string]interface{}
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(raw) == 0 {
			continue
		}
		meta, _ := raw["metadata"].(map[string]interface{})
		name, _ := meta["name"].(string)
		namespace, _ := meta["namespace"].(string)
		kind, _ := raw["kind"].(string)
		if !allowedKinds[kind] || name == "" {
			return fmt.Errorf("Chart contains unsupported resource kind %s", kind)
		}
		if namespace != t.Deployment.Namespace {
			return fmt.Errorf("Chart resource %s is outside the installation namespace", name)
		}
		if kind != "Deployment" || name != t.Deployment.Name {
			continue
		}
		if found {
			return errors.New("Chart has duplicate application Deployments")
		}
		found = true
		var rendered appsv1.Deployment
		encoded, err := json.Marshal(raw)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(encoded, &rendered); err != nil {
			return err
		}
		if !sameImages(&rendered, t.Deployment) {
			return errors.New("Chart workload image differs from the fixed execution target")
		}
		stationValues := vals["station"].(map[string]interface{})
		for key, value := range map[string]interface{}{
			ownerKey: stationValues["stationId"], orgKey: stationValues["organizationId"],
			installationKey: stationValues["installationId"], hashKey: stationValues["templateHash"],
		} {
			if rendered.Annotations[key] != value {
				return fmt.Errorf("Chart workload lacks bound %s", key)
			}
		}
	}
	if !found {
		return errors.New("Chart does not render the bound Deployment")
	}
	return checkPVCRetention(manifest)
}

func (h *HelmLifecycle) Ensure(ctx context.Context, t Target, actionName, operation, station, org string) (bool, error) {
	if t.OCIChart == nil || len(t.OCIChart.Package) == 0 || t.InstallationID == "" {
		return false, errors.New("verified Chart and installation identity are required")
	}
	vals := chartValues(t, station, org)
	manifest, err := renderChart(t, vals, operation)
	if err != nil {
		return false, err
	}
	chart, err := loader.LoadArchive(bytes.NewReader(t.OCIChart.Package))
	if err != nil {
		return false, err
	}
	cfg, err := h.configuration(t.Deployment.Namespace)
	if err != nil {
		return false, err
	}
	current, err := cfg.Releases.Last(t.AppID)
	if errors.Is(err, driver.ErrReleaseNotFound) {
		if actionName != "install" {
			return false, errors.New("Helm release is absent during upgrade")
		}
		install := action.NewInstall(cfg)
		install.ReleaseName = t.AppID
		install.Namespace = t.Deployment.Namespace
		install.DisableHooks = true
		install.PostRenderer = &stationPostRenderer{target: t, station: vals["station"].(map[string]interface{}), operation: operation}
		_, err = install.RunWithContext(ctx, chart, vals)
		return err == nil, err
	}
	if err != nil {
		return false, err
	}
	if current.Namespace != t.Deployment.Namespace || current.Info == nil {
		return false, errors.New("Helm release is not in the bound namespace")
	}
	if current.Info.Status.IsPending() && current.Manifest != "" && releaseBoundToInstallation(current, t, station, org) && releasePackageDigest(current) == t.OCIChart.PackageDigest {
		return true, nil
	}
	if current.Info.Status != release.StatusDeployed {
		return false, errors.New("Helm release is not deployed in the bound namespace")
	}
	if current.Manifest != "" {
		if !releaseBoundToInstallation(current, t, station, org) {
			return false, errors.New("Helm release installation identity differs")
		}
		if releasePackageDigest(current) == t.OCIChart.PackageDigest {
			present, err := releaseResourcesPresent(cfg, current)
			if err != nil {
				return false, err
			}
			if present {
				return false, nil
			}
		}
	}
	if actionName == "install" && current.Manifest != "" {
		return false, errors.New("existing Helm release has a different package")
	}
	upgrade := action.NewUpgrade(cfg)
	upgrade.Namespace = t.Deployment.Namespace
	upgrade.DisableHooks = true
	upgrade.PostRenderer = &stationPostRenderer{target: t, station: vals["station"].(map[string]interface{}), operation: operation}
	if current.Manifest == "" {
		if err := verifyLegacyAdoption(ctx, cfg, manifest, t, station, org); err != nil {
			return false, err
		}
		upgrade.TakeOwnership = true
	}
	_, err = upgrade.RunWithContext(ctx, t.AppID, chart, vals)
	return err == nil, err
}

func releasePackageDigest(rel *release.Release) string {
	if rel == nil || rel.Config == nil {
		return ""
	}
	station, _ := rel.Config["station"].(map[string]interface{})
	digest, _ := station["packageDigest"].(string)
	return digest
}

func releaseBoundToInstallation(rel *release.Release, t Target, station, org string) bool {
	if rel == nil || rel.Config == nil {
		return false
	}
	bound, _ := rel.Config["station"].(map[string]interface{})
	return bound["stationId"] == station && bound["organizationId"] == org && bound["installationId"] == t.InstallationID
}

func releaseResourcesPresent(cfg *action.Configuration, rel *release.Release) (bool, error) {
	resources, err := cfg.KubeClient.Build(strings.NewReader(rel.Manifest), false)
	if err != nil {
		return false, err
	}
	for _, info := range resources {
		if err := info.Get(); apierrors.IsNotFound(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
	}
	return true, nil
}

func verifyLegacyAdoption(ctx context.Context, cfg *action.Configuration, manifest string, t Target, station, org string) error {
	resources, err := cfg.KubeClient.Build(strings.NewReader(manifest), false)
	if err != nil {
		return err
	}
	for _, info := range resources {
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.Namespace != t.Deployment.Namespace || info.Mapping.Scope.Name() != metameta.RESTScopeNameNamespace {
			return errors.New("legacy release contains a resource outside the installation namespace")
		}
		if err := info.Get(); apierrors.IsNotFound(err) {
			continue
		} else if err != nil {
			return err
		}
		accessor, err := metameta.Accessor(info.Object)
		if err != nil {
			return err
		}
		annotations, labels := accessor.GetAnnotations(), accessor.GetLabels()
		if annotations["meta.helm.sh/release-name"] != t.AppID || annotations["meta.helm.sh/release-namespace"] != t.Deployment.Namespace || labels["app.kubernetes.io/managed-by"] != "Helm" {
			return fmt.Errorf("existing Chart resource %s is not owned by this release", info.Name)
		}
		if info.Name == t.Deployment.Name && info.Mapping.GroupVersionKind.Kind == "Deployment" &&
			(annotations[ownerKey] != station || annotations[orgKey] != org || annotations[installationKey] != t.InstallationID) {
			return errors.New("legacy Deployment installation identity differs")
		}
	}
	return nil
}

func (h *HelmLifecycle) Uninstall(ctx context.Context, t Target, station, org string) (bool, error) {
	cfg, err := h.configuration(t.Deployment.Namespace)
	if err != nil {
		return false, err
	}
	current, err := cfg.Releases.Last(t.AppID)
	if errors.Is(err, driver.ErrReleaseNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.Namespace != t.Deployment.Namespace {
		return false, errors.New("Helm release is not in the bound namespace")
	}
	if current.Manifest == "" {
		// The old executor recorded an empty release while creating the workload
		// directly. Adopt its resources through the verified Chart first, so the
		// next reconcile can uninstall the complete, owned release.
		if _, err := h.Ensure(ctx, t, "install", "", station, org); err != nil {
			return false, err
		}
		return true, nil
	}
	if !releaseBoundToInstallation(current, t, station, org) || releasePackageDigest(current) != t.OCIChart.PackageDigest {
		return false, errors.New("Helm release differs from the bound installation")
	}
	if err := checkPVCRetention(current.Manifest); err != nil {
		return false, err
	}
	uninstall := action.NewUninstall(cfg)
	uninstall.Wait = false
	uninstall.DisableHooks = true
	_, err = uninstall.Run(t.AppID)
	return err == nil, err
}

func checkPVCRetention(manifest string) error {
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	for {
		var raw map[string]interface{}
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if raw["kind"] != "PersistentVolumeClaim" {
			continue
		}
		meta, _ := raw["metadata"].(map[string]interface{})
		annotations, _ := meta["annotations"].(map[string]interface{})
		if annotations["helm.sh/resource-policy"] != "keep" {
			return errors.New("Chart PVC has no Helm keep policy")
		}
	}
}

var _ ChartLifecycle = (*HelmLifecycle)(nil)
var _ postrender.PostRenderer = (*stationPostRenderer)(nil)

type stationPostRenderer struct {
	target    Target
	station   map[string]interface{}
	operation string
}

func (p *stationPostRenderer) Run(in *bytes.Buffer) (*bytes.Buffer, error) {
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(in.Bytes()), 4096)
	out := new(bytes.Buffer)
	for {
		var raw map[string]interface{}
		if err := decoder.Decode(&raw); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return nil, err
		}
		if len(raw) == 0 {
			continue
		}
		metadata, _ := raw["metadata"].(map[string]interface{})
		if metadata == nil {
			return nil, errors.New("Chart resource has no metadata")
		}
		if namespace, _ := metadata["namespace"].(string); namespace != "" && namespace != p.target.Deployment.Namespace {
			return nil, errors.New("Chart resource targets another namespace")
		}
		metadata["namespace"] = p.target.Deployment.Namespace
		annotations, _ := metadata["annotations"].(map[string]interface{})
		if annotations == nil {
			annotations = map[string]interface{}{}
		}
		for key, value := range map[string]interface{}{
			ownerKey: p.station["stationId"], orgKey: p.station["organizationId"],
			installationKey: p.station["installationId"], hashKey: p.station["templateHash"], appKey: p.target.AppID,
		} {
			annotations[key] = value
		}
		if p.operation != "" {
			annotations[appCenterOperationKey] = p.operation
		}
		metadata["annotations"] = annotations
		raw["metadata"] = metadata
		encoded, err := yaml.Marshal(raw)
		if err != nil {
			return nil, err
		}
		if out.Len() > 0 {
			out.WriteString("---\n")
		}
		out.Write(encoded)
	}
}
