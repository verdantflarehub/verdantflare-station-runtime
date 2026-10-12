package executor

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const legacyComfyTransport = "comfyui-0.1.3-exec"

//go:embed comfyui_013_maintenance.py
var legacyComfyProgram string

type legacyMaintenanceExec func(context.Context, string, string, string, []byte) ([]byte, error)

func validMaintenanceTransport(config ManagementMaintenanceRelease, target Target) bool {
	switch config.Transport {
	case "", "http":
		return config.Container == "" && validManagementToken(config.Token)
	case legacyComfyTransport:
		return config.Token == "" && config.Snapshot.AppID == "comfyui" && config.Snapshot.AppVersion == "0.1.3" &&
			config.Snapshot.DefaultWorkerVersion == "0.1.3" && len(config.Snapshot.CompatibleWorkerVersions) == 1 && config.Snapshot.CompatibleWorkerVersions[0] == "0.1.3" &&
			config.Snapshot.DataContract == "comfyui-management-v1" && target.Deployment != nil &&
			len(target.Deployment.Spec.Template.Spec.Containers) == 1 && len(target.Deployment.Spec.Template.Spec.InitContainers) == 0 &&
			validName(config.Container) && config.Container == target.Deployment.Spec.Template.Spec.Containers[0].Name
	default:
		return false
	}
}

func legacyComfyPod(p corev1.Pod, container string) bool {
	if len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 0 || (p.Spec.ShareProcessNamespace != nil && *p.Spec.ShareProcessNamespace) {
		return false
	}
	c := p.Spec.Containers[0]
	if c.Name != container || len(c.Command) != 0 || len(c.Args) != 0 || (c.WorkingDir != "" && c.WorkingDir != "/opt/vf") ||
		c.SecurityContext == nil || c.SecurityContext.ReadOnlyRootFilesystem == nil || !*c.SecurityContext.ReadOnlyRootFilesystem {
		return false
	}
	paths := map[string]bool{}
	for _, mount := range c.VolumeMounts {
		if paths[mount.MountPath] {
			return false
		}
		paths[mount.MountPath] = true
		switch mount.MountPath {
		case "/data", "/tmp":
		case "/config":
			if !mount.ReadOnly {
				return false
			}
		default:
			return false
		}
	}
	return paths["/data"] && paths["/config"]
}

type maintenanceOutput struct {
	buffer  bytes.Buffer
	maximum int
}

func (b *maintenanceOutput) Len() int { return b.buffer.Len() }

func (b *maintenanceOutput) Write(p []byte) (int, error) {
	if len(p) > b.maximum-b.Len() {
		return 0, errors.New("maintenance output exceeded limit")
	}
	return b.buffer.Write(p)
}

// Private closure with a fixed program and bounded input/output. No caller can
// choose an executable, flags, database path or script through this interface.
func newLegacyMaintenanceExec(config *rest.Config, client kubernetes.Interface) legacyMaintenanceExec {
	return func(ctx context.Context, namespace, pod, container string, input []byte) ([]byte, error) {
		if config == nil || client == nil || !validName(namespace) || !validName(pod) || !validName(container) || len(input) > 16<<10 {
			return nil, ErrManagementMaintenanceUnavailable
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		cfg := rest.CopyConfig(config)
		cfg.Timeout = 10 * time.Second
		req := client.CoreV1().RESTClient().Post().Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{
			Container: container, Command: []string{"/usr/local/bin/python", "-I", "-c", legacyComfyProgram}, Stdin: true, Stdout: true, Stderr: true,
		}, scheme.ParameterCodec)
		executor, err := remotecommand.NewSPDYExecutor(cfg, http.MethodPost, req.URL())
		if err != nil {
			return nil, ErrManagementMaintenanceUnavailable
		}
		output, stderr := &maintenanceOutput{maximum: 16 << 10}, &maintenanceOutput{maximum: 4096}
		err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: bytes.NewReader(input), Stdout: output, Stderr: stderr})
		if err != nil || stderr.Len() != 0 || ctx.Err() != nil {
			return nil, ErrManagementMaintenanceUnavailable
		}
		return bytes.Clone(output.buffer.Bytes()), nil
	}
}
