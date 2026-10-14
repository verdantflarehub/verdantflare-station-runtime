package instances

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

var (
	ErrNotFound     = errors.New("NOT_FOUND")
	ErrNotDestroyed = errors.New("INSTANCE_NOT_DESTROYED")
)

type WorkspaceVolume struct {
	WorkspaceID    string   `json:"workspace_id"`
	PVCName        string   `json:"pvc_name"`
	PVName         string   `json:"pv_name"`
	Namespace      string   `json:"namespace"`
	PoolName       string   `json:"pool_name"`
	AppID          string   `json:"app_id"`
	AppDisplayName string   `json:"app_display_name"`
	StorageBytes   int64    `json:"storage_bytes"`
	StorageClass   string   `json:"storage_class"`
	AccessMode     string   `json:"access_mode"`
	NodeName       string   `json:"node_name"`
	InstanceID     string   `json:"instance_id,omitempty"`
	InstanceName   string   `json:"instance_name,omitempty"`
	OrganizationID string   `json:"organization_id,omitempty"`
	ProjectID      string   `json:"project_id,omitempty"`
	InstallationID string   `json:"installation_id,omitempty"`
	State          string   `json:"state"`
	AllowedActions []string `json:"allowed_actions"`
	BoundAt        string   `json:"bound_at,omitempty"`
	RetainedAt     string   `json:"retained_at,omitempty"`
}

type ReleaseWorkspaceRequest struct {
	StationID      string `json:"station_id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Namespace      string `json:"namespace"`
	PVCName        string `json:"pvc_name"`
}

type runtimeWorkspaceRow struct {
	InstanceID     string
	OrganizationID string
	ProjectID      string
	ProfileID      string
	Namespace      string
	PoolName       string
	PVCName        string
	PVName         string
	NodeName       string
	ReservedBytes  int64
	CreatedAt      time.Time
	RetainedAt     *time.Time
	LastStatus     string
	LastPhase      string
	LastAction     string
}

type appWorkspaceRow struct {
	InstanceID     string
	OrganizationID string
	InstallationID string
	Namespace      string
	PVCName        string
	PVName         string
	CreatedAt      time.Time
	AppID          string
	HasActiveStart bool
	LastPhase      string
}

func inferAppMeta(namespace, pool, appID string) (string, string) {
	id := strings.ToLower(strings.TrimSpace(appID))
	if id == "" {
		switch {
		case namespace == "verdantflare-comfyui" || strings.HasPrefix(pool, "comfyui"):
			id = "comfyui"
		case namespace == "verdantflare-blender" || strings.HasPrefix(pool, "blender"):
			id = "blender"
		default:
			id = strings.TrimPrefix(namespace, "verdantflare-")
		}
	}
	switch id {
	case "comfyui":
		return "comfyui", "ComfyUI"
	case "blender":
		return "blender", "Blender"
	default:
		return id, id
	}
}

func (s *Service) workspaceNamespaces(extra ...string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(ns string) {
		ns = strings.TrimSpace(ns)
		if ns == "" || seen[ns] {
			return
		}
		seen[ns] = true
		out = append(out, ns)
	}
	for _, p := range s.Profiles {
		add(p.Namespace)
	}
	for _, ns := range extra {
		add(ns)
	}
	add("verdantflare-blender")
	add("verdantflare-comfyui")
	sort.Strings(out)
	return out
}

func (s *Service) Workspaces(ctx context.Context) ([]WorkspaceVolume, error) {
	if s == nil || s.Pool == nil || s.Client == nil || s.StationID == "" {
		return nil, errors.New("workspace service not configured")
	}
	runtimeMap := map[string]runtimeWorkspaceRow{}
	appMap := map[string]appWorkspaceRow{}
	var dbNamespaces []string

	rows, err := s.Pool.Query(ctx, `
		SELECT w.instance_id::text, w.organization_id::text, w.project_id::text, w.profile_id,
		       w.namespace, w.pool_name, w.pvc_name, w.pv_name, w.node_name, w.reserved_bytes,
		       w.created_at, w.retained_at,
		       COALESCE((SELECT e.status FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.phase FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.action FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), '')
		FROM station.runtime_instance_workspaces w
		WHERE w.station_id = $1`, s.StationID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var r runtimeWorkspaceRow
		if err := rows.Scan(&r.InstanceID, &r.OrganizationID, &r.ProjectID, &r.ProfileID,
			&r.Namespace, &r.PoolName, &r.PVCName, &r.PVName, &r.NodeName, &r.ReservedBytes,
			&r.CreatedAt, &r.RetainedAt, &r.LastStatus, &r.LastPhase, &r.LastAction); err != nil {
			rows.Close()
			return nil, err
		}
		runtimeMap[r.Namespace+"/"+r.PVCName] = r
		dbNamespaces = append(dbNamespaces, r.Namespace)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	appRows, err := s.Pool.Query(ctx, `
		SELECT w.instance_id::text, w.organization_id::text, w.installation_id::text,
		       w.namespace, w.pvc_name, w.pv_name, w.created_at,
		       COALESCE(i.app_id, ''),
		       EXISTS(SELECT 1 FROM station.app_instance_starts s WHERE s.instance_id = w.instance_id AND s.phase <> 'stopped'),
		       COALESCE((SELECT s.phase FROM station.app_instance_starts s WHERE s.instance_id = w.instance_id ORDER BY s.generation DESC LIMIT 1), '')
		FROM station.app_instance_workspaces w
		LEFT JOIN station.app_installations i ON i.installation_id = w.installation_id
		WHERE w.station_id = $1`, s.StationID)
	if err != nil {
		return nil, err
	}
	for appRows.Next() {
		var r appWorkspaceRow
		if err := appRows.Scan(&r.InstanceID, &r.OrganizationID, &r.InstallationID,
			&r.Namespace, &r.PVCName, &r.PVName, &r.CreatedAt, &r.AppID, &r.HasActiveStart, &r.LastPhase); err != nil {
			appRows.Close()
			return nil, err
		}
		appMap[r.Namespace+"/"+r.PVCName] = r
		dbNamespaces = append(dbNamespaces, r.Namespace)
	}
	appRows.Close()
	if err := appRows.Err(); err != nil {
		return nil, err
	}

	namespaces := s.workspaceNamespaces(dbNamespaces...)
	out := []WorkspaceVolume{}
	seenKeys := map[string]bool{}

	for _, ns := range namespaces {
		pvcList, err := s.Client.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			if apierrors.IsForbidden(err) || apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		podList, err := s.Client.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil && !apierrors.IsForbidden(err) && !apierrors.IsNotFound(err) {
			return nil, err
		}
		mountedClaims := map[string]bool{}
		if podList != nil {
			for _, pod := range podList.Items {
				if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
					continue
				}
				for _, vol := range pod.Spec.Volumes {
					if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName != "" {
						mountedClaims[vol.PersistentVolumeClaim.ClaimName] = true
					}
				}
			}
		}

		for _, pvc := range pvcList.Items {
			if pvc.Labels[stationKey] != s.StationID || pvc.Labels[poolKey] == "" {
				continue
			}
			key := ns + "/" + pvc.Name
			seenKeys[key] = true
			pool := pvc.Labels[poolKey]
			storageBytes := pvc.Status.Capacity.Storage().Value()
			if storageBytes <= 0 {
				if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
					storageBytes = req.Value()
				}
			}
			storageClass := value(pvc.Spec.StorageClassName)
			if storageClass == "" {
				storageClass = "local-nvme"
			}
			accessMode := "RWO"
			if len(pvc.Spec.AccessModes) > 0 && pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
				accessMode = string(pvc.Spec.AccessModes[0])
			}
			nodeName := pvc.Annotations["volume.kubernetes.io/selected-node"]
			if nodeName == "" {
				nodeName = "verdentflare-5090"
			}
			appID, appDisplay := inferAppMeta(ns, pool, pvc.Annotations["station.verdantflare.com/app-id"])

			item := WorkspaceVolume{
				WorkspaceID:    key,
				PVCName:        pvc.Name,
				PVName:         pvc.Spec.VolumeName,
				Namespace:      ns,
				PoolName:       pool,
				AppID:          appID,
				AppDisplayName: appDisplay,
				StorageBytes:   storageBytes,
				StorageClass:   storageClass,
				AccessMode:     accessMode,
				NodeName:       nodeName,
				State:          "available",
				AllowedActions: []string{},
			}

			if r, ok := runtimeMap[key]; ok {
				if item.PVName == "" {
					item.PVName = r.PVName
				}
				if r.NodeName != "" {
					item.NodeName = r.NodeName
				}
				if item.StorageBytes <= 0 {
					item.StorageBytes = r.ReservedBytes
				}
				item.InstanceID = r.InstanceID
				item.OrganizationID = r.OrganizationID
				item.ProjectID = r.ProjectID
				item.BoundAt = r.CreatedAt.UTC().Format(time.RFC3339)
				if r.RetainedAt != nil || (r.LastAction == "destroy" && r.LastStatus == "succeeded") {
					item.State = "retained"
					item.AllowedActions = []string{"release"}
					if r.RetainedAt != nil {
						item.RetainedAt = r.RetainedAt.UTC().Format(time.RFC3339)
					}
				} else if mountedClaims[pvc.Name] || r.LastStatus == "running" || r.LastPhase == "running" {
					item.State = "in_use"
				} else {
					item.State = "bound_stopped"
				}
			} else if a, ok := appMap[key]; ok {
				if item.PVName == "" {
					item.PVName = a.PVName
				}
				if a.AppID != "" {
					item.AppID, item.AppDisplayName = inferAppMeta(ns, pool, a.AppID)
				}
				item.InstanceID = a.InstanceID
				item.OrganizationID = a.OrganizationID
				item.InstallationID = a.InstallationID
				item.BoundAt = a.CreatedAt.UTC().Format(time.RFC3339)
				if retainedAt := pvc.Annotations["station.verdantflare.com/retained-at"]; retainedAt != "" {
					item.State = "retained"
					item.RetainedAt = retainedAt
					item.AllowedActions = []string{"release"}
				} else if a.HasActiveStart || mountedClaims[pvc.Name] {
					item.State = "in_use"
				} else {
					item.State = "bound_stopped"
				}
			}

			out = append(out, item)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].PVCName < out[j].PVCName
	})
	return out, nil
}

func (s *Service) ReleaseWorkspace(ctx context.Context, in ReleaseWorkspaceRequest) (WorkspaceVolume, error) {
	if s == nil || s.Pool == nil || s.Client == nil || s.StationID == "" {
		return WorkspaceVolume{}, errors.New("workspace service not configured")
	}
	for _, idStr := range []string{in.StationID, in.OrganizationID, in.UserID, in.IdempotencyKey} {
		parsed, err := uuid.Parse(idStr)
		if err != nil || parsed == uuid.Nil || parsed.String() != idStr {
			return WorkspaceVolume{}, ErrInvalid
		}
	}
	if in.StationID != s.StationID || len(validation.IsDNS1123Label(in.Namespace)) != 0 || in.Namespace == "default" || len(validation.IsDNS1123Subdomain(in.PVCName)) != 0 {
		return WorkspaceVolume{}, ErrInvalid
	}

	pvc, err := s.Client.CoreV1().PersistentVolumeClaims(in.Namespace).Get(ctx, in.PVCName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return WorkspaceVolume{}, ErrNotFound
	}
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if pvc.Labels[stationKey] != s.StationID || pvc.Labels[poolKey] == "" {
		return WorkspaceVolume{}, ErrNotFound
	}

	if err := s.verifyNoActiveMounts(ctx, in.Namespace, in.PVCName); err != nil {
		return WorkspaceVolume{}, err
	}

	nodeName := pvc.Annotations["volume.kubernetes.io/selected-node"]
	if nodeName == "" {
		nodeName = "verdentflare-5090"
	}

	// Pre-check DB binding state before running disk cleanup.
	preKind, preInstanceID, preNode, err := s.inspectWorkspaceBinding(ctx, in.Namespace, in.PVCName)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if preNode != "" {
		nodeName = preNode
	}

	if preKind != "" && s.WorkspaceCleaner != nil {
		if err := s.WorkspaceCleaner(ctx, in.Namespace, in.PVCName, nodeName); err != nil {
			return WorkspaceVolume{}, err
		}
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-workspaces:"+in.StationID); err != nil {
		return WorkspaceVolume{}, err
	}

	kind, instanceID, secretName, err := s.lockAndValidateRelease(ctx, tx, in.Namespace, in.PVCName)
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if preInstanceID != "" && instanceID != "" && preInstanceID != instanceID {
		return WorkspaceVolume{}, ErrBinding
	}

	if err := s.verifyNoActiveMounts(ctx, in.Namespace, in.PVCName); err != nil {
		return WorkspaceVolume{}, err
	}
	if instanceID != "" {
		services, err := s.Client.CoreV1().Services(in.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return WorkspaceVolume{}, err
		}
		for _, svc := range services.Items {
			if svc.Annotations[instanceKey] == instanceID {
				return WorkspaceVolume{}, ErrBusy
			}
		}
	}

	if secretName != "" {
		if err := s.Client.CoreV1().Secrets(in.Namespace).Delete(ctx, secretName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return WorkspaceVolume{}, err
		}
	}

	pvc, err = s.Client.CoreV1().PersistentVolumeClaims(in.Namespace).Get(ctx, in.PVCName, metav1.GetOptions{})
	if err != nil {
		return WorkspaceVolume{}, err
	}
	if pvc.Labels[stationKey] != s.StationID || pvc.Labels[poolKey] == "" {
		return WorkspaceVolume{}, ErrBinding
	}
	if err := clearPVCStationAnnotations(ctx, s.Client, pvc); err != nil {
		return WorkspaceVolume{}, err
	}

	switch kind {
	case "runtime":
		if _, err := tx.Exec(ctx, `DELETE FROM station.runtime_instance_workspaces WHERE instance_id=$1 AND station_id=$2 AND namespace=$3 AND pvc_name=$4`,
			instanceID, in.StationID, in.Namespace, in.PVCName); err != nil {
			return WorkspaceVolume{}, err
		}
	case "app":
		if _, err := tx.Exec(ctx, `DELETE FROM station.app_instance_stops WHERE start_operation_id IN (SELECT operation_id FROM station.app_instance_starts WHERE instance_id=$1)`, instanceID); err != nil {
			return WorkspaceVolume{}, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM station.app_instance_starts WHERE instance_id=$1`, instanceID); err != nil {
			return WorkspaceVolume{}, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM station.app_instance_workspaces WHERE instance_id=$1 AND station_id=$2 AND namespace=$3 AND pvc_name=$4`,
			instanceID, in.StationID, in.Namespace, in.PVCName); err != nil {
			return WorkspaceVolume{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return WorkspaceVolume{}, err
	}

	pool := pvc.Labels[poolKey]
	storageBytes := pvc.Status.Capacity.Storage().Value()
	if storageBytes <= 0 {
		if req, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			storageBytes = req.Value()
		}
	}
	storageClass := value(pvc.Spec.StorageClassName)
	if storageClass == "" {
		storageClass = "local-nvme"
	}
	appID, appDisplay := inferAppMeta(in.Namespace, pool, "")
	return WorkspaceVolume{
		WorkspaceID:    in.Namespace + "/" + in.PVCName,
		PVCName:        in.PVCName,
		PVName:         pvc.Spec.VolumeName,
		Namespace:      in.Namespace,
		PoolName:       pool,
		AppID:          appID,
		AppDisplayName: appDisplay,
		StorageBytes:   storageBytes,
		StorageClass:   storageClass,
		AccessMode:     "RWO",
		NodeName:       nodeName,
		State:          "available",
		AllowedActions: []string{},
	}, nil
}

func (s *Service) verifyNoActiveMounts(ctx context.Context, namespace, pvcName string) error {
	pods, err := s.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvcName {
				return ErrBusy
			}
		}
	}
	return nil
}

func (s *Service) inspectWorkspaceBinding(ctx context.Context, namespace, pvcName string) (string, string, string, error) {
	var instanceID, nodeName, lastStatus, lastPhase, lastAction string
	var retained bool
	err := s.Pool.QueryRow(ctx, `
		SELECT w.instance_id::text, w.node_name, w.retained_at IS NOT NULL,
		       COALESCE((SELECT e.status FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.phase FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.action FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), '')
		FROM station.runtime_instance_workspaces w
		WHERE w.station_id=$1 AND w.namespace=$2 AND w.pvc_name=$3`, s.StationID, namespace, pvcName).
		Scan(&instanceID, &nodeName, &retained, &lastStatus, &lastPhase, &lastAction)
	if err == nil {
		if lastStatus == "running" || lastPhase == "running" {
			return "", "", "", ErrBusy
		}
		if !retained && !(lastAction == "destroy" && lastStatus == "succeeded") {
			return "", "", "", ErrNotDestroyed
		}
		return "runtime", instanceID, nodeName, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", err
	}

	var activeStart, activeStop bool
	err = s.Pool.QueryRow(ctx, `
		SELECT w.instance_id::text,
		       EXISTS(SELECT 1 FROM station.app_instance_starts s WHERE s.instance_id = w.instance_id AND s.phase <> 'stopped'),
		       EXISTS(SELECT 1 FROM station.app_instance_stops t JOIN station.app_instance_starts s ON s.operation_id = t.start_operation_id WHERE s.instance_id = w.instance_id AND t.phase <> 'stopped')
		FROM station.app_instance_workspaces w
		WHERE w.station_id=$1 AND w.namespace=$2 AND w.pvc_name=$3`, s.StationID, namespace, pvcName).
		Scan(&instanceID, &activeStart, &activeStop)
	if err == nil {
		if activeStart || activeStop {
			return "", "", "", ErrBusy
		}
		return "app", instanceID, "", nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", nil
	}
	return "", "", "", err
}

func (s *Service) lockAndValidateRelease(ctx context.Context, tx pgx.Tx, namespace, pvcName string) (string, string, string, error) {
	var instanceID, lastStatus, lastPhase, lastAction string
	var retained bool
	err := tx.QueryRow(ctx, `
		SELECT w.instance_id::text, w.retained_at IS NOT NULL,
		       COALESCE((SELECT e.status FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.phase FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), ''),
		       COALESCE((SELECT e.action FROM station.runtime_instance_executions e WHERE e.instance_id = w.instance_id ORDER BY e.created_at DESC, e.operation_id DESC LIMIT 1), '')
		FROM station.runtime_instance_workspaces w
		WHERE w.station_id=$1 AND w.namespace=$2 AND w.pvc_name=$3`, s.StationID, namespace, pvcName).
		Scan(&instanceID, &retained, &lastStatus, &lastPhase, &lastAction)
	if err == nil {
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance:"+s.StationID+":"+instanceID).Scan(&locked); err != nil {
			return "", "", "", err
		}
		if !locked || lastStatus == "running" || lastPhase == "running" {
			return "", "", "", ErrBusy
		}
		if !retained && !(lastAction == "destroy" && lastStatus == "succeeded") {
			return "", "", "", ErrNotDestroyed
		}
		return "runtime", instanceID, "blender-" + strings.ReplaceAll(instanceID, "-", ""), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", err
	}

	var activeStart, activeStop bool
	err = tx.QueryRow(ctx, `
		SELECT w.instance_id::text,
		       EXISTS(SELECT 1 FROM station.app_instance_starts s WHERE s.instance_id = w.instance_id AND s.phase <> 'stopped'),
		       EXISTS(SELECT 1 FROM station.app_instance_stops t JOIN station.app_instance_starts s ON s.operation_id = t.start_operation_id WHERE s.instance_id = w.instance_id AND t.phase <> 'stopped')
		FROM station.app_instance_workspaces w
		WHERE w.station_id=$1 AND w.namespace=$2 AND w.pvc_name=$3`, s.StationID, namespace, pvcName).
		Scan(&instanceID, &activeStart, &activeStop)
	if err == nil {
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance:"+s.StationID+":"+instanceID).Scan(&locked); err != nil {
			return "", "", "", err
		}
		if !locked || activeStart || activeStop {
			return "", "", "", ErrBusy
		}
		return "app", instanceID, "appi-" + strings.ReplaceAll(instanceID, "-", ""), nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", "", nil
	}
	return "", "", "", err
}

func clearPVCStationAnnotations(ctx context.Context, client kubernetes.Interface, pvc *corev1.PersistentVolumeClaim) error {
	changed := false
	for k := range pvc.Annotations {
		if strings.HasPrefix(k, "station.verdantflare.com/") {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}
	next := pvc.DeepCopy()
	for k := range next.Annotations {
		if strings.HasPrefix(k, "station.verdantflare.com/") {
			delete(next.Annotations, k)
		}
	}
	_, err := client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, next, metav1.UpdateOptions{})
	return err
}

func CleanWorkspacePod(ctx context.Context, client kubernetes.Interface, namespace, pvcName, nodeName, image string, runAsUser int64) error {
	if image == "" {
		return nil
	}
	podName := "ws-clean-" + pvcName
	zero := int64(0)
	_ = client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	for range 20 {
		if _, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	no, yes := false, true
	if runAsUser <= 0 {
		runAsUser = 1000
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/component": "workspace-cleaner",
			},
		},
		Spec: corev1.PodSpec{
			AutomountServiceAccountToken:  &no,
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &zero,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &yes,
				RunAsUser:      &runAsUser,
				RunAsGroup:     &runAsUser,
				FSGroup:        &runAsUser,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            "clean",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{
					"python3", "-c",
					"import os, shutil\nroot = '/workspace'\nfor n in os.listdir(root):\n    if n == 'lost+found':\n        continue\n    p = os.path.join(root, n)\n    if os.path.isdir(p) and not os.path.islink(p):\n        shutil.rmtree(p)\n    else:\n        os.unlink(p)\n",
				},
				Env: []corev1.EnvVar{{Name: "NVIDIA_VISIBLE_DEVICES", Value: "void"}},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &no,
					Privileged:               &no,
					ReadOnlyRootFilesystem:   &yes,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "workspace",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
	}
	if nodeName != "" {
		pod.Spec.NodeSelector = map[string]string{corev1.LabelHostname: nodeName}
	}
	if namespace == "verdantflare-comfyui" {
		prio := int32(0)
		preempt := corev1.PreemptNever
		pod.Spec.PriorityClassName = "station-app-worker"
		pod.Spec.Priority = &prio
		pod.Spec.PreemptionPolicy = &preempt
	}

	if _, err := client.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return err
	}
	defer func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.CoreV1().Pods(namespace).Delete(cleanCtx, podName, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		for range 25 {
			if _, err := client.CoreV1().Pods(namespace).Get(cleanCtx, podName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	for {
		cur, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cur.Status.Phase == corev1.PodSucceeded {
			return nil
		}
		if cur.Status.Phase == corev1.PodFailed {
			return errors.New("workspace cleanup pod failed")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}
