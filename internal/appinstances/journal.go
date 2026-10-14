package appinstances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/instances"
	"github.com/verdantflarehub/verdantflare-station-runtime/internal/strictjson"
	"k8s.io/client-go/kubernetes"
)

var ErrConflict = errors.New("INSTANCE_OPERATION_CONFLICT")
var ErrDenied = errors.New("INSTANCE_INSTALLATION_DENIED")

// Journal has no HTTP surface. Authorize must check the current Core manager and
// installed App and worker compatibility on EVERY call, including replay.
// The returned revision is a fresh grant; old journal revisions are history.
type Journal struct {
	Pool      *pgxpool.Pool
	Client    kubernetes.Interface
	StationID string
	Authorize func(context.Context, Run, Profile) (int64, error)
	Observer  WorkerObserver
	Stopper   WorkerStopper
}

type StartRecord struct {
	Run                  Run
	Volume               Volume
	ProfileHash          string
	InstallationRevision int64
	Phase                string
	Observation          Observation
	Revision             int64
}

func (j *Journal) valid(in Run, p Profile) bool {
	return j != nil && j.Pool != nil && j.Client != nil && j.Authorize != nil && in.StationID == j.StationID &&
		in.Identity.valid() && validID(in.OperationID) && in.Generation > 0 && p.Valid()
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func instanceLock(ctx context.Context, tx pgx.Tx, in Run) error {
	var locked bool
	if e := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "runtime-instance:"+in.StationID+":"+in.InstanceID).Scan(&locked); e != nil {
		return e
	}
	if !locked {
		return ErrBusy
	}
	return nil
}

const recordColumns = `s.input,s.profile_hash,s.installation_revision,s.phase,s.observation,s.revision,w.binding`

func scanRecord(row pgx.Row) (StartRecord, error) {
	var r StartRecord
	var input, observation, binding []byte
	if e := row.Scan(&input, &r.ProfileHash, &r.InstallationRevision, &r.Phase, &observation, &r.Revision, &binding); e != nil {
		return r, e
	}
	if strictjson.Decode(bytes.NewReader(input), 4096, &r.Run) != nil ||
		strictjson.Decode(bytes.NewReader(binding), 4096, &r.Volume) != nil ||
		strictjson.Decode(bytes.NewReader(observation), 4096, &r.Observation) != nil || !r.Observation.valid() || r.Revision < 1 {
		return StartRecord{}, ErrBinding
	}
	return r, nil
}

func sameRecord(r StartRecord, in Run, p Profile, revision int64) bool {
	return r.Run == in && r.ProfileHash == p.Hash() && r.InstallationRevision > 0 && r.InstallationRevision <= revision && validVolume(in.Identity, p, r.Volume)
}

// Authorize is followed by an installation row check in the acceptance transaction
// so removal or a revision change after authorization cannot receive a binding.
// Core has already authorized this worker version for that exact revision.
type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func installationMatches(ctx context.Context, tx rowQuerier, in Run, p Profile, revision int64) error {
	var ok bool
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_installations WHERE installation_id=$1 AND station_id=$2 AND organization_id=$3 AND app_id=$4 AND state='installed' AND revision=$5)`,
		in.InstallationID, in.StationID, in.OrganizationID, p.AppID, revision).Scan(&ok); e != nil {
		return e
	}
	if revision < 1 || !ok {
		return ErrDenied
	}
	return nil
}

// Accept commits a start and its volume reservation together. It performs only
// Kubernetes reads, so a failed transaction cannot leave a worker or credential.
func (j *Journal) Accept(ctx context.Context, in Run, p Profile) (StartRecord, error) {
	if !j.valid(in, p) {
		return StartRecord{}, ErrInvalid
	}
	tx, e := j.Pool.Begin(ctx)
	if e != nil {
		return StartRecord{}, e
	}
	defer rollback(tx)
	if e = instanceLock(ctx, tx, in); e != nil {
		return StartRecord{}, e
	}
	revision, e := j.Authorize(ctx, in, p)
	if e != nil {
		return StartRecord{}, e
	}
	if e = installationMatches(ctx, tx, in, p, revision); e != nil {
		return StartRecord{}, e
	}
	old, e := scanRecord(tx.QueryRow(ctx, `SELECT `+recordColumns+` FROM station.app_instance_starts s JOIN station.app_instance_workspaces w USING(instance_id) WHERE operation_id=$1`, in.OperationID))
	if e == nil {
		if !sameRecord(old, in, p, revision) {
			return StartRecord{}, ErrConflict
		}
		return old, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return StartRecord{}, e
	}
	var previousGeneration int64
	var previousPhase string
	var previousObservation []byte
	e = tx.QueryRow(ctx, `SELECT generation,phase,observation FROM station.app_instance_starts WHERE instance_id=$1 ORDER BY generation DESC LIMIT 1`, in.InstanceID).Scan(&previousGeneration, &previousPhase, &previousObservation)
	initial := Observation{}
	if errors.Is(e, pgx.ErrNoRows) {
		if in.Generation != 1 {
			return StartRecord{}, ErrConflict
		}
	} else if e != nil {
		return StartRecord{}, e
	} else {
		if previousPhase != "stopped" || previousGeneration >= in.Generation || in.Generation-previousGeneration != 1 {
			return StartRecord{}, ErrConflict
		}
		var prior Observation
		if strictjson.Decode(bytes.NewReader(previousObservation), 4096, &prior) != nil || !prior.valid() || prior.SecretUID == "" {
			return StartRecord{}, ErrBinding
		}
		// Credentials belong to the instance, while Pod and Service belong to a run.
		initial.SecretUID = prior.SecretUID
	}
	b, e := j.reserve(ctx, tx, in, p)
	if e != nil {
		return StartRecord{}, e
	}
	input, _ := json.Marshal(in)
	observed, _ := json.Marshal(initial)
	_, e = tx.Exec(ctx, `INSERT INTO station.app_instance_starts(operation_id,instance_id,generation,installation_revision,profile_hash,input,phase,observation) VALUES($1,$2,$3,$4,$5,$6,'accepted',$7)`,
		in.OperationID, in.InstanceID, in.Generation, revision, p.Hash(), input, observed)
	if e != nil {
		return StartRecord{}, e
	}
	r := StartRecord{Run: in, Volume: b, ProfileHash: p.Hash(), InstallationRevision: revision, Phase: "accepted", Observation: initial, Revision: 1}
	return r, tx.Commit(ctx)
}

func (j *Journal) reserve(ctx context.Context, tx pgx.Tx, in Run, p Profile) (Volume, error) {
	// Shared with the legacy allocator. Both query both tables under this lock.
	if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runtime-workspaces:"+in.StationID); e != nil {
		return Volume{}, e
	}
	var raw []byte
	e := tx.QueryRow(ctx, `SELECT binding FROM station.app_instance_workspaces WHERE instance_id=$1`, in.InstanceID).Scan(&raw)
	if e == nil {
		var b Volume
		if strictjson.Decode(bytes.NewReader(raw), 4096, &b) != nil || !validVolume(in.Identity, p, b) {
			return Volume{}, ErrBinding
		}
		return b, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return Volume{}, e
	}
	for _, claim := range p.Claims {
		var used bool
		e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_instance_workspaces WHERE station_id=$1 AND namespace=$2 AND pvc_name=$3 UNION ALL SELECT 1 FROM station.runtime_instance_workspaces WHERE station_id=$1 AND namespace=$2 AND pvc_name=$3)`, in.StationID, p.Namespace, claim).Scan(&used)
		if e != nil {
			return Volume{}, e
		}
		if used {
			continue
		}
		b, e := InspectVolume(ctx, j.Client, in.Identity, p, claim, nil, nil)
		if errors.Is(e, ErrCapacity) {
			if err := instances.EnsurePoolClaim(ctx, j.Client, in.StationID, p.Namespace, p.Pool, claim); err != nil {
				return Volume{}, err
			}
			b, e = InspectVolume(ctx, j.Client, in.Identity, p, claim, nil, nil)
		}
		if errors.Is(e, ErrBinding) || errors.Is(e, ErrBusy) || errors.Is(e, ErrCapacity) {
			continue
		}
		if e != nil {
			return Volume{}, e
		}
		e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_instance_workspaces WHERE pvc_uid=$1 OR pv_uid=$2 UNION ALL SELECT 1 FROM station.runtime_instance_workspaces WHERE pvc_uid=$1 OR pv_uid=$2)`, b.PVCUID, b.PVUID).Scan(&used)
		if e != nil {
			return Volume{}, e
		}
		if used {
			continue
		}
		raw, _ := json.Marshal(b)
		_, e = tx.Exec(ctx, `INSERT INTO station.app_instance_workspaces(instance_id,station_id,organization_id,user_id,installation_id,profile_hash,namespace,pvc_name,pvc_uid,pv_name,pv_uid,binding) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			in.InstanceID, in.StationID, in.OrganizationID, in.UserID, in.InstallationID, p.Hash(), p.Namespace, b.PVCName, b.PVCUID, b.PVName, b.PVUID, raw)
		return b, e
	}
	return Volume{}, ErrCapacity
}

func monotonicObservation(previous, next Observation) bool {
	if !previous.valid() || !next.valid() {
		return false
	}
	return (previous.SecretUID == "" || previous.SecretUID == next.SecretUID) &&
		(previous.ServiceUID == "" || previous.ServiceUID == next.ServiceUID) &&
		(previous.PodUID == "" || previous.PodUID == next.PodUID) &&
		(previous.BootID == "" || previous.BootID == next.BootID)
}

// Each checkpoint commits on the same connection holding the session advisory
// lock. No transaction spans Kubernetes effects or rolls back earlier UID writes.
func (j *Journal) checkpoint(ctx context.Context, conn *pgxpool.Conn, r *StartRecord, next Observation) error {
	if !monotonicObservation(r.Observation, next) || (r.Phase != "accepted" && r.Phase != "launching") {
		return ErrBinding
	}
	raw, _ := json.Marshal(next)
	phase := "launching"
	if next.BootID != "" {
		if !next.PodReady {
			return ErrNotReady
		}
		phase = "running"
	}
	result, e := conn.Exec(ctx, `UPDATE station.app_instance_starts SET observation=$1,phase=$8,revision=revision+1,updated_at=now() WHERE operation_id=$2 AND instance_id=$3 AND generation=$4 AND profile_hash=$5 AND installation_revision=$6 AND revision=$7 AND phase IN ('accepted','launching')`,
		raw, r.Run.OperationID, r.Run.InstanceID, r.Run.Generation, r.ProfileHash, r.InstallationRevision, r.Revision, phase)
	if e != nil {
		return e
	}
	if result.RowsAffected() != 1 {
		return ErrConflict
	}
	r.Observation, r.Phase, r.Revision = next, phase, r.Revision+1
	return nil
}

// withRecord holds the instance lock through a fresh authorization and visit.
func (j *Journal) withRecord(ctx context.Context, in Run, p Profile, visit func(*pgxpool.Conn, *StartRecord) error) (StartRecord, error) {
	return j.withLockedRecord(ctx, in, p, false, visit)
}

// Access is read-only and can share an instance lock with other access checks.
// Every mutating visit retains the exclusive lock on the same key.
func (j *Journal) withLockedRecord(ctx context.Context, in Run, p Profile, shared bool, visit func(*pgxpool.Conn, *StartRecord) error) (StartRecord, error) {
	if !j.valid(in, p) {
		return StartRecord{}, ErrInvalid
	}
	conn, e := j.Pool.Acquire(ctx)
	if e != nil {
		return StartRecord{}, e
	}
	defer conn.Release()
	key := "runtime-instance:" + in.StationID + ":" + in.InstanceID
	acquire := `SELECT pg_try_advisory_lock(hashtextextended($1,0))`
	release := `SELECT pg_advisory_unlock(hashtextextended($1,0))`
	if shared {
		acquire = `SELECT pg_try_advisory_lock_shared(hashtextextended($1,0))`
		release = `SELECT pg_advisory_unlock_shared(hashtextextended($1,0))`
	}
	var locked bool
	if e = conn.QueryRow(ctx, acquire, key).Scan(&locked); e != nil {
		// A lost acquisition response leaves lock ownership uncertain. Never put
		// this session back in the pool where it could retain an invisible lock.
		discardConnection(conn)
		return StartRecord{}, e
	}
	if !locked {
		return StartRecord{}, ErrBusy
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if e := conn.QueryRow(cleanup, release, key).Scan(&unlocked); e != nil || !unlocked {
			discardConnection(conn)
		}
	}()
	revision, e := j.Authorize(ctx, in, p)
	if e != nil {
		return StartRecord{}, e
	}
	if e = installationMatches(ctx, conn, in, p, revision); e != nil {
		return StartRecord{}, e
	}
	r, e := scanRecord(conn.QueryRow(ctx, `SELECT `+recordColumns+` FROM station.app_instance_starts s JOIN station.app_instance_workspaces w USING(instance_id) WHERE operation_id=$1`, in.OperationID))
	if e != nil {
		return StartRecord{}, e
	}
	if !sameRecord(r, in, p, revision) {
		return StartRecord{}, ErrConflict
	}
	e = visit(conn, &r)
	return r, e
}

// AdvanceResources advances a committed start; access does not call this method.
func (j *Journal) AdvanceResources(ctx context.Context, in Run, p Profile) (StartRecord, error) {
	return j.withRecord(ctx, in, p, func(conn *pgxpool.Conn, r *StartRecord) error {
		if r.Phase != "accepted" && r.Phase != "launching" && r.Phase != "running" {
			return ErrConflict
		}
		if r.Phase == "running" {
			if r.Observation.BootID == "" {
				return ErrBinding
			}
			_, e := j.verifyWorker(ctx, in, p, *r)
			return e
		}
		e := volumeClaimed(ctx, j.Client, in.Identity, p, r.Volume)
		if errors.Is(e, ErrBinding) {
			e = ClaimVolume(ctx, j.Client, in.Identity, p, r.Volume)
		}
		if e != nil {
			return e
		}
		_, e = EnsureWorker(ctx, j.Client, in, p, r.Volume, r.Observation, func(ctx context.Context, next Observation) error {
			return j.checkpoint(ctx, conn, r, next)
		})
		if e == nil && r.Observation.PodReady && j.Observer != nil {
			var verified Observation
			verified, e = j.verifyWorker(ctx, in, p, *r)
			if e == nil {
				e = j.checkpoint(ctx, conn, r, verified)
			}
		}
		return e
	})
}

func (j *Journal) Start(ctx context.Context, in Run, p Profile) (StartRecord, error) {
	if j == nil || j.Observer == nil {
		return StartRecord{}, ErrUnavailable
	}
	if _, e := j.Accept(ctx, in, p); e != nil {
		return StartRecord{}, e
	}
	return j.AdvanceResources(ctx, in, p)
}

func discardConnection(conn *pgxpool.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Hijack().Close(ctx)
}
