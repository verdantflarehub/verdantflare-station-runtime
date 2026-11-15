package modelcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/google/uuid"
)

type DiskStatFunc func(rootDir string) (totalBytes, freeBytes int64, err error)

// FileIdentity captures symlink-free physical inode / volume metadata.
type FileIdentity struct {
	Dev   uint64
	Ino   uint64
	Nlink uint64
	Perm  os.FileMode
	Uid   uint32
	Size  int64
}

// WriterProbeState distinguishes proven kernel-enforced writer absence from active
// writers and unprovable environments (e.g., F_SETLEASE EPERM/EINVAL/ENOSYS, R3-H01).
type WriterProbeState int

const (
	WriterProbeProvenSafe   WriterProbeState = iota // Kernel-proven zero open writable FDs on this inode
	WriterProbeActiveWriter                         // Active writer lock or open writable FD detected
	WriterProbeUnprovable                           // Cannot prove absence of non-cooperative O_RDWR FDs
)

// classifyLinuxWriterProbe classifies (flock(LOCK_EX|LOCK_NB), fcntl(F_SETLEASE, F_RDLCK))
// outcomes strictly: advisory flock success NEVER substitutes for a failed F_SETLEASE probe (R3-H01).
func classifyLinuxWriterProbe(flockErr, setLeaseErr error) WriterProbeState {
	if flockErr != nil {
		return WriterProbeActiveWriter
	}
	if setLeaseErr == nil {
		return WriterProbeProvenSafe
	}
	if errors.Is(setLeaseErr, syscall.EAGAIN) || errors.Is(setLeaseErr, syscall.EACCES) || errors.Is(setLeaseErr, syscall.EBUSY) {
		return WriterProbeActiveWriter
	}
	return WriterProbeUnprovable
}

// FenceArtifacts carries the exact FileIdentity values observed when fence-owned
// staging / temporary-view entries were created or opened so failure cleanup NEVER
// degrades to unverified path-based deletion (R3-H02).
type FenceArtifacts struct {
	PartID     *FileIdentity
	TempViewID *FileIdentity
}

// Fault-injection and race-barrier hooks used exclusively in unit tests
// (T32-M05, R2-H02, R2-M02, R3-H01, R3-H02).
var (
	testHookSetReadOnly                 func(relPath string) error
	testHookFsyncDir                    func(relDir string) error
	testHookAfterFinalStatBeforeSyscall func(op, relPath string)
	testHookWriterProbeOverride         func(op, relPath string) *WriterProbeState
	quarantineSeq                       uint64
	nsProcessLocks                      sync.Map // map[string]*sync.Mutex
)

// platformNamespace holds an exclusive cross-process cache lock (.ns.lock)
// together with pinned, symlink-free directory descriptors/handles for a
// single cache rootDir so stat/link/rename/quarantine-unlink execute on bound
// handles without re-walking untrusted path prefixes (R2-H02, R3-H02).
type platformNamespace interface {
	Close()
	RootDev() uint64
	StatInDir(relDirSlash, baseName string) (FileIdentity, error)
	PinInodeForQuarantine(relDirSlash, baseName string) (io.Closer, FileIdentity, error)
	ListDirNames(relDirSlash string) ([]string, error)
	OpenReadOnlyInDir(relDirSlash, baseName string) (*os.File, FileIdentity, error)
	LinkOpenFileToDir(srcFile *os.File, expectedID FileIdentity, dstRelDirSlash, dstBaseName string) error
	RenameNoReplaceInDirs(srcRelDirSlash, srcBaseName, dstRelDirSlash, dstBaseName string) error
	UnlinkInQuarantine(qName string, restoreReadOnlyRelDir, restoreReadOnlyBase string) error
	TryExclusiveHandoffPart(prevBase, dstBase string, expectedID FileIdentity) (handedOff bool, probeState WriterProbeState, err error)
	CopyPartToIndependentInode(prevBase, dstBase string, copyBytes int64, prevIno uint64) (FileIdentity, error)
	FsyncDir(relDirSlash string) error
}

func acquireProcessLock(rootDir string) func() {
	key := filepath.Clean(rootDir)
	actual, _ := nsProcessLocks.LoadOrStore(key, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func withTrustedNamespace(rootDir string, fn func(ns platformNamespace) error) error {
	if strings.TrimSpace(rootDir) == "" {
		return NewError(500, "STORAGE_IO_ERROR", "empty model cache root directory")
	}
	unlockProc := acquireProcessLock(rootDir)
	defer unlockProc()

	if err := EnsureLayout(rootDir); err != nil {
		return err
	}
	ns, err := openLockedNamespacePlatform(rootDir)
	if err != nil {
		return err
	}
	defer ns.Close()
	return fn(ns)
}

func StagingDir(rootDir string) string {
	return filepath.Join(rootDir, ".staging")
}

func QuarantineDir(rootDir string) string {
	return filepath.Join(rootDir, ".quarantine")
}

func CASDir(rootDir string) string {
	return filepath.Join(rootDir, "cas")
}

func ViewsDir(rootDir string) string {
	return filepath.Join(rootDir, "views")
}

func PartBaseName(sha256Hex string, fence int64) string {
	return fmt.Sprintf("%s.%d.part", sha256Hex, fence)
}

func PartRelPath(sha256Hex string, fence int64) string {
	return ".staging/" + PartBaseName(sha256Hex, fence)
}

func PartFilePath(rootDir, sha256Hex string, fence int64) string {
	return filepath.Join(StagingDir(rootDir), PartBaseName(sha256Hex, fence))
}

func TempViewBaseName(sha256Hex string, fence int64) string {
	prefix := sha256Hex
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}
	return fmt.Sprintf(".tmp.%s.%d", prefix, fence)
}

func TempViewRelPath(sha256Hex string, fence int64) string {
	return "views/" + TempViewBaseName(sha256Hex, fence)
}

func TempViewLinkPath(rootDir, sha256Hex string, fence int64) string {
	return filepath.Join(ViewsDir(rootDir), TempViewBaseName(sha256Hex, fence))
}

func CASRelPath(sha256Hex string) string {
	return "cas/" + sha256Hex
}

func CASFilePath(rootDir, sha256Hex string) string {
	return filepath.Join(CASDir(rootDir), sha256Hex)
}

func validateRelViewSlash(relViewPath string) (string, error) {
	clean := filepath.ToSlash(strings.TrimSpace(relViewPath))
	if clean == "" || strings.HasPrefix(clean, "/") || strings.Contains(clean, "..") {
		return "", NewError(400, "INVALID_PARAMETERS", "invalid relative view path")
	}
	parts := strings.Split(clean, "/")
	if len(parts) != 2 {
		return "", NewError(400, "INVALID_PARAMETERS", "relative view path must be <category_dir>/<view_filename>")
	}
	validSubdir := false
	for _, dir := range categoryDirs {
		if parts[0] == dir {
			validSubdir = true
			break
		}
	}
	if !validSubdir {
		return "", NewError(400, "INVALID_PARAMETERS", "unknown model view category directory")
	}
	return parts[0] + "/" + parts[1], nil
}

func ViewFilePath(rootDir, relViewPath string) (string, error) {
	clean, err := validateRelViewSlash(relViewPath)
	if err != nil {
		return "", err
	}
	parts := strings.Split(clean, "/")
	return filepath.Join(ViewsDir(rootDir), parts[0], parts[1]), nil
}

func cleanRelUnderRoot(relSlash string) (string, error) {
	norm := strings.TrimSpace(filepath.ToSlash(relSlash))
	if norm == "" || norm == "." {
		return ".", nil
	}
	if strings.HasPrefix(norm, "/") || strings.Contains(norm, "\x00") {
		return "", NewError(400, "INVALID_PARAMETERS", "absolute or NUL relative path forbidden")
	}
	for _, seg := range strings.Split(norm, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", NewError(400, "INVALID_PARAMETERS", "path traversal segment forbidden")
		}
	}
	return path.Clean(norm), nil
}

func validateNoSymlinkDirChain(rootDir, relDirSlash string) error {
	if strings.TrimSpace(rootDir) == "" {
		return NewError(500, "STORAGE_IO_ERROR", "empty model cache root directory")
	}
	rootInfo, err := os.Lstat(rootDir)
	if err != nil {
		return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("cannot lstat model cache root: %v", err))
	}
	if isReparseOrSymlink(rootInfo) || !rootInfo.IsDir() {
		return NewError(500, "STORAGE_IO_ERROR", "model cache root directory must not be a symlink or non-directory")
	}
	cleanRel, err := cleanRelUnderRoot(relDirSlash)
	if err != nil {
		return err
	}
	if cleanRel == "." {
		return nil
	}
	cur := rootDir
	for _, seg := range strings.Split(cleanRel, "/") {
		cur = filepath.Join(cur, seg)
		st, lErr := os.Lstat(cur)
		if lErr != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("cannot lstat cache subdirectory: %v", lErr))
		}
		if isReparseOrSymlink(st) || !st.IsDir() {
			return NewError(500, "STORAGE_IO_ERROR", "symlink or non-directory detected inside model cache hierarchy")
		}
	}
	return nil
}

func ensureSafeDir(dirPath string, perm os.FileMode) error {
	if st, err := os.Lstat(dirPath); err == nil {
		if isReparseOrSymlink(st) || !st.IsDir() {
			return NewError(500, "STORAGE_IO_ERROR", "symlink or file colliding with model cache directory")
		}
		return os.Chmod(dirPath, perm)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(dirPath, perm); err != nil {
		return err
	}
	st, err := os.Lstat(dirPath)
	if err != nil || isReparseOrSymlink(st) || !st.IsDir() {
		return NewError(500, "STORAGE_IO_ERROR", "failed verifying newly created model cache directory")
	}
	return os.Chmod(dirPath, perm)
}

// EnsureLayout initializes /data/models/comfyui/{.staging,.quarantine,cas,views/<8 categories>}
// while rejecting any symlinked root or intermediate directory.
func EnsureLayout(rootDir string) error {
	if strings.TrimSpace(rootDir) == "" {
		return errors.New("empty model cache root directory")
	}
	if st, err := os.Lstat(rootDir); err == nil {
		if isReparseOrSymlink(st) || !st.IsDir() {
			return NewError(500, "STORAGE_IO_ERROR", "model cache root must be a real directory, not a symlink")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if mkErr := os.MkdirAll(rootDir, 0o755); mkErr != nil {
			return mkErr
		}
	} else {
		return err
	}
	if err := ensureSafeDir(StagingDir(rootDir), 0o700); err != nil {
		return err
	}
	if err := ensureSafeDir(QuarantineDir(rootDir), 0o700); err != nil {
		return err
	}
	if err := ensureSafeDir(CASDir(rootDir), 0o755); err != nil {
		return err
	}
	if err := ensureSafeDir(ViewsDir(rootDir), 0o755); err != nil {
		return err
	}
	for _, sub := range categoryDirs {
		if err := ensureSafeDir(filepath.Join(ViewsDir(rootDir), sub), 0o755); err != nil {
			return err
		}
	}
	return nil
}

func fsyncNamespaceDirChecked(ns platformNamespace, relDirSlash string) error {
	if testHookFsyncDir != nil {
		if err := testHookFsyncDir(relDirSlash); err != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("directory fsync failed: %v", err))
		}
	}
	if err := ns.FsyncDir(relDirSlash); err != nil {
		return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("directory fsync failed: %v", err))
	}
	return nil
}

func fsyncSortedDirs(ns platformNamespace, dirs []string) error {
	uniq := make([]string, 0, len(dirs))
	seen := map[string]bool{}
	for _, d := range dirs {
		clean, err := cleanRelUnderRoot(d)
		if err != nil {
			return err
		}
		if !seen[clean] {
			seen[clean] = true
			uniq = append(uniq, clean)
		}
	}
	sort.Strings(uniq)
	for _, d := range uniq {
		if err := fsyncNamespaceDirChecked(ns, d); err != nil {
			return err
		}
	}
	return nil
}

func setReadOnlyChecked(f *os.File, rootDir, relSlashPath string) error {
	if testHookSetReadOnly != nil {
		if err := testHookSetReadOnly(relSlashPath); err != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("chmod 0444 failed: %v", err))
		}
	}
	if err := setFileReadOnlyPlatform(f, rootDir, relSlashPath); err != nil {
		return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("chmod 0444 failed: %v", err))
	}
	return nil
}

// quarantineAndConfirmUnlink implements identity-bound quarantine-move + confirmation (R2-H02, R3-H02):
// 1. NEVER deletes when expectedID is nil/zero; preserves the entry intact and reports an error.
// 2. Inspects (relDirSlash, baseName) via pinned directory FD without following symlinks:
//    - On pre-move identity mismatch: preserves entry intact and returns swapped=true without moving/deleting.
// 3. Fires testHookAfterFinalStatBeforeSyscall directly between final stat and move syscall.
// 4. Atomically moves entry into private 0700 .quarantine/<qName> via RENAME_NOREPLACE (no data unlinked).
// 5. Confirms quarantined entry's (Dev, Ino) matches expectedID:
//    - On mismatch (concurrent swap between stat and syscall!): atomically restores entry back
//      to (relDirSlash, baseName) without unlinking and returns swapped=true!
//    - On match: unlinks .quarantine/<qName> safely inside private .quarantine.
func quarantineAndConfirmUnlink(
	ns platformNamespace,
	relDirSlash, baseName string,
	expectedID *FileIdentity,
	restoreRORelDir, restoreROBase string,
) (unlinked bool, swapped bool, err error) {
	relFull := relDirSlash + "/" + baseName
	if expectedID == nil || expectedID.Ino == 0 {
		if _, sErr := ns.StatInDir(relDirSlash, baseName); errors.Is(sErr, os.ErrNotExist) {
			return false, false, nil
		} else if sErr != nil {
			return false, false, sErr
		}
		return false, false, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("refusing quarantine unlink on %s without recorded file identity", relFull))
	}
	pinCloser, id, sErr := ns.PinInodeForQuarantine(relDirSlash, baseName)
	if errors.Is(sErr, os.ErrNotExist) {
		return false, false, nil
	}
	if sErr != nil {
		return false, false, sErr
	}
	closePin := func() {
		if pinCloser != nil {
			_ = pinCloser.Close()
			pinCloser = nil
		}
	}
	defer closePin()

	if (expectedID.Dev != 0 && id.Dev != expectedID.Dev) || id.Ino != expectedID.Ino {
		closePin()
		return false, true, nil
	}

	if testHookAfterFinalStatBeforeSyscall != nil {
		testHookAfterFinalStatBeforeSyscall("quarantine_unlink", relFull)
	}

	seq := atomic.AddUint64(&quarantineSeq, 1)
	qName := fmt.Sprintf(".q.%d.%s", seq, strings.ReplaceAll(uuid.NewString(), "-", ""))
	if rErr := ns.RenameNoReplaceInDirs(relDirSlash, baseName, ".quarantine", qName); rErr != nil {
		closePin()
		if errors.Is(rErr, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("quarantine move failed for %s: %v", relFull, rErr))
	}

	qStat, qErr := ns.StatInDir(".quarantine", qName)
	closePin()
	if qErr != nil || (expectedID.Dev != 0 && qStat.Dev != expectedID.Dev) || qStat.Ino != expectedID.Ino {
		// Concurrent entry replacement occurred between PinInodeForQuarantine and RenameNoReplaceInDirs!
		// Because pinCloser kept expectedID.Ino's kernel reference count > 0 on POSIX VFS across the move,
		// any newly created replacement file could NEVER reuse expectedID.Ino. Restore intact without deleting.
		if restErr := ns.RenameNoReplaceInDirs(".quarantine", qName, relDirSlash, baseName); restErr != nil {
			return false, true, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("quarantine restore after inode swap failed: %v", restErr))
		}
		return false, true, nil
	}

	if uErr := ns.UnlinkInQuarantine(qName, restoreRORelDir, restoreROBase); uErr != nil && !errors.Is(uErr, os.ErrNotExist) {
		return false, false, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("quarantine unlink failed for %s: %v", relFull, uErr))
	}
	return true, false, nil
}

func quarantineUnlinkWithIdentity(
	rootDir, relDirSlash, baseName string,
	expectedID *FileIdentity,
	restoreRORelDir, restoreROBase string,
) (unlinked bool, swapped bool, err error) {
	if expectedID == nil || expectedID.Ino == 0 {
		return false, false, NewError(500, "STORAGE_IO_ERROR", "refusing cleanup unlink without recorded file identity")
	}
	wErr := withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		u, sw, qErr := quarantineAndConfirmUnlink(ns, relDirSlash, baseName, expectedID, restoreRORelDir, restoreROBase)
		unlinked = u
		swapped = sw
		if qErr != nil {
			return qErr
		}
		if u {
			return fsyncSortedDirs(ns, []string{relDirSlash, ".quarantine"})
		}
		return nil
	})
	if wErr != nil {
		return unlinked, swapped, wErr
	}
	return unlinked, swapped, nil
}

// InspectResumeCandidate safely inspects an existing staging .part file in read-only mode
// without following symlinks or deleting entries, requiring regular file and nlink == 1.
func InspectResumeCandidate(rootDir, relPartPath string, expectedSize int64, resumeETag string) int64 {
	id, err := statBeneathNoFollow(rootDir, relPartPath)
	if err != nil {
		return 0
	}
	if id.Nlink != 1 || id.Size <= 0 || id.Size >= expectedSize || resumeETag == "" {
		return 0
	}
	return id.Size
}

// OpenStagingPartForWriteWithIdentity opens a staging .part file safely beneath rootDir while
// acquiring an OS-level writer lock (flock LOCK_EX|LOCK_NB on POSIX, no-write-share
// handle on Windows, R2-H01) and returns the opened file's exact FileIdentity (R3-H02):
//   - When resumeOffset == 0: clears any stale entry via identity-confirmed quarantine move
//     (never unverified path unlink) and creates exclusively (O_CREAT|O_EXCL|O_NOFOLLOW, 0600),
//     verifying nlink == 1.
//   - When resumeOffset > 0: opens existing regular file (O_RDWR|O_NOFOLLOW), verifying
//     nlink == 1, ownership, and exact size == resumeOffset.
func OpenStagingPartForWriteWithIdentity(rootDir, relPartPath string, resumeOffset int64) (*os.File, int64, FileIdentity, error) {
	cleanRel, err := cleanRelUnderRoot(relPartPath)
	if err != nil || cleanRel == "." {
		return nil, 0, FileIdentity{}, NewError(400, "INVALID_PARAMETERS", "invalid staging .part relative path")
	}
	parentRel := path.Dir(cleanRel)
	baseName := path.Base(cleanRel)

	if resumeOffset > 0 {
		f, openErr := openStagingPartForWritePlatform(rootDir, cleanRel, false)
		if openErr == nil {
			id, idErr := inspectOpenFileIdentity(f)
			if idErr == nil && id.Nlink == 1 && id.Size == resumeOffset {
				if _, seekErr := f.Seek(resumeOffset, io.SeekStart); seekErr == nil {
					return f, resumeOffset, id, nil
				}
			}
			_ = f.Close()
		}
		// Any mismatch or active writer lock on resume candidate falls back cleanly to exclusive fresh creation.
	}

	if clrErr := withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		stID, stErr := ns.StatInDir(parentRel, baseName)
		if errors.Is(stErr, os.ErrNotExist) {
			return nil
		}
		if stErr != nil {
			return stErr
		}
		unlinked, swapped, qErr := quarantineAndConfirmUnlink(ns, parentRel, baseName, &stID, "", "")
		if qErr != nil {
			return qErr
		}
		if swapped {
			return NewError(500, "STORAGE_IO_ERROR", "concurrent staging .part replacement detected while clearing stale entry")
		}
		if unlinked {
			return fsyncSortedDirs(ns, []string{parentRel, ".quarantine"})
		}
		return nil
	}); clrErr != nil {
		return nil, 0, FileIdentity{}, clrErr
	}

	f, err := openStagingPartForWritePlatform(rootDir, cleanRel, true)
	if err != nil {
		return nil, 0, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed creating exclusive staging .part: %v", err))
	}
	id, idErr := inspectOpenFileIdentity(f)
	if idErr != nil {
		_ = f.Close()
		return nil, 0, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "newly created staging .part failed identity inspection")
	}
	if id.Nlink != 1 || id.Size != 0 {
		_ = f.Close()
		_, _, _ = quarantineUnlinkWithIdentity(rootDir, parentRel, baseName, &id, "", "")
		return nil, 0, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "newly created staging .part failed nlink==1 or empty-file verification")
	}
	return f, 0, id, nil
}

// OpenStagingPartForWrite preserves the 3-return signature on top of OpenStagingPartForWriteWithIdentity.
func OpenStagingPartForWrite(rootDir, relPartPath string, resumeOffset int64) (*os.File, int64, error) {
	f, off, _, err := OpenStagingPartForWriteWithIdentity(rootDir, relPartPath, resumeOffset)
	return f, off, err
}

func prepareResumePartWithIdentity(
	rootDir, sha256Hex string,
	prevFence, newFence, expectedMax int64,
	allowInodeHandoff bool,
	copyQuotaApproved func(copyBytes int64) bool,
) (resumedBytes int64, usedIndependentInode bool, preparedID *FileIdentity) {
	if prevFence <= 0 || newFence <= prevFence || expectedMax <= 0 {
		return 0, false, nil
	}
	prevBase := PartBaseName(sha256Hex, prevFence)
	dstBase := PartBaseName(sha256Hex, newFence)
	prevRel := PartRelPath(sha256Hex, prevFence)

	_ = withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		if existingDst, err := ns.StatInDir(".staging", dstBase); err == nil {
			if existingDst.Nlink == 1 && existingDst.Size > 0 && existingDst.Size < expectedMax {
				cp := existingDst
				preparedID = &cp
			}
			return nil
		}
		id, err := ns.StatInDir(".staging", prevBase)
		if err != nil || id.Nlink != 1 || id.Size <= 0 || id.Size >= expectedMax {
			return nil
		}
		if allowInodeHandoff {
			var handedOff bool
			var probeState WriterProbeState
			var hErr error
			if testHookWriterProbeOverride != nil {
				if ov := testHookWriterProbeOverride("handoff_probe", prevRel); ov != nil {
					probeState = *ov
					if probeState == WriterProbeProvenSafe {
						handedOff, probeState, hErr = ns.TryExclusiveHandoffPart(prevBase, dstBase, id)
					}
				} else {
					handedOff, probeState, hErr = ns.TryExclusiveHandoffPart(prevBase, dstBase, id)
				}
			} else {
				handedOff, probeState, hErr = ns.TryExclusiveHandoffPart(prevBase, dstBase, id)
			}

			// ONLY WriterProbeProvenSafe permits sharing the previous inode (R3-H01).
			if hErr == nil && handedOff && probeState == WriterProbeProvenSafe {
				_ = fsyncNamespaceDirChecked(ns, ".staging")
				resumedBytes = id.Size
				usedIndependentInode = false
				cp := id
				preparedID = &cp
				return nil
			}
			if probeState != WriterProbeActiveWriter && probeState != WriterProbeUnprovable {
				return nil
			}
			// Either an active writer FD exists OR kernel lease probing could not prove zero
			// non-cooperative writable FDs (R3-H01): NEVER share the old inode; clone to a
			// brand-new independent inode only when copy quota is approved!
			if copyQuotaApproved == nil || !copyQuotaApproved(id.Size) {
				return nil
			}
		} else if copyQuotaApproved != nil && !copyQuotaApproved(id.Size) {
			return nil
		}

		if dstID, cErr := ns.CopyPartToIndependentInode(prevBase, dstBase, id.Size, id.Ino); cErr == nil {
			_ = fsyncNamespaceDirChecked(ns, ".staging")
			resumedBytes = id.Size
			usedIndependentInode = true
			cp := dstID
			preparedID = &cp
		}
		return nil
	})
	return resumedBytes, usedIndependentInode, preparedID
}

// PrepareResumePart prepares fence N's staging .part from fence N-1 (R2-H01, R3-H01):
//   - Inode handoff (zero-copy rename) is permitted ONLY when allowInodeHandoff == true
//     AND OS-level writer probe returns WriterProbeProvenSafe (proving zero processes hold
//     any cooperative or non-cooperative writable FD).
//   - When probe returns WriterProbeActiveWriter OR WriterProbeUnprovable, or when
//     allowInodeHandoff == false, valid prefix bytes are cloned into a brand-new independent
//     inode (when copyQuotaApproved allows) so late writes on old FDs can NEVER mutate CAS!
func PrepareResumePart(
	rootDir, sha256Hex string,
	prevFence, newFence, expectedMax int64,
	allowInodeHandoff bool,
	copyQuotaApproved func(copyBytes int64) bool,
) (resumedBytes int64, usedIndependentInode bool) {
	n, indep, _ := prepareResumePartWithIdentity(rootDir, sha256Hex, prevFence, newFence, expectedMax, allowInodeHandoff, copyQuotaApproved)
	return n, indep
}

// HandOffResumePart preserves compatibility on top of PrepareResumePart, performing
// zero-copy inode handoff ONLY after confirming WriterProbeProvenSafe.
func HandOffResumePart(rootDir, sha256Hex string, prevFence, newFence, expectedMax int64) (int64, bool) {
	n, _ := PrepareResumePart(rootDir, sha256Hex, prevFence, newFence, expectedMax, true, nil)
	return n, n > 0
}

// ComputeFileSHA256 computes the lowercase hex SHA-256 and exact byte count of a symlink-free file under rootDir.
func ComputeFileSHA256(pathStr string) (string, int64, error) {
	st, err := os.Lstat(pathStr)
	if err != nil {
		return "", 0, err
	}
	if isReparseOrSymlink(st) || !st.Mode().IsRegular() {
		return "", 0, errors.New("refusing to hash symlink or non-regular file")
	}
	f, err := os.Open(pathStr)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func checkVerifiedPartWriterProbe(f *os.File, rootDir, relSlashPath string) error {
	if testHookWriterProbeOverride != nil {
		if ov := testHookWriterProbeOverride("verify_probe", relSlashPath); ov != nil {
			switch *ov {
			case WriterProbeProvenSafe:
				return ensureNoOpenWriterOnVerifiedPartPlatform(f, rootDir, relSlashPath)
			case WriterProbeActiveWriter:
				return NewError(409, "OPERATION_IN_PROGRESS", "staging .part still has an active open writer FD")
			default:
				return NewError(500, "STORAGE_IO_ERROR", "cannot prove absence of open writer FDs on staging .part")
			}
		}
	}
	return ensureNoOpenWriterOnVerifiedPartPlatform(f, rootDir, relSlashPath)
}

// VerifiedStagingPart binds the open/verified file descriptor from OpenAndVerifyStagingPart
// directly into PublishCASAndViews so publish links from the verified FD itself (R2-H02).
type VerifiedStagingPart struct {
	RootDir      string
	RelPartPath  string
	SHA256       string
	Fence        int64
	ExpectedSize int64
	Identity     FileIdentity
	pinnedFile   *os.File
	probeHeld    bool
}

func (v *VerifiedStagingPart) Close() {
	if v != nil && v.pinnedFile != nil {
		if v.probeHeld {
			releaseVerifiedPartWriterProbePlatform(v.pinnedFile)
			v.probeHeld = false
		}
		_ = v.pinnedFile.Close()
		v.pinnedFile = nil
	}
}

// OpenAndVerifyStagingPartWithExpected opens .staging/<sha256>.<fence>.part without following symlinks,
// confirms identity matches expectedPartID (when non-nil), proves zero open writer FDs via kernel
// lease/share check (R3-H01), verifies nlink == 1, exact size, full SHA-256, and .safetensors frame,
// sets immutable 0444, fsyncs, and cleans up invalid files ONLY via identity-confirmed quarantine
// move (never unverified path unlink, R3-H02).
func OpenAndVerifyStagingPartWithExpected(
	rootDir, sha256Hex string,
	fence int64,
	expectedSize int64,
	expectedPartID *FileIdentity,
) (*VerifiedStagingPart, error) {
	relPart := PartRelPath(sha256Hex, fence)
	partBase := PartBaseName(sha256Hex, fence)
	f, err := openRegularReadOnlyPlatform(rootDir, relPart)
	if err != nil {
		return nil, NewError(500, "STORAGE_IO_ERROR", "staging .part file missing or unsafe before verification")
	}

	id, statErr := inspectOpenFileIdentity(f)
	if statErr != nil {
		_ = f.Close()
		return nil, NewError(500, "STORAGE_IO_ERROR", "staging .part failed open-handle identity inspection")
	}
	if expectedPartID != nil && expectedPartID.Ino != 0 {
		if (expectedPartID.Dev != 0 && id.Dev != expectedPartID.Dev) || id.Ino != expectedPartID.Ino {
			// Swapped before verification: NEVER delete the replacement entry (R3-H02)!
			_ = f.Close()
			return nil, NewError(500, "STORAGE_IO_ERROR", "staging .part identity changed before verification; preserved intact")
		}
	}

	if wErr := checkVerifiedPartWriterProbe(f, rootDir, relPart); wErr != nil {
		_ = f.Close()
		return nil, wErr
	}
	probeHeld := true
	closeWithoutUnlink := func() {
		if probeHeld {
			releaseVerifiedPartWriterProbePlatform(f)
			probeHeld = false
		}
		_ = f.Close()
	}
	cleanupIdentityBound := func() error {
		closeWithoutUnlink()
		_, swapped, qErr := quarantineUnlinkWithIdentity(rootDir, ".staging", partBase, &id, "", "")
		if qErr != nil {
			return qErr
		}
		if swapped {
			return NewError(500, "STORAGE_IO_ERROR", "concurrent replacement detected on staging .part during verification failure cleanup; preserved intact")
		}
		return nil
	}

	if id.Nlink != 1 {
		if cErr := cleanupIdentityBound(); cErr != nil {
			return nil, cErr
		}
		return nil, NewError(422, "MODEL_SHA256_MISMATCH", "staging .part file failed single-link check")
	}
	if id.Size != expectedSize {
		if cErr := cleanupIdentityBound(); cErr != nil {
			return nil, cErr
		}
		return nil, NewError(422, "MODEL_SHA256_MISMATCH", fmt.Sprintf("staging file size %d != expected %d", id.Size, expectedSize))
	}

	h := sha256.New()
	if n, copyErr := io.Copy(h, f); copyErr != nil || n != expectedSize {
		closeWithoutUnlink()
		return nil, NewError(500, "STORAGE_IO_ERROR", "failed reading staging .part file for SHA-256")
	}
	actualSHA := hex.EncodeToString(h.Sum(nil))
	if actualSHA != expectedSHA256Normalize(sha256Hex) {
		if cErr := cleanupIdentityBound(); cErr != nil {
			return nil, cErr
		}
		return nil, NewError(422, "MODEL_SHA256_MISMATCH", "downloaded bytes SHA-256 does not match authoritative catalog digest")
	}

	if frameErr := ValidateSafetensorsFile(f, expectedSize); frameErr != nil {
		if cErr := cleanupIdentityBound(); cErr != nil {
			return nil, cErr
		}
		return nil, frameErr
	}

	if roErr := setReadOnlyChecked(f, rootDir, relPart); roErr != nil {
		closeWithoutUnlink()
		return nil, roErr
	}
	if syncErr := syncVerifiedPartFile(f); syncErr != nil {
		closeWithoutUnlink()
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed syncing verified staging .part: %v", syncErr))
	}
	finalID, idErr2 := inspectOpenFileIdentity(f)
	if idErr2 != nil || finalID.Dev != id.Dev || finalID.Ino != id.Ino || finalID.Nlink != 1 || finalID.Size != expectedSize {
		_ = cleanupIdentityBound()
		return nil, NewError(500, "STORAGE_IO_ERROR", "staging .part identity changed during verification")
	}

	return &VerifiedStagingPart{
		RootDir:      rootDir,
		RelPartPath:  relPart,
		SHA256:       sha256Hex,
		Fence:        fence,
		ExpectedSize: expectedSize,
		Identity:     finalID,
		pinnedFile:   f,
		probeHeld:    probeHeld,
	}, nil
}

// OpenAndVerifyStagingPart preserves the 4-argument signature on top of OpenAndVerifyStagingPartWithExpected.
func OpenAndVerifyStagingPart(rootDir, sha256Hex string, fence int64, expectedSize int64) (*VerifiedStagingPart, error) {
	return OpenAndVerifyStagingPartWithExpected(rootDir, sha256Hex, fence, expectedSize, nil)
}

func expectedSHA256Normalize(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

// VerifyStagingPart verifies an arbitrary staging .part path using the exact same
// writer-probe and identity-bound quarantine cleanup protocol (never unverified path unlink).
func VerifyStagingPart(partPath, expectedSHA256 string, expectedSize int64) error {
	stagingDir := filepath.Dir(partPath)
	rootDir := filepath.Dir(stagingDir)
	relDir := filepath.ToSlash(filepath.Base(stagingDir))
	base := filepath.Base(partPath)
	rel := relDir + "/" + base

	f, err := openRegularReadOnlyPlatform(rootDir, rel)
	if err != nil {
		return NewError(500, "STORAGE_IO_ERROR", "staging .part file missing before verification")
	}
	id, statErr := inspectOpenFileIdentity(f)
	if statErr != nil {
		_ = f.Close()
		return NewError(500, "STORAGE_IO_ERROR", "staging .part failed open-handle identity inspection")
	}
	if wErr := checkVerifiedPartWriterProbe(f, rootDir, rel); wErr != nil {
		_ = f.Close()
		return wErr
	}
	probeHeld := true
	closeOnly := func() {
		if probeHeld {
			releaseVerifiedPartWriterProbePlatform(f)
			probeHeld = false
		}
		_ = f.Close()
	}
	cleanup := func() error {
		closeOnly()
		_, swapped, qErr := quarantineUnlinkWithIdentity(rootDir, relDir, base, &id, "", "")
		if qErr != nil {
			return qErr
		}
		if swapped {
			return NewError(500, "STORAGE_IO_ERROR", "concurrent replacement detected on staging .part during verification cleanup")
		}
		return nil
	}

	if id.Nlink != 1 {
		if cErr := cleanup(); cErr != nil {
			return cErr
		}
		return NewError(422, "MODEL_SHA256_MISMATCH", "invalid staging .part file state")
	}
	if id.Size != expectedSize {
		if cErr := cleanup(); cErr != nil {
			return cErr
		}
		return NewError(422, "MODEL_SHA256_MISMATCH", fmt.Sprintf("staging file size %d != expected %d", id.Size, expectedSize))
	}
	h := sha256.New()
	if _, copyErr := io.Copy(h, f); copyErr != nil {
		closeOnly()
		return NewError(500, "STORAGE_IO_ERROR", "failed reading staging .part file for SHA-256")
	}
	if hex.EncodeToString(h.Sum(nil)) != expectedSHA256Normalize(expectedSHA256) {
		if cErr := cleanup(); cErr != nil {
			return cErr
		}
		return NewError(422, "MODEL_SHA256_MISMATCH", "downloaded bytes SHA-256 does not match authoritative catalog digest")
	}
	if frameErr := ValidateSafetensorsFile(f, expectedSize); frameErr != nil {
		if cErr := cleanup(); cErr != nil {
			return cErr
		}
		return frameErr
	}
	if roErr := setReadOnlyChecked(f, rootDir, rel); roErr != nil {
		closeOnly()
		return roErr
	}
	if syncErr := syncVerifiedPartFile(f); syncErr != nil {
		closeOnly()
		return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed syncing staging .part: %v", syncErr))
	}
	closeOnly()
	return nil
}

// CleanupFenceArtifactsWithIdentity removes ONLY fence-owned staging and temporary-view
// entries whose on-disk (Dev, Ino) matches the recorded FenceArtifacts identity (R3-H02).
// If an entry exists on disk when its identity is unknown (nil) or mismatched (concurrent
// swap), the entry is preserved intact and tampered=true + error is returned.
func CleanupFenceArtifactsWithIdentity(rootDir, sha256Hex string, fence int64, known FenceArtifacts) (tampered bool, err error) {
	if rootDir == "" || !ValidSHA256(sha256Hex) || fence <= 0 {
		return false, nil
	}
	var anomErr error
	wErr := withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		var dirsToSync []string
		partBase := PartBaseName(sha256Hex, fence)
		if _, stErr := ns.StatInDir(".staging", partBase); stErr == nil {
			if known.PartID == nil || known.PartID.Ino == 0 {
				tampered = true
				anomErr = NewError(500, "STORAGE_IO_ERROR", "refusing fence cleanup on staging .part with unknown recorded identity; entry preserved intact")
			} else {
				unlinked, swapped, qErr := quarantineAndConfirmUnlink(ns, ".staging", partBase, known.PartID, "", "")
				if swapped {
					tampered = true
					anomErr = NewError(500, "STORAGE_IO_ERROR", "concurrent replacement detected on staging .part during fence cleanup; replacement preserved intact")
				} else if qErr != nil && anomErr == nil {
					anomErr = qErr
				} else if unlinked {
					dirsToSync = append(dirsToSync, ".staging", ".quarantine")
				}
			}
		} else if !errors.Is(stErr, os.ErrNotExist) && anomErr == nil {
			anomErr = stErr
		}

		tmpBase := TempViewBaseName(sha256Hex, fence)
		if _, tvErr := ns.StatInDir("views", tmpBase); tvErr == nil {
			if known.TempViewID == nil || known.TempViewID.Ino == 0 {
				tampered = true
				if anomErr == nil {
					anomErr = NewError(500, "STORAGE_IO_ERROR", "refusing fence cleanup on temporary view with unknown recorded identity; entry preserved intact")
				}
			} else {
				unlinked, swapped, qErr := quarantineAndConfirmUnlink(ns, "views", tmpBase, known.TempViewID, "cas", sha256Hex)
				if swapped {
					tampered = true
					if anomErr == nil {
						anomErr = NewError(500, "STORAGE_IO_ERROR", "concurrent replacement detected on temporary view during fence cleanup; replacement preserved intact")
					}
				} else if qErr != nil && anomErr == nil {
					anomErr = qErr
				} else if unlinked {
					dirsToSync = append(dirsToSync, "views", ".quarantine")
				}
			}
		} else if !errors.Is(tvErr, os.ErrNotExist) && anomErr == nil {
			anomErr = tvErr
		}

		if len(dirsToSync) > 0 {
			if fErr := fsyncSortedDirs(ns, dirsToSync); fErr != nil && anomErr == nil {
				anomErr = fErr
			}
		}
		return nil
	})
	if anomErr != nil {
		return tampered, anomErr
	}
	return tampered, wErr
}

// CleanupFenceArtifacts delegates to CleanupFenceArtifactsWithIdentity with empty
// recorded identities: because identity is unknown, any existing entry is preserved
// intact and never deleted blindly by path (R3-H02).
func CleanupFenceArtifacts(rootDir, sha256Hex string, fence int64) {
	_, _ = CleanupFenceArtifactsWithIdentity(rootDir, sha256Hex, fence, FenceArtifacts{})
}

func openAndVerifyCASInNamespace(
	ns platformNamespace,
	sha256Hex string,
	expectedSize int64,
	expectedFromVerified *FileIdentity,
	trustedReady bool,
) (*os.File, FileIdentity, error) {
	f, id, err := ns.OpenReadOnlyInDir("cas", sha256Hex)
	if err != nil {
		return nil, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "existing CAS object missing or unsafe")
	}
	if id.Size != expectedSize {
		_ = f.Close()
		return nil, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "existing CAS object failed size check")
	}
	if err := verifyReadOnlyOwnershipPlatform(id); err != nil {
		_ = f.Close()
		return nil, FileIdentity{}, err
	}
	if expectedFromVerified != nil {
		if id.Dev != expectedFromVerified.Dev || id.Ino != expectedFromVerified.Ino {
			_ = f.Close()
			return nil, FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "published CAS inode mismatch with verified staging object")
		}
		return f, id, nil
	}
	if !trustedReady {
		h := sha256.New()
		n, cErr := io.Copy(h, f)
		if cErr != nil || n != expectedSize || hex.EncodeToString(h.Sum(nil)) != expectedSHA256Normalize(sha256Hex) {
			_ = f.Close()
			return nil, FileIdentity{}, NewError(422, "MODEL_SHA256_MISMATCH", "existing unverified CAS object failed SHA-256 verification")
		}
		if frameErr := ValidateSafetensorsFile(f, expectedSize); frameErr != nil {
			_ = f.Close()
			return nil, FileIdentity{}, frameErr
		}
	}
	return f, id, nil
}

// PublishCASAndViews atomically publishes cas/<sha256> directly from the verified open file
// handle (when provided), creates all hardlink views from the open verified CAS handle
// under the exclusive namespace lock + pinned directory FDs (R2-H02, R3-H01, R3-H02), and
// fsyncs EVERY affected directory strictly AFTER all directory mutations complete — including
// on EEXIST retry recovery paths (R2-M02).
func PublishCASAndViews(
	rootDir, sha256Hex string,
	relViews []string,
	expectedSize int64,
	fence int64,
	verified *VerifiedStagingPart,
	alreadyVerifiedCAS bool,
) (uint64, []string, error) {
	if verified != nil {
		defer verified.Close()
	}
	cleanViews := make([]string, 0, len(relViews))
	for _, rv := range relViews {
		c, err := validateRelViewSlash(rv)
		if err != nil {
			return 0, nil, err
		}
		cleanViews = appendUniqueString(cleanViews, c)
	}
	if len(cleanViews) == 0 {
		return 0, nil, NewError(400, "INVALID_PARAMETERS", "at least one relative view path required for publish")
	}

	var publishedCASIno uint64
	err := withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		partBase := PartBaseName(sha256Hex, fence)
		partRel := PartRelPath(sha256Hex, fence)
		tmpBase := TempViewBaseName(sha256Hex, fence)

		// Always fsync "cas" at the persistence barrier, even on EEXIST retry after a prior fsync failure (R2-M02).
		dirsToSync := []string{"cas", "views"}

		partID, partErr := ns.StatInDir(".staging", partBase)
		var verifiedMatch *FileIdentity
		if partErr == nil {
			var srcFile *os.File
			var srcID FileIdentity
			var closeSrc bool

			if verified != nil && verified.pinnedFile != nil {
				if partID.Dev != verified.Identity.Dev || partID.Ino != verified.Identity.Ino || partID.Nlink != 1 || partID.Size != expectedSize {
					return NewError(500, "STORAGE_IO_ERROR", "staging .part inode or link count changed after verification")
				}
				srcFile = verified.pinnedFile
				srcID = verified.Identity
			} else {
				if partID.Nlink != 1 || partID.Size != expectedSize {
					return NewError(500, "STORAGE_IO_ERROR", "unverified staging .part failed link/size check")
				}
				openedF, openedID, oErr := ns.OpenReadOnlyInDir(".staging", partBase)
				if oErr != nil {
					return NewError(500, "STORAGE_IO_ERROR", "failed opening staging .part for read-only mode")
				}
				if wErr := checkVerifiedPartWriterProbe(openedF, rootDir, partRel); wErr != nil {
					_ = openedF.Close()
					return wErr
				}
				closeSrc = true
				if roErr := setReadOnlyChecked(openedF, rootDir, partRel); roErr != nil {
					releaseVerifiedPartWriterProbePlatform(openedF)
					_ = openedF.Close()
					return roErr
				}
				refreshedID, rErr := inspectOpenFileIdentity(openedF)
				if rErr != nil || refreshedID.Dev != openedID.Dev || refreshedID.Ino != openedID.Ino {
					releaseVerifiedPartWriterProbePlatform(openedF)
					_ = openedF.Close()
					return NewError(500, "STORAGE_IO_ERROR", "staging .part identity changed during read-only transition")
				}
				srcFile = openedF
				srcID = refreshedID
			}

			if testHookAfterFinalStatBeforeSyscall != nil {
				testHookAfterFinalStatBeforeSyscall("publish_cas", partRel)
			}

			linkErr := ns.LinkOpenFileToDir(srcFile, srcID, "cas", sha256Hex)
			if closeSrc {
				releaseVerifiedPartWriterProbePlatform(srcFile)
				_ = srcFile.Close()
			}
			if verified != nil {
				verified.Close()
			}

			if linkErr != nil {
				if !errors.Is(linkErr, os.ErrExist) {
					return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed linking verified object to CAS: %v", linkErr))
				}
				// CAS already exists: clean staging part safely via identity-confirmed quarantine.
				_, swapped, qErr := quarantineAndConfirmUnlink(ns, ".staging", partBase, &srcID, "cas", sha256Hex)
				if qErr != nil || swapped {
					return NewError(500, "STORAGE_IO_ERROR", "concurrent tampering detected on staging .part during EEXIST cleanup")
				}
				dirsToSync = append(dirsToSync, ".staging", ".quarantine")
			} else {
				idCopy := srcID
				verifiedMatch = &idCopy
				_, swapped, qErr := quarantineAndConfirmUnlink(ns, ".staging", partBase, &srcID, "cas", sha256Hex)
				if qErr != nil || swapped {
					// Staging entry was tampered concurrently: remove the newly linked CAS (matching srcID) and fail closed.
					_, _, _ = quarantineAndConfirmUnlink(ns, "cas", sha256Hex, &srcID, "", "")
					return NewError(500, "STORAGE_IO_ERROR", "concurrent tampering detected on staging .part during publish")
				}
				dirsToSync = append(dirsToSync, ".staging", ".quarantine")
			}
		} else if errors.Is(partErr, os.ErrNotExist) {
			if verified != nil {
				// If verified was provided, staging .part MUST NOT have vanished unless CAS already exists with valid content!
				verified.Close()
				if _, casStatErr := ns.StatInDir("cas", sha256Hex); errors.Is(casStatErr, os.ErrNotExist) {
					return NewError(500, "STORAGE_IO_ERROR", "verified staging .part disappeared before CAS publish")
				}
			}
		} else {
			return partErr
		}

		casFile, casID, casErr := openAndVerifyCASInNamespace(ns, sha256Hex, expectedSize, verifiedMatch, alreadyVerifiedCAS)
		if casErr != nil {
			return casErr
		}
		defer casFile.Close()

		// Create / verify every hardlink view from the open verified CAS handle BEFORE running directory fsyncs.
		for _, rv := range cleanViews {
			parts := strings.Split(rv, "/")
			catRelDir := "views/" + parts[0]
			viewBase := parts[1]
			dirsToSync = append(dirsToSync, catRelDir)

			if existingTmpID, tErr := ns.StatInDir("views", tmpBase); tErr == nil {
				if existingTmpID.Dev != casID.Dev || existingTmpID.Ino != casID.Ino {
					return NewError(500, "STORAGE_IO_ERROR", "unexpected foreign entry at temporary view path; preserved intact")
				}
				_, swapped, qErr := quarantineAndConfirmUnlink(ns, "views", tmpBase, &casID, "cas", sha256Hex)
				if qErr != nil || swapped {
					return NewError(500, "STORAGE_IO_ERROR", "concurrent tampering detected on temporary view path")
				}
				dirsToSync = append(dirsToSync, ".quarantine")
			} else if !errors.Is(tErr, os.ErrNotExist) {
				return tErr
			}

			if lErr := ns.LinkOpenFileToDir(casFile, casID, "views", tmpBase); lErr != nil {
				return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed linking verified CAS handle to temporary view: %v", lErr))
			}
			if renErr := ns.RenameNoReplaceInDirs("views", tmpBase, catRelDir, viewBase); renErr != nil {
				_, swapped, qErr := quarantineAndConfirmUnlink(ns, "views", tmpBase, &casID, "cas", sha256Hex)
				dirsToSync = append(dirsToSync, ".quarantine")
				if qErr != nil || swapped {
					return NewError(500, "STORAGE_IO_ERROR", "concurrent tampering detected while cleaning temporary view link")
				}
				if !errors.Is(renErr, os.ErrExist) {
					return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed publishing model view with RENAME_NOREPLACE: %v", renErr))
				}
				viewID, vErr := ns.StatInDir(catRelDir, viewBase)
				if vErr != nil || viewID.Dev != casID.Dev || viewID.Ino != casID.Ino {
					return NewError(409, "MODEL_VIEW_INODE_CONFLICT", "published view already exists with different inode or symlink")
				}
			}
		}

		// Unified durability barrier AFTER all CAS & view directory mutations complete (R2-M02).
		if err := fsyncSortedDirs(ns, dirsToSync); err != nil {
			return err
		}
		publishedCASIno = casID.Ino
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return publishedCASIno, cleanViews, nil
}

// PublishCASAndView preserves single-view signature on top of PublishCASAndViews.
func PublishCASAndView(rootDir, sha256Hex, category, filename string, expectedSize int64, fence int64, alreadyVerifiedCAS bool) (uint64, string, error) {
	relView, err := RelativeViewPath(category, sha256Hex, filename)
	if err != nil {
		return 0, "", err
	}
	casIno, views, err := PublishCASAndViews(rootDir, sha256Hex, []string{relView}, expectedSize, fence, nil, alreadyVerifiedCAS)
	if err != nil {
		return 0, "", err
	}
	return casIno, views[0], nil
}

// EvictCASAndViews deletes published hardlink views and cas/<sha256> using the
// identity-bound quarantine-move + inode-confirmation protocol under the exclusive
// namespace lock + pinned directory FDs (R2-H02, R3-H02), syncing all affected directories
// strictly AFTER all unlinks complete (R2-M02). If expectedCASInode is nil while files
// exist on disk, deletion is refused fail-closed.
func EvictCASAndViews(rootDir, sha256Hex string, relViews []string, expectedCASInode *uint64) error {
	return withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		if expectedCASInode == nil || *expectedCASInode == 0 {
			for _, rv := range relViews {
				cleanRV, err := validateRelViewSlash(rv)
				if err != nil {
					continue
				}
				parts := strings.Split(cleanRV, "/")
				if _, sErr := ns.StatInDir("views/"+parts[0], parts[1]); sErr == nil {
					return NewError(500, "STORAGE_IO_ERROR", "refusing model view eviction without recorded CAS inode identity")
				} else if !errors.Is(sErr, os.ErrNotExist) {
					return sErr
				}
			}
			if _, cErr := ns.StatInDir("cas", sha256Hex); cErr == nil {
				return NewError(500, "STORAGE_IO_ERROR", "refusing CAS eviction without recorded CAS inode identity")
			} else if !errors.Is(cErr, os.ErrNotExist) {
				return cErr
			}
			return nil
		}

		expectedID := &FileIdentity{
			Dev: ns.RootDev(),
			Ino: *expectedCASInode,
		}
		dirsToSync := []string{"cas", ".quarantine"}
		for _, rv := range relViews {
			cleanRV, err := validateRelViewSlash(rv)
			if err != nil {
				continue
			}
			parts := strings.Split(cleanRV, "/")
			catRelDir := "views/" + parts[0]
			viewBase := parts[1]
			dirsToSync = append(dirsToSync, catRelDir)

			_, _, qErr := quarantineAndConfirmUnlink(ns, catRelDir, viewBase, expectedID, "cas", sha256Hex)
			if qErr != nil {
				return qErr
			}
		}

		if _, _, cErr := quarantineAndConfirmUnlink(ns, "cas", sha256Hex, expectedID, "", ""); cErr != nil {
			return cErr
		}

		return fsyncSortedDirs(ns, dirsToSync)
	})
}

func isPositiveDecimalFenceSegment(seg string) bool {
	if len(seg) == 0 || seg[0] < '1' || seg[0] > '9' {
		return false
	}
	for i := 1; i < len(seg); i++ {
		if seg[i] < '0' || seg[i] > '9' {
			return false
		}
	}
	return true
}

func isStagingPartNameForSHA(name, sha256Hex string) bool {
	prefix := sha256Hex + "."
	const suffix = ".part"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	if len(name) <= len(prefix)+len(suffix) {
		return false
	}
	mid := name[len(prefix) : len(name)-len(suffix)]
	return isPositiveDecimalFenceSegment(mid)
}

func isTempViewNameForSHA(name, sha256Hex string) bool {
	shortSHA := sha256Hex
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}
	prefix := ".tmp." + shortSHA + "."
	if !strings.HasPrefix(name, prefix) || len(name) <= len(prefix) {
		return false
	}
	return isPositiveDecimalFenceSegment(name[len(prefix):])
}

// EvictCASViewsAndStaging deletes published hardlink views and cas/<sha256> via EvictCASAndViews,
// and enumerates all residual fence-owned .staging/<sha256>.<fence>.part / temporary-view entries
// across every historical fence under the exclusive namespace lock + identity-confirmed quarantine
// unlink protocol, verifying zero residual staging artifacts remain before returning.
func EvictCASViewsAndStaging(rootDir, sha256Hex string, relViews []string, expectedCASInode *uint64, _ int64) error {
	if err := EvictCASAndViews(rootDir, sha256Hex, relViews, expectedCASInode); err != nil {
		return err
	}
	return withTrustedNamespace(rootDir, func(ns platformNamespace) error {
		var dirsToSync []string

		stagingNames, lErr := ns.ListDirNames(".staging")
		if lErr != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed enumerating .staging during eviction: %v", lErr))
		}
		for _, partBase := range stagingNames {
			if !isStagingPartNameForSHA(partBase, sha256Hex) {
				continue
			}
			stID, stErr := ns.StatInDir(".staging", partBase)
			if errors.Is(stErr, os.ErrNotExist) {
				continue
			}
			if stErr != nil {
				return stErr
			}
			unlinked, swapped, qErr := quarantineAndConfirmUnlink(ns, ".staging", partBase, &stID, "", "")
			if qErr != nil {
				return qErr
			}
			if swapped {
				return NewError(500, "STORAGE_IO_ERROR", "concurrent staging .part replacement detected during eviction")
			}
			if unlinked {
				dirsToSync = append(dirsToSync, ".staging", ".quarantine")
			}
		}

		viewNames, vErr := ns.ListDirNames("views")
		if vErr != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed enumerating views during eviction: %v", vErr))
		}
		for _, tmpBase := range viewNames {
			if !isTempViewNameForSHA(tmpBase, sha256Hex) {
				continue
			}
			tvID, tvErr := ns.StatInDir("views", tmpBase)
			if errors.Is(tvErr, os.ErrNotExist) {
				continue
			}
			if tvErr != nil {
				return tvErr
			}
			ownsTempView := tvID.Nlink == 1 || (expectedCASInode != nil && *expectedCASInode != 0 && tvID.Dev == ns.RootDev() && tvID.Ino == *expectedCASInode)
			if !ownsTempView {
				continue
			}
			unlinked, swapped, qErr := quarantineAndConfirmUnlink(ns, "views", tmpBase, &tvID, "cas", sha256Hex)
			if qErr != nil {
				return qErr
			}
			if swapped {
				return NewError(500, "STORAGE_IO_ERROR", "concurrent temporary view replacement detected during eviction")
			}
			if unlinked {
				dirsToSync = append(dirsToSync, "views", ".quarantine")
			}
		}

		// Post-check under exclusive .ns.lock: verify zero residual staging .part files remain for sha256Hex.
		afterStaging, aErr := ns.ListDirNames(".staging")
		if aErr != nil {
			return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed post-verifying .staging after eviction: %v", aErr))
		}
		for _, name := range afterStaging {
			if isStagingPartNameForSHA(name, sha256Hex) {
				return NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("residual staging artifact %s remained after eviction", name))
			}
		}

		if len(dirsToSync) > 0 {
			return fsyncSortedDirs(ns, dirsToSync)
		}
		return nil
	})
}

func StatDiskSpace(rootDir string) (int64, int64, error) {
	return platformDiskSpace(rootDir)
}

func inspectFileInode(pathStr string) (uint64, error) {
	st, err := os.Lstat(pathStr)
	if err != nil {
		return 0, err
	}
	if isReparseOrSymlink(st) || !st.Mode().IsRegular() {
		return 0, errors.New("refusing to inspect inode of symlink or non-regular file")
	}
	f, err := os.Open(pathStr)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	id, err := inspectOpenFileIdentity(f)
	if err != nil {
		return 0, err
	}
	return id.Ino, nil
}
