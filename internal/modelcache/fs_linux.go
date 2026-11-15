//go:build linux

package modelcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func isReparseOrSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func syncVerifiedPartFile(f *os.File) error {
	return f.Sync()
}

func openRootDirNoFollow(rootDir string) (int, error) {
	fd, err := unix.Open(rootDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, NewError(500, "STORAGE_IO_ERROR", "failed opening model cache root directory FD without symlinks")
	}
	return fd, nil
}

func openat2OrWalk(rootFD int, relSlash string, flags uint64, mode uint64) (int, error) {
	cleanRel, err := cleanRelUnderRoot(relSlash)
	if err != nil {
		return -1, err
	}
	if cleanRel == "." {
		dupFD, dErr := unix.Dup(rootFD)
		if dErr != nil {
			return -1, dErr
		}
		unix.CloseOnExec(dupFD)
		return dupFD, nil
	}
	how := &unix.OpenHow{
		Flags:   flags | unix.O_CLOEXEC,
		Mode:    mode,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	}
	fd, err := unix.Openat2(rootFD, cleanRel, how)
	if err == nil {
		return fd, nil
	}
	if !errors.Is(err, unix.ENOSYS) {
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.EXDEV) {
			return -1, NewError(500, "STORAGE_IO_ERROR", "symlink or cross-device resolution blocked by openat2")
		}
		if errors.Is(err, unix.ENOENT) {
			return -1, os.ErrNotExist
		}
		if errors.Is(err, unix.EEXIST) {
			return -1, os.ErrExist
		}
		return -1, err
	}

	// Fallback ONLY when kernel lacks openat2 syscall: component-wise O_NOFOLLOW + same-dev check.
	var rootStat unix.Stat_t
	if sErr := unix.Fstat(rootFD, &rootStat); sErr != nil {
		return -1, sErr
	}
	segs := strings.Split(cleanRel, "/")
	curFD, err := unix.Dup(rootFD)
	if err != nil {
		return -1, err
	}
	unix.CloseOnExec(curFD)
	for i := 0; i < len(segs)-1; i++ {
		nextFD, oErr := unix.Openat(curFD, segs[i], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(curFD)
		if oErr != nil {
			if errors.Is(oErr, unix.ENOENT) {
				return -1, os.ErrNotExist
			}
			return -1, oErr
		}
		var st unix.Stat_t
		if fErr := unix.Fstat(nextFD, &st); fErr != nil || st.Dev != rootStat.Dev {
			_ = unix.Close(nextFD)
			return -1, NewError(500, "STORAGE_IO_ERROR", "cross-device directory traversal blocked")
		}
		curFD = nextFD
	}
	leafFD, lErr := unix.Openat(curFD, segs[len(segs)-1], int(flags)|unix.O_NOFOLLOW|unix.O_CLOEXEC, uint32(mode))
	_ = unix.Close(curFD)
	if lErr != nil {
		if errors.Is(lErr, unix.ENOENT) {
			return -1, os.ErrNotExist
		}
		if errors.Is(lErr, unix.EEXIST) {
			return -1, os.ErrExist
		}
		return -1, lErr
	}
	var leafStat unix.Stat_t
	if fErr := unix.Fstat(leafFD, &leafStat); fErr != nil || leafStat.Dev != rootStat.Dev {
		_ = unix.Close(leafFD)
		return -1, NewError(500, "STORAGE_IO_ERROR", "cross-device file traversal blocked")
	}
	return leafFD, nil
}

type linuxNamespace struct {
	rootDir string
	rootFD  int
	lockFD  int
	rootDev uint64
	dirFDs  map[string]int
}

func openLockedNamespacePlatform(rootDir string) (platformNamespace, error) {
	rfd, err := openRootDirNoFollow(rootDir)
	if err != nil {
		return nil, err
	}
	lockFD, lErr := openat2OrWalk(rfd, ".ns.lock", uint64(unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW), 0o600)
	if lErr != nil {
		_ = unix.Close(rfd)
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed opening .ns.lock: %v", lErr))
	}
	var lockSt unix.Stat_t
	if fErr := unix.Fstat(lockFD, &lockSt); fErr != nil || (lockSt.Mode&syscall.S_IFMT) != syscall.S_IFREG {
		_ = unix.Close(lockFD)
		_ = unix.Close(rfd)
		return nil, NewError(500, "STORAGE_IO_ERROR", ".ns.lock is not a regular file")
	}
	if flErr := unix.Flock(lockFD, unix.LOCK_EX); flErr != nil {
		_ = unix.Close(lockFD)
		_ = unix.Close(rfd)
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed acquiring exclusive .ns.lock: %v", flErr))
	}

	ns := &linuxNamespace{
		rootDir: rootDir,
		rootFD:  rfd,
		lockFD:  lockFD,
		rootDev: uint64(lockSt.Dev),
		dirFDs:  map[string]int{},
	}
	for _, dir := range []string{".staging", ".quarantine", "cas", "views"} {
		if _, dErr := ns.pinnedDirFD(dir); dErr != nil {
			ns.Close()
			return nil, dErr
		}
	}
	return ns, nil
}

func (n *linuxNamespace) RootDev() uint64 {
	return n.rootDev
}

func (n *linuxNamespace) Close() {
	for k, fd := range n.dirFDs {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		delete(n.dirFDs, k)
	}
	if n.lockFD >= 0 {
		_ = unix.Flock(n.lockFD, unix.LOCK_UN)
		_ = unix.Close(n.lockFD)
		n.lockFD = -1
	}
	if n.rootFD >= 0 {
		_ = unix.Close(n.rootFD)
		n.rootFD = -1
	}
}

func (n *linuxNamespace) pinnedDirFD(relDirSlash string) (int, error) {
	clean, err := cleanRelUnderRoot(relDirSlash)
	if err != nil {
		return -1, err
	}
	if clean == "." {
		return n.rootFD, nil
	}
	if fd, ok := n.dirFDs[clean]; ok {
		return fd, nil
	}
	fd, oErr := openat2OrWalk(n.rootFD, clean, uint64(unix.O_RDONLY|unix.O_DIRECTORY), 0)
	if oErr != nil {
		return -1, oErr
	}
	n.dirFDs[clean] = fd
	return fd, nil
}

func statToIdentityLinux(st unix.Stat_t) (FileIdentity, error) {
	if (st.Mode & syscall.S_IFMT) != syscall.S_IFREG {
		return FileIdentity{}, NewError(400, "UNSAFE_MODEL_FORMAT", "symlink or non-regular file rejected")
	}
	euid := uint32(os.Geteuid())
	if euid != 0 && st.Uid != euid {
		return FileIdentity{}, NewError(500, "STORAGE_IO_ERROR", "file UID mismatch in model cache")
	}
	return FileIdentity{
		Dev:   uint64(st.Dev),
		Ino:   uint64(st.Ino),
		Nlink: uint64(st.Nlink),
		Perm:  os.FileMode(st.Mode & 0o777),
		Uid:   st.Uid,
		Size:  st.Size,
	}, nil
}

func (n *linuxNamespace) StatInDir(relDirSlash, baseName string) (FileIdentity, error) {
	dfd, err := n.pinnedDirFD(relDirSlash)
	if err != nil {
		return FileIdentity{}, err
	}
	var st unix.Stat_t
	if fErr := unix.Fstatat(dfd, baseName, &st, unix.AT_SYMLINK_NOFOLLOW); fErr != nil {
		if errors.Is(fErr, unix.ENOENT) {
			return FileIdentity{}, os.ErrNotExist
		}
		return FileIdentity{}, fErr
	}
	return statToIdentityLinux(st)
}

// PinInodeForQuarantine holds an O_RDONLY|O_NOFOLLOW FD open on (relDirSlash, baseName)
// across quarantine move + identity confirmation so Linux VFS cannot recycle st_ino if
// the directory entry is concurrently unlinked and recreated between check and move.
func (n *linuxNamespace) PinInodeForQuarantine(relDirSlash, baseName string) (io.Closer, FileIdentity, error) {
	return n.OpenReadOnlyInDir(relDirSlash, baseName)
}

func (n *linuxNamespace) ListDirNames(relDirSlash string) ([]string, error) {
	dfd, err := n.pinnedDirFD(relDirSlash)
	if err != nil {
		return nil, err
	}
	iterFD, oErr := unix.Openat(dfd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if oErr != nil {
		return nil, oErr
	}
	dirFile := os.NewFile(uintptr(iterFD), relDirSlash)
	defer dirFile.Close()
	names, rErr := dirFile.Readdirnames(-1)
	if rErr != nil {
		return nil, rErr
	}
	sort.Strings(names)
	return names, nil
}

func (n *linuxNamespace) OpenReadOnlyInDir(relDirSlash, baseName string) (*os.File, FileIdentity, error) {
	dfd, err := n.pinnedDirFD(relDirSlash)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	fd, oErr := openat2OrWalk(dfd, baseName, uint64(unix.O_RDONLY|unix.O_NOFOLLOW), 0)
	if oErr != nil {
		return nil, FileIdentity{}, oErr
	}
	var st unix.Stat_t
	if sErr := unix.Fstat(fd, &st); sErr != nil {
		_ = unix.Close(fd)
		return nil, FileIdentity{}, sErr
	}
	id, idErr := statToIdentityLinux(st)
	if idErr != nil {
		_ = unix.Close(fd)
		return nil, FileIdentity{}, idErr
	}
	full := filepath.Join(n.rootDir, filepath.FromSlash(relDirSlash), baseName)
	return os.NewFile(uintptr(fd), full), id, nil
}

func (n *linuxNamespace) LinkOpenFileToDir(srcFile *os.File, expectedID FileIdentity, dstRelDirSlash, dstBaseName string) error {
	dstDFD, err := n.pinnedDirFD(dstRelDirSlash)
	if err != nil {
		return err
	}
	curID, idErr := inspectOpenFileIdentity(srcFile)
	if idErr != nil || curID.Dev != expectedID.Dev || curID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "verified source FD identity mismatch before linkat")
	}

	// Link directly from the kernel struct file via /proc/self/fd/<fd> so source path TOCTOU is impossible!
	procPath := fmt.Sprintf("/proc/self/fd/%d", int(srcFile.Fd()))
	lErr := unix.Linkat(unix.AT_FDCWD, procPath, dstDFD, dstBaseName, unix.AT_SYMLINK_FOLLOW)
	if lErr != nil {
		if errors.Is(lErr, unix.EEXIST) {
			return os.ErrExist
		}
		// Fallback only if /proc is unmounted in container: try AT_EMPTY_PATH on srcFile FD.
		lErr2 := unix.Linkat(int(srcFile.Fd()), "", dstDFD, dstBaseName, unix.AT_EMPTY_PATH)
		if errors.Is(lErr2, unix.EEXIST) {
			return os.ErrExist
		}
		if lErr2 != nil {
			return lErr
		}
	}

	linkedID, sErr := n.StatInDir(dstRelDirSlash, dstBaseName)
	if sErr != nil || linkedID.Dev != expectedID.Dev || linkedID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "linked destination inode did not match verified source FD; preserved intact")
	}
	return nil
}

func (n *linuxNamespace) RenameNoReplaceInDirs(srcRelDirSlash, srcBaseName, dstRelDirSlash, dstBaseName string) error {
	srcDFD, err := n.pinnedDirFD(srcRelDirSlash)
	if err != nil {
		return err
	}
	dstDFD, err := n.pinnedDirFD(dstRelDirSlash)
	if err != nil {
		return err
	}
	rErr := unix.Renameat2(srcDFD, srcBaseName, dstDFD, dstBaseName, unix.RENAME_NOREPLACE)
	if rErr == nil {
		return nil
	}
	if errors.Is(rErr, unix.EEXIST) {
		return os.ErrExist
	}
	if errors.Is(rErr, unix.ENOENT) {
		return os.ErrNotExist
	}
	return rErr
}

func (n *linuxNamespace) UnlinkInQuarantine(qName string, _, _ string) error {
	qDFD, err := n.pinnedDirFD(".quarantine")
	if err != nil {
		return err
	}
	uErr := unix.Unlinkat(qDFD, qName, 0)
	if errors.Is(uErr, unix.ENOENT) {
		return os.ErrNotExist
	}
	return uErr
}

// probeNoOpenWriterLinux classifies open-writer absence into three strict states (R3-H01):
//   - WriterProbeProvenSafe: BOTH flock(LOCK_EX|LOCK_NB) AND fcntl(F_SETLEASE, F_RDLCK) succeeded,
//     kernel-proving atomic_read(&inode->i_writecount) == 0 across ALL processes (including
//     non-cooperative O_RDWR FDs that never called flock).
//   - WriterProbeActiveWriter: flock contended OR F_SETLEASE returned EAGAIN/EACCES/EBUSY.
//   - WriterProbeUnprovable: F_SETLEASE failed with EPERM/EINVAL/ENOSYS/etc. Advisory flock
//     alone NEVER substitutes for kernel lease verification!
func probeNoOpenWriterLinux(fd int) WriterProbeState {
	flErr := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if flErr != nil {
		return classifyLinuxWriterProbe(flErr, nil)
	}
	_, lErr := unix.FcntlInt(uintptr(fd), unix.F_SETLEASE, unix.F_RDLCK)
	state := classifyLinuxWriterProbe(nil, lErr)
	if state != WriterProbeProvenSafe {
		_ = unix.Flock(fd, unix.LOCK_UN)
	}
	return state
}

func releaseWriterProbeLinux(fd int) {
	_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETLEASE, unix.F_UNLCK)
	_ = unix.Flock(fd, unix.LOCK_UN)
}

func (n *linuxNamespace) TryExclusiveHandoffPart(prevBase, dstBase string, expectedID FileIdentity) (bool, WriterProbeState, error) {
	stDFD, err := n.pinnedDirFD(".staging")
	if err != nil {
		return false, WriterProbeUnprovable, err
	}
	fd, oErr := openat2OrWalk(stDFD, prevBase, uint64(unix.O_RDONLY|unix.O_NOFOLLOW), 0)
	if oErr != nil {
		return false, WriterProbeUnprovable, oErr
	}
	defer unix.Close(fd)

	var st unix.Stat_t
	if fErr := unix.Fstat(fd, &st); fErr != nil {
		return false, WriterProbeUnprovable, fErr
	}
	id, idErr := statToIdentityLinux(st)
	if idErr != nil || id.Dev != expectedID.Dev || id.Ino != expectedID.Ino || id.Nlink != 1 || id.Size != expectedID.Size {
		return false, WriterProbeUnprovable, errors.New("staging resume candidate identity changed")
	}

	probeState := probeNoOpenWriterLinux(fd)
	if probeState != WriterProbeProvenSafe {
		return false, probeState, nil
	}
	defer releaseWriterProbeLinux(fd)

	if rErr := unix.Renameat2(stDFD, prevBase, stDFD, dstBase, unix.RENAME_NOREPLACE); rErr != nil {
		return false, WriterProbeProvenSafe, rErr
	}
	return true, WriterProbeProvenSafe, nil
}

func (n *linuxNamespace) CopyPartToIndependentInode(prevBase, dstBase string, copyBytes int64, prevIno uint64) (FileIdentity, error) {
	stDFD, err := n.pinnedDirFD(".staging")
	if err != nil {
		return FileIdentity{}, err
	}
	srcFD, sErr := openat2OrWalk(stDFD, prevBase, uint64(unix.O_RDONLY|unix.O_NOFOLLOW), 0)
	if sErr != nil {
		return FileIdentity{}, sErr
	}
	srcFile := os.NewFile(uintptr(srcFD), prevBase)
	defer srcFile.Close()

	dstFD, dErr := openat2OrWalk(stDFD, dstBase, uint64(unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW), 0o600)
	if dErr != nil {
		return FileIdentity{}, dErr
	}
	dstFile := os.NewFile(uintptr(dstFD), dstBase)
	dstID, idErr := inspectOpenFileIdentity(dstFile)
	if idErr != nil {
		_ = dstFile.Close()
		return FileIdentity{}, errors.New("independent staging .part failed identity inspection")
	}
	cleanupDst := func() {
		_ = dstFile.Close()
		_, _, _ = quarantineAndConfirmUnlink(n, ".staging", dstBase, &dstID, "", "")
	}

	if dstID.Nlink != 1 || dstID.Ino == prevIno {
		cleanupDst()
		return FileIdentity{}, errors.New("independent staging .part failed distinct-inode verification")
	}
	copied, cErr := io.CopyN(dstFile, srcFile, copyBytes)
	if cErr != nil || copied != copyBytes {
		cleanupDst()
		return FileIdentity{}, errors.New("failed copying prefix bytes to independent staging .part")
	}
	if syncErr := dstFile.Sync(); syncErr != nil {
		cleanupDst()
		return FileIdentity{}, syncErr
	}
	finalID, fErr := inspectOpenFileIdentity(dstFile)
	if fErr != nil || finalID.Dev != dstID.Dev || finalID.Ino != dstID.Ino {
		cleanupDst()
		return FileIdentity{}, errors.New("independent staging .part identity changed before close")
	}
	if err := dstFile.Close(); err != nil {
		return FileIdentity{}, err
	}
	return finalID, nil
}

func (n *linuxNamespace) FsyncDir(relDirSlash string) error {
	dfd, err := n.pinnedDirFD(relDirSlash)
	if err != nil {
		return err
	}
	return unix.Fsync(dfd)
}

func openParentDirBeneath(rootDir, relSlashPath string) (rootFD int, dirFD int, baseName string, err error) {
	cleanRel, cErr := cleanRelUnderRoot(relSlashPath)
	if cErr != nil || cleanRel == "." {
		return -1, -1, "", NewError(400, "INVALID_PARAMETERS", "invalid relative file path")
	}
	rfd, rErr := openRootDirNoFollow(rootDir)
	if rErr != nil {
		return -1, -1, "", rErr
	}
	parentRel := path.Dir(cleanRel)
	dfd, dErr := openat2OrWalk(rfd, parentRel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if dErr != nil {
		_ = unix.Close(rfd)
		return -1, -1, "", dErr
	}
	return rfd, dfd, path.Base(cleanRel), nil
}

func openStagingPartForWritePlatform(rootDir, relSlashPath string, exclusive bool) (*os.File, error) {
	rfd, err := openRootDirNoFollow(rootDir)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rfd)

	flags := uint64(unix.O_RDWR | unix.O_NOFOLLOW)
	var mode uint64
	if exclusive {
		flags |= unix.O_CREAT | unix.O_EXCL
		mode = 0o600
	}
	fd, oErr := openat2OrWalk(rfd, relSlashPath, flags, mode)
	if oErr != nil {
		return nil, oErr
	}
	// Acquire non-blocking exclusive writer lock held for the lifetime of the writer FD (R2-H01).
	if flErr := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); flErr != nil {
		_ = unix.Close(fd)
		return nil, NewError(409, "OPERATION_IN_PROGRESS", "staging .part is locked by an active writer")
	}
	return os.NewFile(uintptr(fd), filepath.Join(rootDir, filepath.FromSlash(relSlashPath))), nil
}

func openRegularReadOnlyPlatform(rootDir, relSlashPath string) (*os.File, error) {
	rfd, err := openRootDirNoFollow(rootDir)
	if err != nil {
		return nil, err
	}
	defer unix.Close(rfd)

	fd, oErr := openat2OrWalk(rfd, relSlashPath, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
	if oErr != nil {
		return nil, oErr
	}
	var st unix.Stat_t
	if sErr := unix.Fstat(fd, &st); sErr != nil || (st.Mode&syscall.S_IFMT) != syscall.S_IFREG {
		_ = unix.Close(fd)
		return nil, NewError(400, "UNSAFE_MODEL_FORMAT", "non-regular file rejected")
	}
	return os.NewFile(uintptr(fd), filepath.Join(rootDir, filepath.FromSlash(relSlashPath))), nil
}

func ensureNoOpenWriterOnVerifiedPartPlatform(f *os.File, _, _ string) error {
	fd := int(f.Fd())
	switch probeNoOpenWriterLinux(fd) {
	case WriterProbeProvenSafe:
		// Keep F_RDLCK + LOCK_EX held on pinnedFile until releaseVerifiedPartWriterProbePlatform!
		return nil
	case WriterProbeActiveWriter:
		return NewError(409, "OPERATION_IN_PROGRESS", "staging .part still has an active open writer FD")
	default:
		return NewError(500, "STORAGE_IO_ERROR", "cannot prove absence of open writer FDs on staging .part (F_SETLEASE failed)")
	}
}

func releaseVerifiedPartWriterProbePlatform(f *os.File) {
	if f != nil {
		releaseWriterProbeLinux(int(f.Fd()))
	}
}

func inspectOpenFileIdentity(f *os.File) (FileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return FileIdentity{}, err
	}
	return statToIdentityLinux(st)
}

func statBeneathNoFollow(rootDir, relSlashPath string) (FileIdentity, error) {
	rfd, dfd, base, err := openParentDirBeneath(rootDir, relSlashPath)
	if err != nil {
		return FileIdentity{}, err
	}
	defer unix.Close(rfd)
	defer unix.Close(dfd)

	var st unix.Stat_t
	if fErr := unix.Fstatat(dfd, base, &st, unix.AT_SYMLINK_NOFOLLOW); fErr != nil {
		if errors.Is(fErr, unix.ENOENT) {
			return FileIdentity{}, os.ErrNotExist
		}
		return FileIdentity{}, fErr
	}
	return statToIdentityLinux(st)
}

func setFileReadOnlyPlatform(f *os.File, _, _ string) error {
	return unix.Fchmod(int(f.Fd()), 0o444)
}

func verifyReadOnlyOwnershipPlatform(id FileIdentity) error {
	if id.Perm != 0o444 {
		return NewError(500, "STORAGE_IO_ERROR", "existing CAS file does not have immutable 0444 permissions")
	}
	euid := uint32(os.Geteuid())
	if euid != 0 && id.Uid != euid {
		return NewError(500, "STORAGE_IO_ERROR", "existing CAS file UID mismatch")
	}
	return nil
}

func platformDiskSpace(rootDir string) (int64, int64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(rootDir, &s); err != nil {
		return 0, 0, err
	}
	total := int64(s.Blocks) * int64(s.Bsize)
	free := int64(s.Bavail) * int64(s.Bsize)
	if total < 0 {
		total = 0
	}
	if free < 0 {
		free = 0
	}
	return total, free, nil
}
