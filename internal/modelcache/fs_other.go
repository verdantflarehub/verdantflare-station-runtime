//go:build !linux && !windows

package modelcache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	"golang.org/x/sys/unix"
)

func isReparseOrSymlink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func syncVerifiedPartFile(f *os.File) error {
	return f.Sync()
}

func resolveSafeOtherTarget(rootDir, relSlashPath string) (string, error) {
	cleanRel, err := cleanRelUnderRoot(relSlashPath)
	if err != nil || cleanRel == "." {
		return "", NewError(400, "INVALID_PARAMETERS", "invalid relative file path")
	}
	if err := validateNoSymlinkDirChain(rootDir, path.Dir(cleanRel)); err != nil {
		return "", err
	}
	full := filepath.Join(rootDir, filepath.FromSlash(cleanRel))
	if st, lErr := os.Lstat(full); lErr == nil {
		if isReparseOrSymlink(st) || st.IsDir() {
			return "", NewError(400, "UNSAFE_MODEL_FORMAT", "symlink or directory rejected")
		}
	} else if !errors.Is(lErr, os.ErrNotExist) {
		return "", lErr
	}
	return full, nil
}

type otherNamespace struct {
	rootDir  string
	lockFile *os.File
	rootDev  uint64
}

func openLockedNamespacePlatform(rootDir string) (platformNamespace, error) {
	lockFull, err := resolveSafeOtherTarget(rootDir, ".ns.lock")
	if err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(lockFull, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed opening .ns.lock: %v", err))
	}
	id, idErr := inspectOpenFileIdentity(lf)
	if idErr != nil {
		_ = lf.Close()
		return nil, NewError(500, "STORAGE_IO_ERROR", ".ns.lock failed regular-file inspection")
	}
	if flErr := unix.Flock(int(lf.Fd()), unix.LOCK_EX); flErr != nil {
		_ = lf.Close()
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed locking .ns.lock: %v", flErr))
	}
	return &otherNamespace{rootDir: rootDir, lockFile: lf, rootDev: id.Dev}, nil
}

func (n *otherNamespace) RootDev() uint64 {
	return n.rootDev
}

func (n *otherNamespace) Close() {
	if n.lockFile != nil {
		_ = unix.Flock(int(n.lockFile.Fd()), unix.LOCK_UN)
		_ = n.lockFile.Close()
		n.lockFile = nil
	}
}

func (n *otherNamespace) StatInDir(relDirSlash, baseName string) (FileIdentity, error) {
	return statBeneathNoFollow(n.rootDir, relDirSlash+"/"+baseName)
}

func (n *otherNamespace) ListDirNames(relDirSlash string) ([]string, error) {
	if err := validateNoSymlinkDirChain(n.rootDir, relDirSlash); err != nil {
		return nil, err
	}
	full := n.rootDir
	if relDirSlash != "." && relDirSlash != "" {
		full = filepath.Join(n.rootDir, filepath.FromSlash(relDirSlash))
	}
	dirFile, err := os.OpenFile(full, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer dirFile.Close()
	names, rErr := dirFile.Readdirnames(-1)
	if rErr != nil {
		return nil, rErr
	}
	sort.Strings(names)
	return names, nil
}

func (n *otherNamespace) OpenReadOnlyInDir(relDirSlash, baseName string) (*os.File, FileIdentity, error) {
	f, err := openRegularReadOnlyPlatform(n.rootDir, relDirSlash+"/"+baseName)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	id, idErr := inspectOpenFileIdentity(f)
	if idErr != nil {
		_ = f.Close()
		return nil, FileIdentity{}, idErr
	}
	return f, id, nil
}

func (n *otherNamespace) LinkOpenFileToDir(srcFile *os.File, expectedID FileIdentity, dstRelDirSlash, dstBaseName string) error {
	dstFull, err := resolveSafeOtherTarget(n.rootDir, dstRelDirSlash+"/"+dstBaseName)
	if err != nil {
		return err
	}
	curID, idErr := inspectOpenFileIdentity(srcFile)
	if idErr != nil || curID.Dev != expectedID.Dev || curID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "verified source FD identity mismatch")
	}
	if lErr := os.Link(srcFile.Name(), dstFull); lErr != nil {
		return lErr
	}
	linkedID, sErr := n.StatInDir(dstRelDirSlash, dstBaseName)
	if sErr != nil || linkedID.Dev != expectedID.Dev || linkedID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "linked destination inode mismatch; preserved intact")
	}
	return nil
}

func (n *otherNamespace) RenameNoReplaceInDirs(srcRelDirSlash, srcBaseName, dstRelDirSlash, dstBaseName string) error {
	oldFull, err := resolveSafeOtherTarget(n.rootDir, srcRelDirSlash+"/"+srcBaseName)
	if err != nil {
		return err
	}
	newFull, err := resolveSafeOtherTarget(n.rootDir, dstRelDirSlash+"/"+dstBaseName)
	if err != nil {
		return err
	}
	if err := os.Link(oldFull, newFull); err != nil {
		return err
	}
	return os.Remove(oldFull)
}

func (n *otherNamespace) UnlinkInQuarantine(qName string, _, _ string) error {
	full, err := resolveSafeOtherTarget(n.rootDir, ".quarantine/"+qName)
	if err != nil {
		return err
	}
	return os.Remove(full)
}

func (n *otherNamespace) TryExclusiveHandoffPart(prevBase, _ string, expectedID FileIdentity) (bool, WriterProbeState, error) {
	f, id, err := n.OpenReadOnlyInDir(".staging", prevBase)
	if err != nil {
		return false, WriterProbeUnprovable, err
	}
	defer f.Close()
	if id.Dev != expectedID.Dev || id.Ino != expectedID.Ino || id.Nlink != 1 || id.Size != expectedID.Size {
		return false, WriterProbeUnprovable, errors.New("resume candidate identity changed")
	}
	if flErr := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); flErr != nil {
		return false, WriterProbeActiveWriter, nil
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	// Non-Linux POSIX lacks F_SETLEASE: never share old inode across fences; force independent inode copy!
	return false, WriterProbeUnprovable, nil
}

func (n *otherNamespace) CopyPartToIndependentInode(prevBase, dstBase string, copyBytes int64, prevIno uint64) (FileIdentity, error) {
	srcF, _, err := n.OpenReadOnlyInDir(".staging", prevBase)
	if err != nil {
		return FileIdentity{}, err
	}
	defer srcF.Close()
	dstFull, err := resolveSafeOtherTarget(n.rootDir, ".staging/"+dstBase)
	if err != nil {
		return FileIdentity{}, err
	}
	dstF, err := os.OpenFile(dstFull, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return FileIdentity{}, err
	}
	dstID, idErr := inspectOpenFileIdentity(dstF)
	if idErr != nil {
		_ = dstF.Close()
		return FileIdentity{}, errors.New("independent staging .part failed identity check")
	}
	cleanup := func() {
		_ = dstF.Close()
		_, _, _ = quarantineAndConfirmUnlink(n, ".staging", dstBase, &dstID, "", "")
	}
	if dstID.Nlink != 1 || dstID.Ino == prevIno {
		cleanup()
		return FileIdentity{}, errors.New("independent staging .part failed distinct inode check")
	}
	copied, cErr := io.CopyN(dstF, srcF, copyBytes)
	if cErr != nil || copied != copyBytes {
		cleanup()
		return FileIdentity{}, errors.New("failed copying prefix bytes")
	}
	if sErr := dstF.Sync(); sErr != nil {
		cleanup()
		return FileIdentity{}, sErr
	}
	finalID, fErr := inspectOpenFileIdentity(dstF)
	if fErr != nil || finalID.Dev != dstID.Dev || finalID.Ino != dstID.Ino {
		cleanup()
		return FileIdentity{}, errors.New("independent staging .part identity changed")
	}
	if err := dstF.Close(); err != nil {
		return FileIdentity{}, err
	}
	return finalID, nil
}

func (n *otherNamespace) FsyncDir(relDirSlash string) error {
	if err := validateNoSymlinkDirChain(n.rootDir, relDirSlash); err != nil {
		return err
	}
	full := n.rootDir
	if relDirSlash != "." && relDirSlash != "" {
		full = filepath.Join(n.rootDir, filepath.FromSlash(relDirSlash))
	}
	f, err := os.Open(full)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func openStagingPartForWritePlatform(rootDir, relSlashPath string, exclusive bool) (*os.File, error) {
	full, err := resolveSafeOtherTarget(rootDir, relSlashPath)
	if err != nil {
		return nil, err
	}
	flags := os.O_RDWR | syscall.O_NOFOLLOW
	if exclusive {
		flags |= os.O_CREATE | os.O_EXCL
	}
	f, err := os.OpenFile(full, flags, 0o600)
	if err != nil {
		return nil, err
	}
	if flErr := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); flErr != nil {
		_ = f.Close()
		return nil, NewError(409, "OPERATION_IN_PROGRESS", "staging .part is locked by an active writer")
	}
	return f, nil
}

func openRegularReadOnlyPlatform(rootDir, relSlashPath string) (*os.File, error) {
	full, err := resolveSafeOtherTarget(rootDir, relSlashPath)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(full, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

func ensureNoOpenWriterOnVerifiedPartPlatform(f *os.File, _, _ string) error {
	if flErr := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); flErr != nil {
		return NewError(409, "OPERATION_IN_PROGRESS", "staging .part still has an active open writer FD")
	}
	return nil
}

func releaseVerifiedPartWriterProbePlatform(f *os.File) {
	if f != nil {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	}
}

func inspectOpenFileIdentity(f *os.File) (FileIdentity, error) {
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return FileIdentity{}, NewError(400, "UNSAFE_MODEL_FORMAT", "non-regular file")
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return FileIdentity{}, errors.New("missing stat_t")
	}
	return FileIdentity{
		Dev:   uint64(sys.Dev),
		Ino:   uint64(sys.Ino),
		Nlink: uint64(sys.Nlink),
		Perm:  st.Mode().Perm(),
		Uid:   sys.Uid,
		Size:  st.Size(),
	}, nil
}

func statBeneathNoFollow(rootDir, relSlashPath string) (FileIdentity, error) {
	full, err := resolveSafeOtherTarget(rootDir, relSlashPath)
	if err != nil {
		return FileIdentity{}, err
	}
	st, lErr := os.Lstat(full)
	if lErr != nil {
		return FileIdentity{}, lErr
	}
	if !st.Mode().IsRegular() {
		return FileIdentity{}, NewError(400, "UNSAFE_MODEL_FORMAT", "symlink or non-regular file")
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return FileIdentity{}, errors.New("missing stat_t")
	}
	return FileIdentity{
		Dev:   uint64(sys.Dev),
		Ino:   uint64(sys.Ino),
		Nlink: uint64(sys.Nlink),
		Perm:  st.Mode().Perm(),
		Uid:   sys.Uid,
		Size:  st.Size(),
	}, nil
}

func setFileReadOnlyPlatform(f *os.File, _, _ string) error {
	return f.Chmod(0o444)
}

func verifyReadOnlyOwnershipPlatform(id FileIdentity) error {
	if id.Perm != 0o444 {
		return NewError(500, "STORAGE_IO_ERROR", "existing CAS file does not have immutable 0444 permissions")
	}
	return nil
}

func platformDiskSpace(rootDir string) (int64, int64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(rootDir, &s); err != nil {
		return 0, 0, err
	}
	return int64(s.Blocks) * int64(s.Bsize), int64(s.Bavail) * int64(s.Bsize), nil
}
