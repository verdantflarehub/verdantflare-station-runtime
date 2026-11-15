//go:build windows

package modelcache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func isReparseOrSymlink(info os.FileInfo) bool {
	if info.Mode()&os.ModeSymlink != 0 {
		return true
	}
	if sys, ok := info.Sys().(*syscall.Win32FileAttributeData); ok {
		if sys.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return true
		}
	}
	return false
}

func syncVerifiedPartFile(_ *os.File) error {
	return nil
}

func resolveSafeWindowsTarget(rootDir, relSlashPath string) (string, error) {
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
			return "", NewError(400, "UNSAFE_MODEL_FORMAT", "reparse point, symlink, or directory rejected")
		}
	} else if !errors.Is(lErr, os.ErrNotExist) {
		return "", lErr
	}
	return full, nil
}

func inspectHandleInfo(h windows.Handle, permHint os.FileMode) (FileIdentity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return FileIdentity{}, err
	}
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_REPARSE_POINT|windows.FILE_ATTRIBUTE_DIRECTORY) != 0 {
		return FileIdentity{}, NewError(400, "UNSAFE_MODEL_FORMAT", "reparse point or directory handle rejected")
	}
	perm := permHint
	if info.FileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		perm = 0o444
	} else if perm == 0 {
		perm = 0o600
	}
	size := (int64(info.FileSizeHigh) << 32) | int64(info.FileSizeLow)
	ino := (uint64(info.FileIndexHigh) << 32) | uint64(info.FileIndexLow)
	return FileIdentity{
		Dev:   uint64(info.VolumeSerialNumber),
		Ino:   ino,
		Nlink: uint64(info.NumberOfLinks),
		Perm:  perm,
		Size:  size,
	}, nil
}

type windowsNamespace struct {
	rootDir    string
	lockHandle windows.Handle
	rootDev    uint64
	dirHandles map[string]windows.Handle
}

func openPinnedDirHandleWindows(fullDir string) (windows.Handle, error) {
	ptr, err := windows.UTF16PtrFromString(fullDir)
	if err != nil {
		return windows.InvalidHandle, err
	}
	// Omit FILE_SHARE_DELETE so no concurrent process can rename, replace, or delete pinned cache directories!
	// Note: FILE_WRITE_DATA (0x0002) == FILE_ADD_FILE on directory handles in WinNT.
	h, err := windows.CreateFile(
		ptr,
		windows.FILE_LIST_DIRECTORY|windows.FILE_WRITE_DATA|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return windows.InvalidHandle, err
	}
	var info windows.ByHandleFileInformation
	if gErr := windows.GetFileInformationByHandle(h, &info); gErr != nil ||
		info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
		info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(h)
		return windows.InvalidHandle, NewError(500, "STORAGE_IO_ERROR", "pinned cache directory is missing or a reparse point")
	}
	return h, nil
}

func openLockedNamespacePlatform(rootDir string) (platformNamespace, error) {
	if err := validateNoSymlinkDirChain(rootDir, "."); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(rootDir, ".ns.lock")
	if st, lErr := os.Lstat(lockPath); lErr == nil && (isReparseOrSymlink(st) || st.IsDir()) {
		return nil, NewError(500, "STORAGE_IO_ERROR", ".ns.lock must not be a symlink or directory")
	}
	lockPtr, err := windows.UTF16PtrFromString(lockPath)
	if err != nil {
		return nil, err
	}
	lh, err := windows.CreateFile(
		lockPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_ALWAYS,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed opening .ns.lock: %v", err))
	}
	lockID, idErr := inspectHandleInfo(lh, 0o600)
	if idErr != nil {
		_ = windows.CloseHandle(lh)
		return nil, NewError(500, "STORAGE_IO_ERROR", ".ns.lock failed regular-file verification")
	}
	var ol windows.Overlapped
	if lErr := windows.LockFileEx(lh, windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &ol); lErr != nil {
		_ = windows.CloseHandle(lh)
		return nil, NewError(500, "STORAGE_IO_ERROR", fmt.Sprintf("failed locking .ns.lock: %v", lErr))
	}

	ns := &windowsNamespace{
		rootDir:    rootDir,
		lockHandle: lh,
		rootDev:    lockID.Dev,
		dirHandles: map[string]windows.Handle{},
	}
	for _, rel := range []string{".", ".staging", ".quarantine", "cas", "views"} {
		if _, dErr := ns.ensurePinnedDir(rel); dErr != nil {
			ns.Close()
			return nil, dErr
		}
	}
	return ns, nil
}

func (n *windowsNamespace) RootDev() uint64 {
	return n.rootDev
}

func (n *windowsNamespace) Close() {
	for k, h := range n.dirHandles {
		if h != windows.InvalidHandle && h != 0 {
			_ = windows.CloseHandle(h)
		}
		delete(n.dirHandles, k)
	}
	if n.lockHandle != windows.InvalidHandle && n.lockHandle != 0 {
		var ol windows.Overlapped
		_ = windows.UnlockFileEx(n.lockHandle, 0, 1, 0, &ol)
		_ = windows.CloseHandle(n.lockHandle)
		n.lockHandle = windows.InvalidHandle
	}
}

func (n *windowsNamespace) ensurePinnedDir(relDirSlash string) (string, error) {
	clean, err := cleanRelUnderRoot(relDirSlash)
	if err != nil {
		return "", err
	}
	full := n.rootDir
	if clean != "." {
		full = filepath.Join(n.rootDir, filepath.FromSlash(clean))
	}
	if _, ok := n.dirHandles[clean]; !ok {
		h, oErr := openPinnedDirHandleWindows(full)
		if oErr != nil {
			return "", oErr
		}
		n.dirHandles[clean] = h
	}
	return full, nil
}

func (n *windowsNamespace) resolvePinnedChild(relDirSlash, baseName string) (string, error) {
	if _, err := cleanRelUnderRoot(baseName); err != nil || path.Base(baseName) != baseName {
		return "", NewError(400, "INVALID_PARAMETERS", "invalid child filename")
	}
	dirFull, err := n.ensurePinnedDir(relDirSlash)
	if err != nil {
		return "", err
	}
	full := filepath.Join(dirFull, baseName)
	if st, lErr := os.Lstat(full); lErr == nil {
		if isReparseOrSymlink(st) || st.IsDir() {
			return "", NewError(400, "UNSAFE_MODEL_FORMAT", "reparse point, symlink, or directory rejected")
		}
	} else if !errors.Is(lErr, os.ErrNotExist) {
		return "", lErr
	}
	return full, nil
}

func (n *windowsNamespace) StatInDir(relDirSlash, baseName string) (FileIdentity, error) {
	full, err := n.resolvePinnedChild(relDirSlash, baseName)
	if err != nil {
		return FileIdentity{}, err
	}
	ptr, err := windows.UTF16PtrFromString(full)
	if err != nil {
		return FileIdentity{}, err
	}
	h, err := windows.CreateFile(
		ptr,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return FileIdentity{}, os.ErrNotExist
		}
		return FileIdentity{}, err
	}
	defer windows.CloseHandle(h)
	return inspectHandleInfo(h, 0)
}

func (n *windowsNamespace) OpenReadOnlyInDir(relDirSlash, baseName string) (*os.File, FileIdentity, error) {
	full, err := n.resolvePinnedChild(relDirSlash, baseName)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	ptr, err := windows.UTF16PtrFromString(full)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	// Omit FILE_SHARE_WRITE so bytes can NEVER be mutated while this verified handle is open!
	// Include metadata-only FILE_WRITE_ATTRIBUTES (no FILE_WRITE_DATA) so SetFileInformationByHandle(FileLinkInfo)
	// links directly from this open handle without any source-path fallback.
	h, err := windows.CreateFile(
		ptr,
		windows.GENERIC_READ|windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return nil, FileIdentity{}, os.ErrNotExist
		}
		return nil, FileIdentity{}, err
	}
	id, iErr := inspectHandleInfo(h, 0o444)
	if iErr != nil {
		_ = windows.CloseHandle(h)
		return nil, FileIdentity{}, iErr
	}
	return os.NewFile(uintptr(h), full), id, nil
}

var (
	ntdllMod                  = windows.NewLazySystemDLL("ntdll.dll")
	procNtSetInformationFile  = ntdllMod.NewProc("NtSetInformationFile")
)

type ioStatusBlockWindows struct {
	Status      uintptr
	Information uintptr
}

// setFileLinkByHandleWindows creates a hardlink directly from an open Windows file HANDLE
// into the pinned destination directory HANDLE via ntdll!NtSetInformationFile(FileLinkInformation=11)
// with ReplaceIfExists=FALSE, eliminating BOTH source-path and destination-path TOCTOU (R2-H02, R3-H02).
func setFileLinkByHandleWindows(h windows.Handle, dstDirHandle windows.Handle, dstBaseName string) error {
	if dstDirHandle == 0 || dstDirHandle == windows.InvalidHandle {
		return NewError(500, "STORAGE_IO_ERROR", "missing pinned destination directory handle for hardlink")
	}
	utf16Chars, err := windows.UTF16FromString(dstBaseName)
	if err != nil {
		return err
	}
	if len(utf16Chars) > 0 && utf16Chars[len(utf16Chars)-1] == 0 {
		utf16Chars = utf16Chars[:len(utf16Chars)-1]
	}
	nameBytesLen := len(utf16Chars) * 2
	// NT FILE_LINK_INFORMATION layout:
	//   64-bit: offset 0 ReplaceIfExists (1+7), offset 8 RootDirectory (8), offset 16 FileNameLength (4), offset 20 FileName[]
	//   32-bit: offset 0 ReplaceIfExists (1+3), offset 4 RootDirectory (4), offset 8  FileNameLength (4), offset 12 FileName[]
	ptrSize := int(unsafe.Sizeof(uintptr(0)))
	rootOffset := 8
	headerSize := 8 + ptrSize + 4
	if ptrSize == 4 {
		rootOffset = 4
		headerSize = 4 + 4 + 4
	}
	buf := make([]byte, headerSize+nameBytesLen)
	buf[0] = 0 // ReplaceIfExists = FALSE
	if ptrSize == 8 {
		binary.LittleEndian.PutUint64(buf[rootOffset:rootOffset+8], uint64(dstDirHandle))
	} else {
		binary.LittleEndian.PutUint32(buf[rootOffset:rootOffset+4], uint32(dstDirHandle))
	}
	lenOffset := headerSize - 4
	binary.LittleEndian.PutUint32(buf[lenOffset:lenOffset+4], uint32(nameBytesLen))
	for i, ch := range utf16Chars {
		off := headerSize + i*2
		binary.LittleEndian.PutUint16(buf[off:off+2], ch)
	}

	const ntFileLinkInformation = 11
	var iosb ioStatusBlockWindows
	r1, _, _ := procNtSetInformationFile.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&iosb)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		uintptr(ntFileLinkInformation),
	)
	if r1 == 0 {
		return nil
	}
	st := windows.NTStatus(r1)
	if st == windows.STATUS_OBJECT_NAME_COLLISION {
		return os.ErrExist
	}
	return st.Errno()
}

type fileBasicInfoWindows struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	FileAttributes uint32
	_              uint32
}

func setHandleReadOnlyWindows(h windows.Handle, readOnly bool) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return err
	}
	attrs := info.FileAttributes
	if readOnly {
		attrs |= windows.FILE_ATTRIBUTE_READONLY
	} else {
		attrs &^= windows.FILE_ATTRIBUTE_READONLY
		if attrs == 0 {
			attrs = windows.FILE_ATTRIBUTE_NORMAL
		}
	}
	bi := fileBasicInfoWindows{
		FileAttributes: attrs,
	}
	return windows.SetFileInformationByHandle(
		h,
		windows.FileBasicInfo,
		(*byte)(unsafe.Pointer(&bi)),
		uint32(unsafe.Sizeof(bi)),
	)
}

func (n *windowsNamespace) LinkOpenFileToDir(srcFile *os.File, expectedID FileIdentity, dstRelDirSlash, dstBaseName string) error {
	if _, err := n.resolvePinnedChild(dstRelDirSlash, dstBaseName); err != nil {
		return err
	}
	cleanDstDir, err := cleanRelUnderRoot(dstRelDirSlash)
	if err != nil {
		return err
	}
	curID, idErr := inspectOpenFileIdentity(srcFile)
	if idErr != nil || curID.Dev != expectedID.Dev || curID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "verified source handle identity mismatch before link")
	}

	srcH := windows.Handle(srcFile.Fd())
	lErr := setFileLinkByHandleWindows(srcH, n.dirHandles[cleanDstDir], dstBaseName)
	if lErr != nil {
		return lErr
	}

	linkedID, sErr := n.StatInDir(dstRelDirSlash, dstBaseName)
	if sErr != nil || linkedID.Dev != expectedID.Dev || linkedID.Ino != expectedID.Ino {
		return NewError(500, "STORAGE_IO_ERROR", "linked destination FileIndex did not match verified source handle; preserved intact")
	}
	if dstRelDirSlash != ".staging" {
		_ = setHandleReadOnlyWindows(srcH, true)
	}
	return nil
}

func (n *windowsNamespace) RenameNoReplaceInDirs(srcRelDirSlash, srcBaseName, dstRelDirSlash, dstBaseName string) error {
	srcFull, err := n.resolvePinnedChild(srcRelDirSlash, srcBaseName)
	if err != nil {
		return err
	}
	dstFull, err := n.resolvePinnedChild(dstRelDirSlash, dstBaseName)
	if err != nil {
		return err
	}
	srcPtr, err := windows.UTF16PtrFromString(srcFull)
	if err != nil {
		return err
	}
	dstPtr, err := windows.UTF16PtrFromString(dstFull)
	if err != nil {
		return err
	}
	// MoveFileEx with flags=0 atomically renames on NTFS without replacing dst and preserves 0444 attribute!
	mErr := windows.MoveFileEx(srcPtr, dstPtr, 0)
	if mErr == nil {
		return nil
	}
	if errors.Is(mErr, windows.ERROR_ALREADY_EXISTS) || errors.Is(mErr, windows.ERROR_FILE_EXISTS) {
		return os.ErrExist
	}
	if errors.Is(mErr, windows.ERROR_FILE_NOT_FOUND) || errors.Is(mErr, windows.ERROR_PATH_NOT_FOUND) {
		return os.ErrNotExist
	}
	return mErr
}

func (n *windowsNamespace) UnlinkInQuarantine(qName string, restoreReadOnlyRelDir, restoreReadOnlyBase string) error {
	qFull, err := n.resolvePinnedChild(".quarantine", qName)
	if err != nil {
		return err
	}
	_ = os.Chmod(qFull, 0o600)
	rmErr := os.Remove(qFull)
	if restoreReadOnlyRelDir != "" && restoreReadOnlyBase != "" {
		if roFull, rErr := n.resolvePinnedChild(restoreReadOnlyRelDir, restoreReadOnlyBase); rErr == nil {
			_ = os.Chmod(roFull, 0o444)
		}
	}
	if errors.Is(rmErr, os.ErrNotExist) {
		return os.ErrNotExist
	}
	return rmErr
}

func (n *windowsNamespace) TryExclusiveHandoffPart(prevBase, dstBase string, expectedID FileIdentity) (bool, WriterProbeState, error) {
	prevFull, err := n.resolvePinnedChild(".staging", prevBase)
	if err != nil {
		return false, WriterProbeUnprovable, err
	}
	ptr, err := windows.UTF16PtrFromString(prevFull)
	if err != nil {
		return false, WriterProbeUnprovable, err
	}
	// dwShareMode = 0 atomically fails with ERROR_SHARING_VIOLATION if ANY process holds ANY open handle!
	h, oErr := windows.CreateFile(
		ptr,
		windows.GENERIC_READ|windows.DELETE,
		0,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if oErr != nil {
		if errors.Is(oErr, windows.ERROR_SHARING_VIOLATION) || errors.Is(oErr, windows.ERROR_LOCK_VIOLATION) {
			return false, WriterProbeActiveWriter, nil
		}
		return false, WriterProbeUnprovable, oErr
	}
	id, idErr := inspectHandleInfo(h, 0o600)
	_ = windows.CloseHandle(h)
	if idErr != nil || id.Dev != expectedID.Dev || id.Ino != expectedID.Ino || id.Nlink != 1 || id.Size != expectedID.Size {
		return false, WriterProbeUnprovable, errors.New("staging resume candidate identity changed")
	}
	if rErr := n.RenameNoReplaceInDirs(".staging", prevBase, ".staging", dstBase); rErr != nil {
		return false, WriterProbeProvenSafe, rErr
	}
	return true, WriterProbeProvenSafe, nil
}

func (n *windowsNamespace) CopyPartToIndependentInode(prevBase, dstBase string, copyBytes int64, prevIno uint64) (FileIdentity, error) {
	prevFull, err := n.resolvePinnedChild(".staging", prevBase)
	if err != nil {
		return FileIdentity{}, err
	}
	dstFull, err := n.resolvePinnedChild(".staging", dstBase)
	if err != nil {
		return FileIdentity{}, err
	}
	prevPtr, err := windows.UTF16PtrFromString(prevFull)
	if err != nil {
		return FileIdentity{}, err
	}
	// Allow sharing with a paused writer in another process so we can read prefix bytes into a NEW independent inode!
	srcH, sErr := windows.CreateFile(
		prevPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if sErr != nil {
		return FileIdentity{}, sErr
	}
	srcFile := os.NewFile(uintptr(srcH), prevFull)
	defer srcFile.Close()

	dstPtr, err := windows.UTF16PtrFromString(dstFull)
	if err != nil {
		return FileIdentity{}, err
	}
	dstH, dErr := windows.CreateFile(
		dstPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ,
		nil,
		windows.CREATE_NEW,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if dErr != nil {
		return FileIdentity{}, dErr
	}
	dstFile := os.NewFile(uintptr(dstH), dstFull)
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
		return FileIdentity{}, errors.New("independent staging .part failed distinct FileIndex verification")
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

func (n *windowsNamespace) FsyncDir(relDirSlash string) error {
	_, err := n.ensurePinnedDir(relDirSlash)
	return err
}

func openStagingPartForWritePlatform(rootDir, relSlashPath string, exclusive bool) (*os.File, error) {
	full, err := resolveSafeWindowsTarget(rootDir, relSlashPath)
	if err != nil {
		return nil, err
	}
	ptr, err := windows.UTF16PtrFromString(full)
	if err != nil {
		return nil, err
	}
	creation := uint32(windows.OPEN_EXISTING)
	if exclusive {
		creation = windows.CREATE_NEW
	}
	// Share ONLY FILE_SHARE_READ (never WRITE or DELETE) while writer handle is open (R2-H01).
	h, err := windows.CreateFile(
		ptr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ,
		nil,
		creation,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return nil, os.ErrNotExist
		}
		if errors.Is(err, windows.ERROR_FILE_EXISTS) || errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
			return nil, os.ErrExist
		}
		if errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, NewError(409, "OPERATION_IN_PROGRESS", "staging .part is locked by an active writer")
		}
		return nil, err
	}
	if _, iErr := inspectHandleInfo(h, 0o600); iErr != nil {
		_ = windows.CloseHandle(h)
		return nil, iErr
	}
	return os.NewFile(uintptr(h), full), nil
}

func openRegularReadOnlyPlatform(rootDir, relSlashPath string) (*os.File, error) {
	full, err := resolveSafeWindowsTarget(rootDir, relSlashPath)
	if err != nil {
		return nil, err
	}
	ptr, err := windows.UTF16PtrFromString(full)
	if err != nil {
		return nil, err
	}
	// Omit FILE_SHARE_WRITE: succeeds ONLY when zero writable handles exist on this inode!
	// Include metadata-only FILE_WRITE_ATTRIBUTES (no FILE_WRITE_DATA) so handle-based
	// FileBasicInfo (0444) and FileLinkInfo (hardlink from handle) operate without path re-walks.
	h, err := windows.CreateFile(
		ptr,
		windows.GENERIC_READ|windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return nil, os.ErrNotExist
		}
		if errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			return nil, NewError(409, "OPERATION_IN_PROGRESS", "staging .part still has an open writable handle")
		}
		return nil, err
	}
	if _, iErr := inspectHandleInfo(h, 0o444); iErr != nil {
		_ = windows.CloseHandle(h)
		return nil, iErr
	}
	return os.NewFile(uintptr(h), full), nil
}

func ensureNoOpenWriterOnVerifiedPartPlatform(_ *os.File, _, _ string) error {
	// openRegularReadOnlyPlatform already omits FILE_SHARE_WRITE, guaranteeing zero open write handles!
	return nil
}

func releaseVerifiedPartWriterProbePlatform(_ *os.File) {}

func inspectOpenFileIdentity(f *os.File) (FileIdentity, error) {
	return inspectHandleInfo(windows.Handle(f.Fd()), 0)
}

func statBeneathNoFollow(rootDir, relSlashPath string) (FileIdentity, error) {
	full, err := resolveSafeWindowsTarget(rootDir, relSlashPath)
	if err != nil {
		return FileIdentity{}, err
	}
	ptr, err := windows.UTF16PtrFromString(full)
	if err != nil {
		return FileIdentity{}, err
	}
	h, err := windows.CreateFile(
		ptr,
		windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return FileIdentity{}, os.ErrNotExist
		}
		return FileIdentity{}, err
	}
	defer windows.CloseHandle(h)
	return inspectHandleInfo(h, 0)
}

func setFileReadOnlyPlatform(f *os.File, rootDir, relSlashPath string) error {
	if f != nil {
		return setHandleReadOnlyWindows(windows.Handle(f.Fd()), true)
	}
	full, err := resolveSafeWindowsTarget(rootDir, relSlashPath)
	if err != nil {
		return err
	}
	return os.Chmod(full, 0o444)
}

func verifyReadOnlyOwnershipPlatform(id FileIdentity) error {
	if id.Perm != 0o444 {
		return NewError(500, "STORAGE_IO_ERROR", "existing CAS file does not have immutable 0444 permissions")
	}
	return nil
}

func platformDiskSpace(rootDir string) (int64, int64, error) {
	ptr, err := windows.UTF16PtrFromString(rootDir)
	if err != nil {
		return 0, 0, err
	}
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(ptr, &freeBytesAvailable, &totalNumberOfBytes, &totalNumberOfFreeBytes); err != nil {
		return 0, 0, err
	}
	return int64(totalNumberOfBytes), int64(freeBytesAvailable), nil
}
