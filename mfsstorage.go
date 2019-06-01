package main

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"os"
	"sync"
)

var (
	errFileOpen = errors.New("leveldb/storage: file still open")
	errReadOnly = errors.New("leveldb/storage: storage is read-only")
)

const typeShift = 4

// Verify at compile-time that typeShift is large enough to cover all FileType
// values by confirming that 0 == 0.
var _ [0]struct{} = [storage.TypeAll >> typeShift]struct{}{}

type mfsStorageLock struct {
	ms *mfsStorage
}

func (lock *mfsStorageLock) Unlock() {
	ms := lock.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock == lock {
		ms.slock = nil
	}
	return
}

// mfsStorage is a memory-backed storage.
type mfsStorage struct {
	mu    sync.Mutex
	slock *mfsStorageLock
	files map[uint64]*memFile
	meta  storage.FileDesc
}

// NewmfsStorage returns a new memory-backed storage implementation.
func NewMFSStorage() storage.Storage {
	return &mfsStorage{
		files: make(map[uint64]*memFile),
	}
}

func (ms *mfsStorage) Lock() (storage.Locker, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, storage.ErrLocked
	}
	ms.slock = &mfsStorageLock{ms: ms}
	return ms.slock, nil
}

func (*mfsStorage) Log(str string) {}

func (ms *mfsStorage) SetMeta(fd storage.FileDesc) error {
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	ms.meta = fd
	ms.mu.Unlock()
	return nil
}

func (ms *mfsStorage) GetMeta() (storage.FileDesc, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.meta.Zero() {
		return storage.FileDesc{}, os.ErrNotExist
	}
	return ms.meta, nil
}

func (ms *mfsStorage) List(ft storage.FileType) ([]storage.FileDesc, error) {
	ms.mu.Lock()
	var fds []storage.FileDesc
	for x := range ms.files {
		fd := unpackFile(x)
		if fd.Type&ft != 0 {
			fds = append(fds, fd)
		}
	}
	ms.mu.Unlock()
	return fds, nil
}

func (ms *mfsStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, exist := ms.files[packFile(fd)]; exist {
		if m.open {
			return nil, errFileOpen
		}
		m.open = true
		return &memReader{Reader: bytes.NewReader(m.Bytes()), ms: ms, m: m}, nil
	}
	return nil, os.ErrNotExist
}

func (ms *mfsStorage) Create(fd storage.FileDesc) (storage.Writer, error) {
	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	m, exist := ms.files[x]
	if exist {
		if m.open {
			return nil, errFileOpen
		}
		m.Reset()
	} else {
		m = &memFile{}
		ms.files[x] = m
	}
	m.open = true
	return &memWriter{memFile: m, ms: ms}, nil
}

func (ms *mfsStorage) Remove(fd storage.FileDesc) error {
	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, exist := ms.files[x]; exist {
		delete(ms.files, x)
		return nil
	}
	return os.ErrNotExist
}

func (ms *mfsStorage) Rename(oldfd, newfd storage.FileDesc) error {
	if !storage.FileDescOk(oldfd) || !storage.FileDescOk(newfd) {
		return storage.ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}

	oldx := packFile(oldfd)
	newx := packFile(newfd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	oldm, exist := ms.files[oldx]
	if !exist {
		return os.ErrNotExist
	}
	newm, exist := ms.files[newx]
	if (exist && newm.open) || oldm.open {
		return errFileOpen
	}
	delete(ms.files, oldx)
	ms.files[newx] = oldm
	return nil
}

func (*mfsStorage) Close() error { return nil }

type memFile struct {
	bytes.Buffer
	open bool
}

type memReader struct {
	*bytes.Reader
	ms     *mfsStorage
	m      *memFile
	closed bool
}

func (mr *memReader) Close() error {
	mr.ms.mu.Lock()
	defer mr.ms.mu.Unlock()
	if mr.closed {
		return storage.ErrClosed
	}
	mr.m.open = false
	return nil
}

type memWriter struct {
	*memFile
	ms     *mfsStorage
	closed bool
}

func (*memWriter) Sync() error { return nil }

func (mw *memWriter) Close() error {
	mw.ms.mu.Lock()
	defer mw.ms.mu.Unlock()
	if mw.closed {
		return storage.ErrClosed
	}
	mw.memFile.open = false
	return nil
}

func packFile(fd storage.FileDesc) uint64 {
	return uint64(fd.Num)<<typeShift | uint64(fd.Type)
}

func unpackFile(x uint64) storage.FileDesc {
	return storage.FileDesc{storage.FileType(x) & storage.TypeAll, int64(x >> typeShift)}
}
