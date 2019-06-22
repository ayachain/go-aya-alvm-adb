package go_aya_alvm_adb

import (
	"context"
	"errors"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	ft "github.com/ipfs/go-unixfs"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"io/ioutil"
	"os"
	"sync"
)

const mfsMetaFilePath = "CURRENT.META"

var (
	errFileOpen = errors.New("leveldb/storage: file still open")
	errReadOnly = errors.New("leveldb/storage: storage is read-only")
)

// mfsStorage is a memory-backed storage.
type mfsStorage struct {

	storage.Storage

	mu    		sync.Mutex

	slock 		*mfsStorageLock

	mdir 		*mfs.Directory

	openedWarp	map[storage.FileDesc]*mfsfileWrap
}

// NewmfsStorage returns a new memory-backed storage implementation.
func NewMFSStorage( mdir *mfs.Directory ) storage.Storage {
	return &mfsStorage{
		mdir: mdir,
		openedWarp:make(map[storage.FileDesc]*mfsfileWrap),
	}
}

func (ms *mfsStorage) Log(str string) {}

func (ms *mfsStorage) SetMeta(fd storage.FileDesc) error {

	content := fsGenName(fd) + "\n"

	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	_ = ms.mdir.Unlink(mfsMetaFilePath)

	nnd := dag.NodeWithData( ft.FilePBData([]byte(content), 0) )
	nnd.SetCidBuilder(ms.mdir.GetCidBuilder())

	if err := ms.mdir.AddChild( mfsMetaFilePath, nnd ); err != nil {
		return err
	}

	return nil
}

func (ms *mfsStorage) GetMeta() (storage.FileDesc, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	mnd, err := ms.mdir.Child( mfsMetaFilePath )
	if err != nil {
		return storage.FileDesc{}, os.ErrNotExist
	}

	fi, ok := mnd.(*mfs.File)
	if !ok {
		return storage.FileDesc{}, os.ErrNotExist
	}

	rd, err := fi.Open(mfs.Flags{Read:true, Sync:false})
	if err != nil {
		return storage.FileDesc{}, os.ErrNotExist
	}
	defer rd.Close()

	bs, err := ioutil.ReadAll(rd)
	if err != nil {
		return storage.FileDesc{}, os.ErrNotExist
	}

	fd, ok := fsParseName(string(bs))
	if !ok {
		return storage.FileDesc{}, os.ErrNotExist
	}

	return fd, nil
}

func (ms *mfsStorage) List(ft storage.FileType) ([]storage.FileDesc, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	var fds []storage.FileDesc

	lnames, err := ms.mdir.ListNames(context.TODO())
	if err != nil {
		return fds, err
	}

	for _, v := range lnames {

		if fd, ok := fsParseName(v); ok && fd.Type & ft != 0 {
			fds = append(fds, fd)
		}

	}

	return fds, nil
}

func (ms *mfsStorage) Open(fd storage.FileDesc) (storage.Reader, error) {

	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if m, err := ms.mdir.Child( fsGenName(fd) ); err != nil {

		return nil, os.ErrNotExist

	} else {

		if fi, ok := m.(*mfs.File); !ok {

			return nil, os.ErrNotExist

		} else {

			fwt, err := fi.Open(mfs.Flags{Read:true, Sync:false})

			if err != nil {
				return nil, err
			}

			fwrap := &mfsfileWrap{
				FileDescriptor:fwt,
				ms:ms,
				fd:fd,
				closed:false,
			}

			ms.openedWarp[fd] = fwrap

			return fwrap, nil
		}
	}
}

func (ms *mfsStorage) Create(fd storage.FileDesc) (storage.Writer, error) {

	if !storage.FileDescOk(fd) {
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fname := fsGenName(fd)
	nd, err := ms.mdir.Child(fname)

	if err == nil {

		// file already must be truncate
		if err := ms.mdir.Unlink( fname ); err != nil {

			return nil, storage.ErrClosed

		}

	}

	//file not exist
	nnd := dag.NodeWithData(ft.FilePBData(nil, 0))
	nnd.SetCidBuilder(ms.mdir.GetCidBuilder())

	if err := ms.mdir.AddChild( fname, nnd ); err != nil {
		return nil, storage.ErrClosed
	}

	nd, err = ms.mdir.Child(fname)
	if err != nil {
		return nil, storage.ErrClosed
	}

	fi, ok := nd.(*mfs.File)
	if !ok {
		return nil, errors.New("expected *mfs.File, didnt get it. This is likely a race condition")
	}

	fwt, err := fi.Open(mfs.Flags{Write:true, Sync:false})
	if err != nil {
		return nil, err
	}

	return &mfsFileWrite{FileDescriptor:fwt}, nil
}

func (ms *mfsStorage) Remove(fd storage.FileDesc) error {

	if !storage.FileDescOk(fd) {
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fname := fsGenName(fd)
	err := ms.mdir.Unlink(fname)
	if err != nil {
		return os.ErrClosed
	}

	return nil
}

func (ms *mfsStorage) Rename(oldfd, newfd storage.FileDesc) error {

	if !storage.FileDescOk(oldfd) || !storage.FileDescOk(newfd) {
		return storage.ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}

	oldName := fsGenName(oldfd)
	newName := fsGenName(newfd)

	ms.mu.Lock()
	defer ms.mu.Unlock()

	srcObj, err := ms.mdir.Child(oldName)
	if err != nil {
		return os.ErrNotExist
	}

	nd, err := srcObj.GetNode()
	if err != nil {
		return os.ErrNotExist
	}

	err = ms.mdir.AddChild(newName, nd)
	if err != nil {
		return os.ErrClosed
	}

	return ms.mdir.Unlink(oldName)
}

func (ms *mfsStorage) Close() error {

	for _, v := range ms.openedWarp {

		if err := v.Close(); err != nil {
			return os.ErrClosed
		}

	}

	return ms.mdir.Flush()
}