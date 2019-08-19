package go_aya_alvm_adb

import (
	"context"
	"errors"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	ft "github.com/ipfs/go-unixfs"
	"github.com/prometheus/common/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/whyrusleeping/go-logging"
	"io/ioutil"
	"os"
	"sync"
)

const mfsMetaFilePath = "CURRENT.META"

type ReadingFunc func ( db *leveldb.DB ) error

// mfsStorage is a memory-backed storage.
type MFSStorage struct {

	storage.Storage

	mu    		sync.Mutex

	slock 		*mfsStorageLock

	mdir 		*mfs.Directory

	openedWarp	sync.Map

	log			*logging.Logger

	dbkey 		string
}

// NewmfsStorage returns a new memory-backed storage implementation.
func NewMFSStorage( mdir *mfs.Directory, dbkey string ) *MFSStorage {

	logging.SetLevel( logging.NOTICE, dbkey )

	return &MFSStorage{
		mdir: mdir,
		log:logging.MustGetLogger(dbkey),
		dbkey:dbkey,
	}

}

func MergeClose( dbdir *mfs.Directory, batch *leveldb.Batch, logKey string ) error {

	mstorage := NewMFSStorage(dbdir, logKey)

	db, err := leveldb.Open(mstorage, nil)
	if err != nil {
		return err
	}
	defer func() {

		if err := db.Close(); err != nil {
			log.Error(err)
		}

	}()

	if err := db.Write(batch, &opt.WriteOptions{Sync:true}); err != nil {
		return err
	}

	return nil
}

func ReadClose( dbdir *mfs.Directory, rf ReadingFunc, logKey string ) error {

	mstorage := NewMFSStorage(dbdir, logKey)

	db, err := leveldb.Open(mstorage, &opt.Options{ReadOnly:true})
	if err != nil {
		return err
	}
	defer func() {

		if err := db.Close(); err != nil {
			log.Error(err)
		}

	}()

	return rf(db)
}

func (ms *MFSStorage) Log(str string) {
	ms.log.Info(str)
}

func (ms *MFSStorage) SetMeta(fd storage.FileDesc) error {

	content := fsGenName(fd) + "\n"

	if !storage.FileDescOk(fd) {
		ms.log.Error(storage.ErrInvalidFile)
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	_ = ms.mdir.Unlink(mfsMetaFilePath)

	nnd := dag.NodeWithData( ft.FilePBData([]byte(content), 0) )
	nnd.SetCidBuilder(ms.mdir.GetCidBuilder())

	if err := ms.mdir.AddChild( mfsMetaFilePath, nnd ); err != nil {
		ms.log.Error(err)
		return err
	}

	if err := ms.mdir.Flush(); err != nil {
		ms.log.Error(err)
		return err
	}

	return nil
}

func (ms *MFSStorage) GetMeta() (storage.FileDesc, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	mnd, err := ms.mdir.Child( mfsMetaFilePath )
	if err != nil {
		ms.log.Info(os.ErrNotExist)
		return storage.FileDesc{}, os.ErrNotExist
	}

	fi, ok := mnd.(*mfs.File)
	if !ok {
		ms.log.Error(os.ErrNotExist)
		return storage.FileDesc{}, os.ErrNotExist
	}

	rd, err := fi.Open(mfs.Flags{Read:true, Sync:false})
	if err != nil {
		ms.log.Error(os.ErrNotExist)
		return storage.FileDesc{}, os.ErrNotExist
	}
	defer rd.Close()

	bs, err := ioutil.ReadAll(rd)
	if err != nil {
		ms.log.Error(os.ErrNotExist)
		return storage.FileDesc{}, os.ErrNotExist
	}

	fd, ok := fsParseName(string(bs))
	if !ok {
		ms.log.Error(os.ErrNotExist)
		return storage.FileDesc{}, os.ErrNotExist
	}

	return fd, nil
}

func (ms *MFSStorage) List(ft storage.FileType) ([]storage.FileDesc, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()

	var fds []storage.FileDesc

	lnames, err := ms.mdir.ListNames(context.TODO())
	if err != nil {
		ms.log.Error(err)
		return fds, err
	}

	for _, v := range lnames {

		if fd, ok := fsParseName(v); ok && fd.Type & ft != 0 {
			fds = append(fds, fd)
		}

	}

	return fds, nil
}

func (ms *MFSStorage) Open(fd storage.FileDesc) (storage.Reader, error) {

	if !storage.FileDescOk(fd) {
		ms.log.Error(storage.ErrInvalidFile)
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if m, err := ms.mdir.Child( fsGenName(fd) ); err != nil {
		ms.log.Error(os.ErrNotExist)
		return nil, os.ErrNotExist

	} else {

		if fi, ok := m.(*mfs.File); !ok {
			ms.log.Error(os.ErrNotExist)
			return nil, os.ErrNotExist

		} else {

			fwt, err := fi.Open(mfs.Flags{Read:true, Sync:false})

			if err != nil {
				ms.log.Error(err)
				return nil, err
			}

			fwrap := &mfsfileWrap{
				FileDescriptor:fwt,
				ms:ms,
				fd:fd,
				closed:false,
			}
			ms.openedWarp.Store(fd, fwrap)

			return fwrap, nil
		}
	}
}

func (ms *MFSStorage) Create(fd storage.FileDesc) (storage.Writer, error) {

	if !storage.FileDescOk(fd) {
		ms.log.Error(os.ErrNotExist)
		return nil, storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fname := fsGenName(fd)
	nd, err := ms.mdir.Child(fname)

	if err != nil {

		//file not exist
		nnd := dag.NodeWithData(ft.FilePBData(nil, 0))
		nnd.SetCidBuilder(ms.mdir.GetCidBuilder())

		if err := ms.mdir.AddChild( fname, nnd ); err != nil {
			ms.log.Error(storage.ErrClosed)
			return nil, storage.ErrClosed
		}

		nd, err = ms.mdir.Child(fname)
		if err != nil {
			ms.log.Error(storage.ErrClosed)
			return nil, storage.ErrClosed
		}

	}

	fi, ok := nd.(*mfs.File)
	if !ok {
		return nil, errors.New("expected *mfs.File, didnt get it. This is likely a race condition")
	}

	fwt, err := fi.Open(mfs.Flags{Write:true, Sync:false})
	if err != nil {
		ms.log.Error(err)
		return nil, err
	}

	if err := fwt.Truncate(0); err != nil {
		ms.log.Error(err)
		return nil, storage.ErrClosed
	}

	fwrap := &mfsfileWrap{
		FileDescriptor:fwt,
		ms:ms,
		fd:fd,
		closed:false,
	}
	ms.openedWarp.Store(fd, fwrap)

	return fwrap, nil
}

func (ms *MFSStorage) Remove(fd storage.FileDesc) error {

	if !storage.FileDescOk(fd) {
		ms.log.Error(storage.ErrInvalidFile)
		return storage.ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	fname := fsGenName(fd)
	err := ms.mdir.Unlink(fname)
	if err != nil {
		ms.log.Error(os.ErrClosed)
		return os.ErrClosed
	}

	return nil
}

func (ms *MFSStorage) Rename(oldfd, newfd storage.FileDesc) error {

	if !storage.FileDescOk(oldfd) || !storage.FileDescOk(newfd) {
		ms.log.Error(storage.ErrInvalidFile)
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
		ms.log.Error(os.ErrNotExist)
		return os.ErrNotExist
	}

	nd, err := srcObj.GetNode()
	if err != nil {
		ms.log.Error(os.ErrNotExist)
		return os.ErrNotExist
	}

	err = ms.mdir.AddChild(newName, nd)
	if err != nil {
		ms.log.Error(os.ErrClosed)
		return os.ErrClosed
	}

	return ms.mdir.Unlink(oldName)
}

func (ms *MFSStorage) Close() error {

	ms.openedWarp.Range(func(key, value interface{}) bool {

		fw, ok := value.(*mfsfileWrap)

		ms.log.Warning("before close db :" + fsGenName(fw.fd) + " file can't close")

		if !ok {
			return false
		}

		if err := fw.Close(); err != nil {
			ms.log.Error(err)
			return false
		}

		ms.openedWarp.Delete(key)

		return true

	})

	return nil
}

func (ms *MFSStorage) Flush() error {

	var err error

	ms.openedWarp.Range(func(key, value interface{}) bool {

		fw, ok := value.(*mfsfileWrap)

		if !ok {
			return false
		}

		if err = fw.Sync(); err != nil {
			return false
		}

		return true

	})

	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
