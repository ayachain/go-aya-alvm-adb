package go_aya_alvm_adb

import (
	"github.com/ipfs/go-mfs"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"io"
)

type mfsfileWrap struct {

	io.ReaderAt

	mfs.FileDescriptor

	ms *mfsStorage
	fd storage.FileDesc
	closed bool
}


func (mf *mfsfileWrap) ReadAt(p []byte, off int64) (int, error) {

	oseek, err := mf.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	defer mf.Seek(oseek, 0)

	_, err = mf.Seek(off, 0)
	if err != nil {
		return 0, err
	}

	return mf.Read(p)
}

func (fw *mfsfileWrap) Sync() error {

	if err := fw.FileDescriptor.Flush(); err != nil {
		return err
	}

	if fw.fd.Type == storage.TypeManifest {

		if err := fw.ms.mdir.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (fw *mfsfileWrap) Close() error {

	fw.ms.mu.Lock()
	defer fw.ms.mu.Unlock()

	if fw.closed {
		return storage.ErrClosed
	}

	if err := fw.Sync(); err != nil {
		return err
	}

	fw.closed = true

	delete( fw.ms.openedWarp, fw.fd )

	err := fw.FileDescriptor.Close()

	return err
}