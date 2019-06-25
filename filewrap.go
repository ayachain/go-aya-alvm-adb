package go_aya_alvm_adb

import (
	"github.com/ipfs/go-mfs"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"io"
)

type mfsfileWrap struct {

	io.ReaderAt

	mfs.FileDescriptor

	ms *MFSStorage

	fd storage.FileDesc

	closed bool
}


func (fw *mfsfileWrap) ReadAt(p []byte, off int64) (int, error) {

	fw.ms.mu.Lock()
	defer fw.ms.mu.Unlock()

	_, err := fw.Seek(off, io.SeekStart)
	if err != nil {
		fw.ms.log.Errorf("ADB FileWrap ReadAt ERR:%v\n", err)
	}

	return fw.Read(p)
}

func (fw *mfsfileWrap) Sync() error {

	fw.ms.mu.Lock()
	defer fw.ms.mu.Unlock()

	if fw.closed {
		fw.ms.log.Warning(storage.ErrClosed)
		return nil
	}

	if err := fw.FileDescriptor.Flush(); err != nil {
		fw.ms.log.Warning(err)
		//return nil
	}

	if fw.fd.Type == storage.TypeManifest {

		if err := fw.ms.mdir.Flush(); err != nil {
			fw.ms.log.Error(err)
			return err
		}
	}

	return nil
}

func (fw *mfsfileWrap) Close() error {

	fw.ms.mu.Lock()
	defer fw.ms.mu.Unlock()

	if fw.closed {
		fw.ms.log.Error(storage.ErrClosed)
		return storage.ErrClosed
	}

	if err := fw.FileDescriptor.Flush(); err != nil {
		fw.ms.log.Error(err)
		return err
	}

	if err := fw.FileDescriptor.Close(); err != nil {
		fw.ms.log.Error(err)
		return err
	}

	fw.closed = true
	fw.ms.openedWarp.Delete(fw.fd)

	return nil
}