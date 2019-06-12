package go_aya_alvm_adb

import (
	"github.com/ipfs/go-mfs"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type mfsFileWrite struct {
	mfs.FileDescriptor
	storage.Syncer
}

func (fw *mfsFileWrite) Sync() error {
	return fw.FileDescriptor.Flush()
}