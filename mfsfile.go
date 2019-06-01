package go_aya_alvm_adb

import (
	"github.com/ipfs/go-mfs"
	"io"
)

type mfsFile struct {
	mfs.FileDescriptor
	io.ReaderAt
}

func (mf *mfsFile) ReadAt(p []byte, off int64) (int, error) {

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