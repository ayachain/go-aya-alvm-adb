package go_aya_alvm_adb

import (
	"github.com/ipfs/go-mfs"
)

type mfsFileReader struct {
	mfs.FileDescriptor
}