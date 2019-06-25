package go_aya_alvm_adb

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

func fsGenName(fd storage.FileDesc) string {
	switch fd.Type {
	case storage.TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case storage.TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case storage.TypeTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case storage.TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		panic("invalid file type")
	}
}

func fsParseName(name string) (fd storage.FileDesc, ok bool) {
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &fd.Num, &tail)
	if err == nil {
		switch tail {
		case "log":
			fd.Type = storage.TypeJournal
		case "ldb", "sst":
			fd.Type = storage.TypeTable
		case "tmp":
			fd.Type = storage.TypeTemp
		default:
			return
		}
		return fd, true
	}

	n, _ := fmt.Sscanf(name, "MANIFEST-%d%s", &fd.Num, &tail)
	if n == 1 {
		fd.Type = storage.TypeManifest
		return fd, true
	}
	return
}

func fsParseNamePtr(name string, fd *storage.FileDesc) bool {
	_fd, ok := fsParseName(name)
	if fd != nil {
		*fd = _fd
	}
	return ok
}
