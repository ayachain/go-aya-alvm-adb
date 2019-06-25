package go_aya_alvm_adb

import (
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type mfsStorageLock struct {
	ms *MFSStorage
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


func (ms *MFSStorage) Lock() (storage.Locker, error) {

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, storage.ErrLocked
	}
	ms.slock = &mfsStorageLock{ms: ms}
	return ms.slock, nil
}