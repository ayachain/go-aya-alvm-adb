package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {

	print("Is Running.")

	mfsStorage := NewMFSStorage()

	db, err := leveldb.Open(mfsStorage, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.Put([]byte("key1"), []byte("value1"), nil)
	db.Put([]byte("key2"), []byte("value2"), nil)
	db.Put([]byte("key3"), []byte("value3"), nil)
	db.Put([]byte("key4"), []byte("value4"), nil)
	db.Put([]byte("key5"), []byte("value5"), nil)

	//循环遍历数据
	fmt.Println("循环遍历数据")
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("key:%s, value:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
}