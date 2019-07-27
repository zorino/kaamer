package kvstore

import (
	"encoding/binary"
	"log"

	"github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
)

// Hash store for values combination used in other stores
type KC_ struct {
	*KVStore
}

func KC_New(opts badger.Options, flushSize int, nbOfThreads int) *KC_ {
	var kc KC_
	kc.KVStore = new(KVStore)
	NewKVStore(kc.KVStore, opts, flushSize, nbOfThreads)
	return &kc
}

func (kc *KC_) CreateKCKeyValue(keys [][]byte) ([]byte, []byte) {

	h := xxhash.New64()
	kComb := &KComb{}

	sortedKeys := RemoveDuplicatesFromSlice(keys)

	for _, k := range sortedKeys {
		intId := binary.BigEndian.Uint32(k)
		kComb.ProteinKeys = append(kComb.ProteinKeys, intId)
		h.Write(k)
	}
	kComb.Count = 0

	kCombPB, err := proto.Marshal(kComb)
	if err != nil {
		log.Fatal(err.Error())
	}

	combKeyByte := make([]byte, 8)
	binary.BigEndian.PutUint64(combKeyByte, h.Sum64())

	return combKeyByte, kCombPB

}
