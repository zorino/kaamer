/*
Copyright 2019 The kaamer Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvstore

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"

	"github.com/OneOfOne/xxhash"
	"github.com/dgraph-io/badger/v3"
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

	val, hasKey := kc.GetValue(combKeyByte)

	if bytes.Compare(val, kCombPB) == 0 {
		return combKeyByte, nil
	}

	proceedWithKey := (hasKey == false)

	for !proceedWithKey {
		token := make([]byte, 4)
		rand.Read(token)
		h.Write(token)
		combKeyByte = make([]byte, 8)
		binary.BigEndian.PutUint64(combKeyByte, h.Sum64())
		_, hasKey := kc.GetValue(combKeyByte)
		proceedWithKey = !hasKey
	}

	return combKeyByte, kCombPB

}
