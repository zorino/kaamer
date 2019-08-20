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
	"encoding/binary"
	"github.com/dgraph-io/badger"
)

// Kmer Entries
type K_ struct {
	*KVStore
	aaTable    map[[2]rune]uint32
	aaBinTable map[uint32][2]rune
}

func K_New(opts badger.Options, flushSize int, nbOfThreads int) *K_ {
	var k K_
	k.KVStore = new(KVStore)
	k.aaTable, k.aaBinTable = NewAATable()
	NewKVStore(k.KVStore, opts, flushSize, nbOfThreads)
	return &k
}

func NewAATable() (map[[2]rune]uint32, map[uint32][2]rune) {

	aa := []rune{'A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'Y'}

	aaTable := make(map[[2]rune]uint32)
	aaBinTable := make(map[uint32][2]rune)

	i := uint32(22)

	for j, a := range aa {
		aaBin := uint32(j)
		_key := [2]rune{a, '.'}
		aaTable[_key] = aaBin
		aaBinTable[aaBin] = _key
		for _, b := range aa {
			aaBin := i
			__key := [2]rune{a, b}
			aaTable[__key] = aaBin
			aaBinTable[aaBin] = __key
			i++
		}
	}

	return aaTable, aaBinTable

}

func (k *K_) CreateBytesKey(kmer string) []byte {
	// expect kmers of length 7
	kmerInt := k.EncodeKmer(kmer)
	byteArrayKmer := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArrayKmer, kmerInt)

	// kmerString := fmt.Sprintf("%x", kmerInt)
	// kmerHex := hex.EncodeToString(byteArrayKmer)

	return byteArrayKmer
}

func (k *K_) CreateBytesVal(entry string) []byte {
	// expect kmers of length 7
	entryInt := k.EncodeEntry(entry)
	byteArrayKmer := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArrayKmer, entryInt)

	// kmerString := fmt.Sprintf("%x", kmerInt)
	// kmerHex := hex.EncodeToString(byteArrayKmer)

	return byteArrayKmer
}


// expect kmers of length 7
func (k *K_) EncodeKmer(kmer string) uint32 {

	// fmt.Println("#Encoding")

	kmerInt := uint32(0)
	i := 0
	shiftIndex := uint8(1)

	// aa pairs
	for (i + 2) < len(kmer) {
		// fmt.Printf("%s => %x\n", kmer[i:i+2], aaTable[kmer[i:i+2]])
		_key := [2]rune{rune(kmer[i]), rune(kmer[i+1])}
		kmerInt |= k.aaTable[_key] << (32 - (shiftIndex * 9))
		shiftIndex++
		i += 2
	}

	// last aa
	_key := [2]rune{rune(kmer[len(kmer)-1]), '.'}
	kmerInt |= k.aaTable[_key]

	// fmt.Printf("%s => %x\n", kmer[len(kmer)-1:], aaTable[kmer[len(kmer)-1:]])
	// fmt.Println(kmerInt)

	return kmerInt

}

// expect kmers of length 7
func (k *K_) DecodeKmer(key []byte) string {

	kmerInt := binary.BigEndian.Uint32(key)
	aa := (kmerInt >> 23) & 0x1FF
	bb := (kmerInt >> 14) & 0x1FF
	cc := (kmerInt >> 5) & 0x1FF
	dd := (kmerInt) & 0x1F

	// fmt.Println("#Decoding")
	// fmt.Printf("%s => %x\n", aaBinTable[aa], aa)
	// fmt.Printf("%s => %x\n", aaBinTable[bb], bb)
	// fmt.Printf("%s => %x\n", aaBinTable[cc], cc)
	// fmt.Printf("%s => %x\n", aaBinTable[dd], dd)

	kmer := ""
	kmer += string(k.aaBinTable[aa][0])
	kmer += string(k.aaBinTable[aa][1])
	kmer += string(k.aaBinTable[bb][0])
	kmer += string(k.aaBinTable[bb][1])
	kmer += string(k.aaBinTable[cc][0])
	kmer += string(k.aaBinTable[cc][1])
	kmer += string(k.aaBinTable[dd][0])

	return kmer

}


func (k *K_) EncodeEntry(kmer string) uint32 {

	// fmt.Println("#Encoding")

	kmerInt := uint32(0)
	i := 0
	shiftIndex := uint8(1)

	// aa pairs
	for (i + 2) < len(kmer) {
		// fmt.Printf("%s => %x\n", kmer[i:i+2], aaTable[kmer[i:i+2]])
		_key := [2]rune{rune(kmer[i]), rune(kmer[i+1])}
		kmerInt |= k.aaTable[_key] << (32 - (shiftIndex * 9))
		shiftIndex++
		i += 2
	}

	// last aa
	_key := [2]rune{rune(kmer[len(kmer)-1]), '.'}
	kmerInt |= k.aaTable[_key]

	// fmt.Printf("%s => %x\n", kmer[len(kmer)-1:], aaTable[kmer[len(kmer)-1:]])
	// fmt.Println(kmerInt)

	return kmerInt

}
