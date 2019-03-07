package kvstore

import (
	"github.com/dgraph-io/badger"
	"strings"
	"strconv"
	"fmt"
	"encoding/binary"
	// "encoding/hex"
)

// Kmer Entries
type K_ struct {
	*KVStore
	aaTable     map[[2]rune]uint32
	aaBinTable  map[uint32][2]rune
}

func K_New(opts badger.Options, flushSize int, nbOfThreads int) *K_ {
	var k K_
	k.KVStore = new(KVStore)
	k.aaTable, k.aaBinTable = NewAATable()
	NewKVStore(k.KVStore, opts, flushSize, nbOfThreads)
	return &k
}

func NewAATable() (map[[2]rune]uint32, map[uint32][2]rune) {

	aa := []rune{'A','C','D','E','F','G','H','I','K','L','M','N','P','Q','R','S','T','U','V','W','Y'}

	aaTable := make(map[[2]rune]uint32)
	aaBinTable := make(map[uint32][2]rune)

	i := uint32(22)

	for j, a := range aa {
		aaBin := uint32(j)
		_key := [2]rune{a,'.'}
		aaTable[_key] = aaBin
		aaBinTable[aaBin] = _key
		for _, b := range aa {
			aaBin := i
			__key:= [2]rune{a,b}
			aaTable[__key] = aaBin
			aaBinTable[aaBin] = __key
			i++
		}
	}

	return aaTable, aaBinTable

}

func (k *K_) CreateBytesKey (kmer string) []byte {
	// expect kmers of length 7
	kmerInt := k.EncodeKmer(kmer)
	byteArrayKmer := make([]byte, 4)
	binary.BigEndian.PutUint32(byteArrayKmer, kmerInt)

	// kmerString := fmt.Sprintf("%x", kmerInt)
	// kmerHex := hex.EncodeToString(byteArrayKmer)

	return byteArrayKmer
}

func (k *K_) CreateBytesKey64Bit (kmer string) []byte {

	kmerBits := []int{1,1,1,1,1}

	for _, rune := range kmer {
		kmerBits = append(kmerBits, GetAminoAcidBits(rune)...)
	}
	kmerBitsString := strings.Trim(strings.Replace(fmt.Sprint(kmerBits), " ", "", -1), "[]")

	// to decode : strconv.FormatInt(v, 2)
	kmerInt64, _ := strconv.ParseInt(kmerBitsString, 2, 64)

	// to decode :
	// i := int64(binary.LittleEndian.Uint64(byteArrayKmer))
	// fmt.Println(i)
	byteArrayKmer := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteArrayKmer, uint64(kmerInt64))

	// fmt.Println(byteArrayKmer)

	return byteArrayKmer

}

// expect kmers of length 7
func (k *K_) EncodeKmer(kmer string) uint32 {

	// fmt.Println("#Encoding")

	kmerInt := uint32(0)
	i := 0
	shiftIndex := uint8(1)

	// aa pairs
	for (i+2) < len(kmer) {
		// fmt.Printf("%s => %x\n", kmer[i:i+2], aaTable[kmer[i:i+2]])
		_key := [2]rune{rune(kmer[i]), rune(kmer[i+1])}
		kmerInt |= k.aaTable[_key] << (32-(shiftIndex*9))
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


func GetAminoAcidBits (aminoAcid rune) []int {

	switch aminoAcid {
	case 'A', 'a':
		return []int{0, 0, 0, 0, 0}
	case 'C', 'c':
		return []int{0, 0, 0, 0, 1}
	case 'D', 'd':
		return []int{0, 0, 0, 1, 0}
	case 'E', 'e':
		return []int{0, 0, 0, 1, 1}
	case 'F', 'f':
		return []int{0, 0, 1, 0, 0}
	case 'G', 'g':
		return []int{0, 0, 1, 0, 1}
	case 'H', 'h':
		return []int{0, 0, 1, 1, 0}
	case 'I', 'i':
		return []int{0, 0, 1, 1, 1}
	case 'K', 'k':
		return []int{0, 1, 0, 0, 0}
	case 'L', 'l':
		return []int{0, 1, 0, 0, 1}
	case 'M', 'm':
		return []int{0, 1, 0, 1, 0}
	case 'N', 'n':
		return []int{0, 1, 0, 1, 1}
	case 'P', 'p':
		return []int{0, 1, 1, 0, 0}
	case 'Q', 'q':
		return []int{0, 1, 1, 0, 1}
	case 'R', 'r':
		return []int{0, 1, 1, 1, 1}
	case 'S', 's':
		return []int{1, 0, 0, 0, 0}
	case 'T', 't':
		return []int{1, 0, 0, 0, 1}
	case 'U', 'u':
		return []int{1, 0, 0, 1, 0}
	case 'V', 'v':
		return []int{1, 0, 0, 1, 1}
	case 'W', 'w':
		return []int{1, 0, 1, 0, 0}
	case 'Y', 'y':
		return []int{1, 0, 1, 0, 1}
	default:
		return []int{0, 0, 0, 0, 0}
	}

}
