package main

import (
	"fmt"
	"os"
	"github.com/willf/bloom"
	"encoding/binary"
)



func encodeKmer(kmer string, aaTable map[[2]rune]uint32) uint32 {

	// fmt.Println("#Encoding")

	kmerInt := uint32(0)
	i := 0
	shiftIndex := uint8(1)

	// aa pairs
	for (i+2) < len(kmer) {
		// fmt.Printf("%s => %x\n", kmer[i:i+2], aaTable[kmer[i:i+2]])
		_key := [2]rune{rune(kmer[i]), rune(kmer[i+1])}
		kmerInt |= aaTable[_key] << (32-(shiftIndex*9))
		shiftIndex++
		i += 2
	}

	// last aa
	_key := [2]rune{rune(kmer[len(kmer)-1]), '.'}
	kmerInt |= aaTable[_key]

	// fmt.Printf("%s => %x\n", kmer[len(kmer)-1:], aaTable[kmer[len(kmer)-1:]])
	// fmt.Println(kmerInt)

	return kmerInt

}


func decodeKmer(kmerInt uint32, aaBinTable map[uint32][2]rune) string {

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
	kmer += string(aaBinTable[aa][0])
	kmer += string(aaBinTable[aa][1])
	kmer += string(aaBinTable[bb][0])
	kmer += string(aaBinTable[bb][1])
	kmer += string(aaBinTable[cc][0])
	kmer += string(aaBinTable[cc][1])
	kmer += string(aaBinTable[dd][0])

	return kmer

}


func main() {

	// aa := []string{"A","C","D","E","F","G","H","I","K","L","M","N","P","Q","R","S","T","U","V","W","Y"}
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


	// testAllKmers(aaTable, aaBinTable)
	createBloomFilter(aaTable, aaBinTable)

}

func testAllKmers(aaTable map[[2]rune]uint32, aaBinTable map[uint32][2]rune) {

	aa := []rune{'A','C','D','E','F','G','H','I','K','L','M','N','P','Q','R','S','T','U','V','W','Y'}

	kmer := ""
	for _, a := range aa {
		for _, b := range aa {
			for _, c := range aa {
				for _, d := range aa {
					for _, e := range aa {
						for _, f := range aa {
							for _, g := range aa {
								kmer = ""
								kmer += string(a)
								kmer += string(b)
								kmer += string(c)
								kmer += string(d)
								kmer += string(e)
								kmer += string(f)
								kmer += string(g)

								kmerInt := encodeKmer(kmer, aaTable)
								kmerDecoded := decodeKmer(kmerInt, aaBinTable)

								if kmerDecoded != kmer {
									fmt.Println("Test: Kmers are equals - encode / decode failed")
									os.Exit(1)
								} else {
									fmt.Printf("%s\t%d\n", kmerDecoded, kmerInt)
								}
							}
						}
					}
				}
			}
		}
	}

}

func createBloomFilter(aaTable map[[2]rune]uint32, aaBinTable map[uint32][2]rune) {

	// aa := []rune{'A','C','D','E','F','G','H','I','K','L','M','N','P','Q','R','S','T','U','V','W','Y'}
	aa := []rune{'A','C','D'}

	bloomF := bloom.NewWithEstimates(1800000000, 0.01)

	kmer := ""
	for _, a := range aa {
		for _, b := range aa {
			for _, c := range aa {
				for _, d := range aa {
					for _, e := range aa {
						for _, f := range aa {
							for _, g := range aa {
								kmer = ""
								kmer += string(a)
								kmer += string(b)
								kmer += string(c)
								kmer += string(d)
								kmer += string(e)
								kmer += string(f)
								kmer += string(g)

								kmerInt := encodeKmer(kmer, aaTable)
								// kmerDecoded := decodeKmer(kmerInt, aaBinTable)

								byteArrayKmer := make([]byte, 4)
								binary.BigEndian.PutUint32(byteArrayKmer, kmerInt)
								bloomF.Add(byteArrayKmer)

							}
						}
					}
				}
			}
		}
	}

	fout, _ := os.Create("/tmp/bloom")
	byteWriten, err := bloomF.WriteTo(fout)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Wrote %d bytes\n",byteWriten)
	}

	fout.Close()

}
