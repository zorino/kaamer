package kvstore

import (
	"sort"
	"crypto/sha1"
	"encoding/hex"
	"strings"
)


func RemoveDuplicatesFromSlice(s []string) []string {

	m := make(map[string]bool)
	for _, item := range s {
		if _, ok := m[item]; ok {
			// duplicate item
			// fmt.Println(item, "is a duplicate")
		} else {
			m[item] = true
		}
	}

	var result []string
	for item, _ := range m {
		result = append(result, item)
	}

	return result

}

func CreateHashValue(ids []string) (string,string) {

	ids = RemoveDuplicatesFromSlice(ids)
	sort.Strings(ids)

	var idsString = strings.Join(ids, ",")

	h := sha1.New()
	h.Write([]byte(idsString))
	bs := h.Sum(nil)
	hashKey := hex.EncodeToString(bs)

	// combined key prefix = "_"
	hashKey = "_" + hashKey[len(hashKey)-11:len(hashKey)]

	return hashKey, idsString

}
