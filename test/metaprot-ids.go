package main

import (
	"fmt"
	"hash/fnv"

	hashids "github.com/speps/go-hashids"
	// "bufio"
	// "os"
	// "log"
	// "strconv"
)

func main() {

	// file, err := os.Open("uniprotkb-ids.txt")
	// if err != nil {
	//	log.Fatal(err)
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	//	intId := hash(scanner.Text())
	//	fmt.Println(intId)
	//	// fmt.Println(scanner.Text())
	// }

	// if err := scanner.Err(); err != nil {
	//	log.Fatal(err)
	// }

	hd := hashids.NewData()
	hd.Salt = "this is my salt"
	h, _ := hashids.NewWithData(hd)
	id, _ := h.EncodeInt64([]int64{9223371920837590526, 9223371990495367099, 9223371990495367091, 9223371990495367092, 9223371990495367093, 9223371990495367094, 9223371990495367095})
	numbers, _ := h.DecodeWithError(id)
	fmt.Println(id)
	fmt.Println(numbers[0])

	// A0A388PK68_9ACTN
	// QDjyxkZMvRJrzUaDvqJZ5P57wk

}

func hash(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	hashInt := h.Sum64()
	if hashInt > 9223372036854775807 {
		hashInt = (hashInt - 9223372036854775807)
	}
	return int64(hashInt)
}

// OqrNkQnwvRxyzIpNMgPXQ8QK5ECaMmxen878JvkT5m1LgO2w2MND
// 52*8
// BkMoVb7w8plm3UNY3Ezke4eBOZF9wOZ6N1r1ymxiJR56V9gKgZ2yhye6nqJkEk13rsrWY8jy7J7zk5fXgVLxroMoRjE
