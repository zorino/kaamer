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

package main

// code based on https://scene-si.org/2018/08/06/basic-monitoring-of-go-apps-with-the-runtime-package/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type Monitor struct {
	Alloc,
	TotalAlloc,
	Sys,
	Mallocs,
	Frees,
	LiveObjects,
	PauseTotalNs uint64

	NumGC        uint32
	NumGoroutine int
}

func NewMonitor(duration int, stop *bool, wg *sync.WaitGroup) {
	var m Monitor
	var rtm runtime.MemStats
	var interval = time.Duration(duration) * time.Second
	var fileOutput, err = os.Create("./monitor.out")
	var first = false

	fileOutput.WriteString("[\n")
	if err != nil {
		log.Fatal(err.Error())
	}
	for {
		<-time.After(interval)

		// Read full mem stats
		runtime.ReadMemStats(&rtm)

		// Number of goroutines
		m.NumGoroutine = runtime.NumGoroutine()

		// Misc memory stats
		m.Alloc = rtm.Alloc
		m.TotalAlloc = rtm.TotalAlloc
		m.Sys = rtm.Sys
		m.Mallocs = rtm.Mallocs
		m.Frees = rtm.Frees

		// Live objects = Mallocs - Frees
		m.LiveObjects = m.Mallocs - m.Frees

		// GC Stats
		m.PauseTotalNs = rtm.PauseTotalNs
		m.NumGC = rtm.NumGC

		// Just encode to json and print
		b, _ := json.Marshal(m)

		if !first {
			first = true
		} else {
			fileOutput.WriteString(",\n")
		}

		fileOutput.WriteString(string(b))

		if *stop {
			fileOutput.WriteString("\n]")
			fileOutput.Close()

			var monRes []Monitor
			jsonFile, err := os.Open("./monitor.out")
			// if we os.Open returns an error then handle it
			if err != nil {
				log.Fatal(err.Error())
			}
			byteValue, _ := ioutil.ReadAll(jsonFile)
			json.Unmarshal([]byte(byteValue), &monRes)

			var maxAlloc = uint64(0)
			for _, r := range monRes {
				if r.Alloc > maxAlloc {
					maxAlloc = r.Alloc
				}
			}
			fmt.Printf("MaxAlloc: %fGB\n", float64(maxAlloc)/1000000000)
			wg.Done()

			return
		}
	}
}
