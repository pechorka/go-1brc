package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"slices"
	"sync"
	"time"

	"github.com/pechorka/stdlib/pkg/errs"
)

func main() {
	now := time.Now()
	if err := run(); err != nil {
		panic(err)
	}
	fmt.Println("Elapsed time:", time.Since(now))
}

type stats struct {
	min   int16
	max   int16
	sum   int32
	count int32
}

const (
	offset32 = 2166136261
	prime32  = 16777619
)

const stationStatsSize = 2 << 17

func run() error {
	filePath := "measurements-1b.txt"
	if len(os.Args) > 1 {
		filePath = os.Args[1]
	}
	f, err := os.Open(filePath)
	if err != nil {
		return errs.Wrap(err, "failed to open file")
	}

	if len(os.Args) > 2 {
		f, err := os.Create("cpu.prof")
		if err != nil {
			return errs.Wrap(err, "failed to create CPU profile")
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	allStationStats := make([][]*stats, 0, 30)

	wg := new(sync.WaitGroup)

	const bufferSize = 100 * 1024 * 1024
	remaining := make([]byte, 0, bufferSize)
	for {
		buf := make([]byte, bufferSize)
		emptyBufStart := copy(buf, remaining)
		n, err := f.Read(buf[emptyBufStart:])
		if err != nil && err != io.EOF {
			return errs.Wrap(err, "failed to read file")
		}
		if n+emptyBufStart == 0 {
			break
		}

		chunk := buf[:n+emptyBufStart]

		lastNewLine := bytes.LastIndexByte(chunk, '\n')
		if lastNewLine == -1 {
			break
		}

		remaining = chunk[lastNewLine+1:]

		wg.Add(1)
		chunk = chunk[:lastNewLine+1]
		stationStats := make([]*stats, stationStatsSize)
		allStationStats = append(allStationStats, stationStats)
		go processChunk(wg, chunk, stationStats)
	}

	wg.Wait()

	stationNames := make([]string, 0)
	stationNamesMap.Range(func(key, value interface{}) bool {
		stationNames = append(stationNames, key.(string))
		return true
	})

	slices.Sort(stationNames)

	output, err := os.Create("output.txt")
	if err != nil {
		return errs.Wrap(err, "failed to create output file")
	}
	defer output.Close()

	fmt.Fprintf(output, "{")
	for i, station := range stationNames {
		if i > 0 {
			fmt.Fprintf(output, ", ")
		}
		hash := uint32(0)
		for j := 0; j < len(station); j++ {
			hash = (hash ^ uint32(station[j])) * prime32
		}
		var mergedStats *stats
		for _, stationStats := range allStationStats {
			s := stationStats[int(hash&uint32(stationStatsSize-1))]
			if s != nil {
				if mergedStats == nil {
					mergedStats = s
					continue
				}

				if s.min < mergedStats.min {
					mergedStats.min = s.min
				}
				if s.max > mergedStats.max {
					mergedStats.max = s.max
				}
				mergedStats.sum += s.sum
				mergedStats.count += s.count
			}
		}

		mean := float64(mergedStats.sum) / float64(10) / float64(mergedStats.count)
		_, err := fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(mergedStats.min)/float64(10), mean, float64(mergedStats.max)/float64(10))
		if err != nil {
			return errs.Wrap(err, "failed to write to output file")
		}
	}
	fmt.Fprintf(output, "}\n")

	return nil
}

func processChunk(wg *sync.WaitGroup, chunk []byte, stationStats []*stats) {
	defer wg.Done()
	for {
		semicolonIndex := -1
		hash := uint32(0)
		for i := 0; i < len(chunk); i++ {
			if chunk[i] == ';' {
				semicolonIndex = i
				break
			}
			if semicolonIndex == -1 {
				hash = (hash ^ uint32(chunk[i])) * prime32
			}
		}
		if semicolonIndex == -1 {
			break
		}

		tempI := semicolonIndex + 1
		sign := int16(1)
		if chunk[tempI] == '-' {
			sign = -1
			tempI++
		}
		temp := int16(chunk[tempI] - '0')
		tempI++
		if chunk[tempI] != '.' {
			temp = temp*10 + int16(chunk[tempI]-'0')
			tempI++
		}
		tempI++ // skip dot
		temp = temp*10 + int16(chunk[tempI]-'0')
		temp *= sign

		tempI += 2 // skip decimal digit and \n

		key := int(hash & uint32(stationStatsSize-1))

		s := stationStats[key]
		if s == nil {
			s = &stats{
				min: temp,
				max: temp,
			}
			stationStats[key] = s
			stationNamesMap.Store(string(chunk[:semicolonIndex]), struct{}{})
		}
		if temp < s.min {
			s.min = temp
		}
		if temp > s.max {
			s.max = temp
		}
		s.sum += int32(temp)
		s.count++

		chunk = chunk[tempI:]
	}
}

var stationNamesMap *sync.Map = new(sync.Map)
