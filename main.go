package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
	"slices"
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

func run() error {
	filePath := "measurements-1k.txt"
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

	const stationStatsSize = 2 << 17
	stationStats := make([]*stats, stationStatsSize)
	stationNames := make([]string, 0, 500)
	scanner := bufio.NewScanner(f)
	const bufferSize = 1024 * 1024
	scanner.Buffer(make([]byte, bufferSize), bufferSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) < 3 { // last line is empty
			continue
		}

		semicolonIndex := 0
		hash := uint32(0)
		for ; semicolonIndex < len(line); semicolonIndex++ {
			if line[semicolonIndex] == ';' {
				break
			}
			hash = (hash ^ uint32(line[semicolonIndex])) * prime32
		}

		temp := bytesToFloat(line[semicolonIndex+1:])
		key := int(hash & uint32(stationStatsSize-1))

		s := stationStats[key]
		if s == nil {
			s = &stats{
				min:   temp,
				max:   temp,
				sum:   int32(temp),
				count: 1,
			}
			stationStats[key] = s
			stationNames = append(stationNames, string(line[:semicolonIndex]))
			continue
		}
		if temp < s.min {
			s.min = temp
		}
		if temp > s.max {
			s.max = temp
		}
		s.sum += int32(temp)
		s.count++
	}

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
		s := stationStats[int(hash&uint32(stationStatsSize-1))]
		mean := float64(s.sum) / float64(10) / float64(s.count)
		_, err := fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(s.min)/float64(10), mean, float64(s.max)/float64(10))
		if err != nil {
			return errs.Wrap(err, "failed to write to output file")
		}
	}
	fmt.Fprintf(output, "}\n")

	return nil
}

func bytesToFloat(b []byte) int16 {
	// b is either
	// 1) -n.n
	// 2) -nn.n
	// 3) n.n
	// 4) nn.n

	if b[0] == '-' {
		// -n.n
		if len(b) == 4 {
			return -(int16((b[1]-'0')*10 + (b[3] - '0')))
		}
		// -nn.n
		return -(int16(b[1]-'0')*100 + int16((b[2]-'0')*10+(b[4]-'0')))
	} else {
		// n.n
		if len(b) == 3 {
			return int16(b[0]-'0')*10 + int16(b[2]-'0')
		}
		// nn.n
		return int16(b[0]-'0')*100 + int16((b[1]-'0')*10+(b[3]-'0'))
	}
}
