package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
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
	min   int64
	max   int64
	sum   int64
	count int64
}

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

	stationStats := make([]*stats, 300_000)
	stationNames := make([]string, 0, 500)
	scanner := bufio.NewScanner(f)
	const bufferSize = 1024 * 1024
	scanner.Buffer(make([]byte, bufferSize), bufferSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) < 3 { // last line is empty
			continue
		}
		// each city is at least 3 characters long, so semicolon is at least at index 3
		semicolonIndex := 3
		for ; semicolonIndex < len(line) && line[semicolonIndex] != ';'; semicolonIndex++ {
		}

		temp := bytesToFloat(line[semicolonIndex+1:])

		key := intHash(line[:semicolonIndex]) % len(stationStats)

		s := stationStats[key]
		if s == nil {
			s = &stats{
				min: temp,
				max: temp,
			}
			stationStats[key] = s
			stationNames = append(stationNames, string(line[:semicolonIndex]))
		}
		if temp < s.min {
			s.min = temp
		}
		if temp > s.max {
			s.max = temp
		}
		s.sum += temp
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
		s := stationStats[intHash([]byte(station))%len(stationStats)]
		mean := float64(s.sum) / float64(10) / float64(s.count)
		_, err := fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(s.min)/float64(10), mean, float64(s.max)/float64(10))
		if err != nil {
			return errs.Wrap(err, "failed to write to output file")
		}
	}
	fmt.Fprintf(output, "}\n")

	return nil
}

func intHash(b []byte) int {
	h := fnv.New32a()
	h.Write(b)
	return int(h.Sum32())
}

func bytesToFloat(b []byte) int64 {
	result := int64(0)
	i := 0
	sign := int64(1)
	if b[0] == '-' {
		sign = -1
		i++
	}
	for ; i < len(b)-2; i++ {
		result = result*10 + int64(b[i]-'0')
	}
	result = result*10 + int64(b[i+1]-'0')

	return result * sign
}
