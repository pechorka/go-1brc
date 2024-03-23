package main

import (
	"bufio"
	"bytes"
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

	stationStats := make(map[string]stats)
	stationNames := make([]string, 0)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		stationBytes, tempBytes, ok := bytes.Cut(line, []byte(`;`))
		if !ok {
			continue
		}

		temp := bytesToFloat(tempBytes)

		station := string(stationBytes)

		s, ok := stationStats[station]
		if !ok {
			s.min = temp
			s.max = temp
			stationNames = append(stationNames, station)
		}
		if temp < s.min {
			s.min = temp
		}
		if temp > s.max {
			s.max = temp
		}
		s.sum += temp
		s.count++
		stationStats[station] = s
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
		s := stationStats[station]
		mean := float64(s.sum) / float64(10) / float64(s.count)
		_, err := fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(s.min)/float64(10), mean, float64(s.max)/float64(10))
		if err != nil {
			return errs.Wrap(err, "failed to write to output file")
		}
	}
	fmt.Fprintf(output, "}\n")

	return nil
}

func bytesToFloat(b []byte) int64 {
	result := int64(0)
	i := 0
	isNegative := false
	if b[0] == '-' {
		isNegative = true
		i++
	}
	for ; i < len(b); i++ {
		if b[i] == '.' {
			continue
		}
		result = result*10 + int64(b[i]-'0')
	}

	if isNegative {
		return -result
	}

	return result
}
