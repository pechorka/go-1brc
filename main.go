package main

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
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
	min   float64
	max   float64
	sum   float64
	count float64
}

func run() error {
	f, err := os.Open("measurements.txt")
	if err != nil {
		return errs.Wrap(err, "failed to open file")
	}
	defer f.Close()

	stationStats := make(map[string]stats)
	stationNames := make([]string, 0)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, ok := strings.Cut(line, ";")
		if !ok {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return errs.Wrap(err, "failed to parse temperature")
		}

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
		mean := s.sum / s.count
		_, err := fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
		if err != nil {
			return errs.Wrap(err, "failed to write to output file")
		}
	}
	fmt.Fprintf(output, "}\n")

	return nil
}
