package main

import (
	"bytes"
	"fmt"
	"io"
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

	const stationStatsSize = 2 << 17
	stationStats := make([]*stats, stationStatsSize)
	stationNames := make([]string, 0, 500)

	const bufferSize = 1024 * 1024
	buf := make([]byte, bufferSize)
	emptyBufStart := 0
	for {
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

		remaining := chunk[lastNewLine+1:]
		chunk = chunk[:lastNewLine+1]

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
				stationNames = append(stationNames, string(chunk[:semicolonIndex]))
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

		emptyBufStart = copy(buf, remaining)
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
