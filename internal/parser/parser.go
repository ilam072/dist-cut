package parser

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type FieldSet struct {
	ranges [][2]int
}

func ParseFields(s string) (*FieldSet, error) {
	if s == "" {
		return nil, fmt.Errorf("empty field list")
	}
	parts := strings.Split(s, ",")
	fs := &FieldSet{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.Contains(p, "-") {
			r := strings.SplitN(p, "-", 2)
			start, err := strconv.Atoi(r[0])
			if err != nil {
				return nil, err
			}
			end := start
			if r[1] != "" {
				tmp, err := strconv.Atoi(r[1])
				if err != nil {
					return nil, err
				}
				end = tmp
			}
			if start < 1 || end < start {
				return nil, fmt.Errorf("bad range: %s", p)
			}
			fs.ranges = append(fs.ranges, [2]int{start, end})
		} else {
			v, err := strconv.Atoi(p)
			if err != nil {
				return nil, err
			}
			if v < 1 {
				return nil, fmt.Errorf("bad field number: %d", v)
			}
			fs.ranges = append(fs.ranges, [2]int{v, v})
		}
	}

	sort.Slice(fs.ranges, func(i, j int) bool { return fs.ranges[i][0] < fs.ranges[j][0] })

	merged := make([][2]int, 0, len(fs.ranges))
	for _, r := range fs.ranges {
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := merged[len(merged)-1]
		if r[0] <= last[1]+1 {
			if r[1] > last[1] {
				merged[len(merged)-1][1] = r[1]
			}
		} else {
			merged = append(merged, r)
		}
	}
	fs.ranges = merged
	return fs, nil
}

func (fs *FieldSet) SelectFields(fields []string) []string {
	out := make([]string, 0)
	for _, r := range fs.ranges {
		for i := r[0]; i <= r[1]; i++ {
			if i-1 < len(fields) {
				out = append(out, fields[i-1])
			}
		}
	}
	return out
}
