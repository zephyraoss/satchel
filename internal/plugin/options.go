package plugin

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const DefaultVolumeSize = int64(10 << 30)

type VolumeOptions struct {
	Mode         string        `json:"mode,omitempty"`
	Scope        string        `json:"scope,omitempty"`
	Durability   string        `json:"durability,omitempty"`
	Seed         string        `json:"seed,omitempty"`
	SyncInterval time.Duration `json:"sync_interval,omitempty"`
	Size         int64         `json:"size"`
	Filesystem   string        `json:"filesystem"`
}

func (o VolumeOptions) ReadOnly() bool   { return o.Mode == "ro" }
func (o VolumeOptions) PerReplica() bool { return o.Scope == "replica" }
func (o VolumeOptions) RemoteDurability() bool {
	return o.Durability != "async"
}

func ParseVolumeOptions(raw map[string]string) (VolumeOptions, error) {
	opts := VolumeOptions{Mode: "rw", Scope: "volume", Durability: "remote", Size: DefaultVolumeSize, Filesystem: "ext4"}
	for key, value := range raw {
		switch key {
		case "mode":
			if value != "rw" && value != "ro" {
				return opts, fmt.Errorf("mode must be rw or ro, got %q", value)
			}
			opts.Mode = value
		case "scope":
			if value != "volume" && value != "replica" {
				return opts, fmt.Errorf("scope must be volume or replica, got %q", value)
			}
			opts.Scope = value
		case "durability":
			if value != "remote" && value != "async" {
				return opts, fmt.Errorf("durability must be remote or async, got %q", value)
			}
			opts.Durability = value
		case "seed":
			opts.Seed = value
		case "sync_interval":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return opts, fmt.Errorf("sync_interval must be a positive duration, got %q", value)
			}
			opts.SyncInterval = d
		case "size":
			size, err := parseSize(value)
			if err != nil || size < 64<<20 || size%4096 != 0 {
				return opts, fmt.Errorf("size must be at least 64MiB and aligned to 4KiB, got %q", value)
			}
			opts.Size = size
		case "filesystem":
			if value != "ext4" {
				return opts, fmt.Errorf("filesystem must be ext4, got %q", value)
			}
			opts.Filesystem = value
		default:
			return opts, fmt.Errorf("unknown option %q", key)
		}
	}
	if opts.ReadOnly() && opts.Seed != "" {
		return opts, fmt.Errorf("seed cannot be combined with mode=ro")
	}
	return opts, nil
}

func parseSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	units := []struct {
		suffix string
		factor int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1},
	}
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(value, unit.suffix)), 10, 64)
			if err != nil || n <= 0 || n > (1<<63-1)/unit.factor {
				return 0, fmt.Errorf("invalid size %q", value)
			}
			return n * unit.factor, nil
		}
	}
	return strconv.ParseInt(value, 10, 64)
}
