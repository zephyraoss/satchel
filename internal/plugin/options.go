package plugin

import (
	"fmt"
	"github.com/zephyraoss/satchel/internal/units"
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
			size, err := units.ParseBytes(value)
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
