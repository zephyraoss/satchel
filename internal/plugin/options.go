package plugin

import (
	"fmt"
	"time"
)

type VolumeOptions struct {
	Mode         string        `json:"mode,omitempty"`
	Scope        string        `json:"scope,omitempty"`
	Seed         string        `json:"seed,omitempty"`
	SyncInterval time.Duration `json:"sync_interval,omitempty"`
	Class        string        `json:"class,omitempty"`
}

func (o VolumeOptions) ReadOnly() bool   { return o.Mode == "ro" }
func (o VolumeOptions) PerReplica() bool { return o.Scope == "replica" }

func ParseVolumeOptions(raw map[string]string) (VolumeOptions, error) {
	opts := VolumeOptions{Mode: "rw", Scope: "volume", Class: "fuse"}
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
		case "seed":
			opts.Seed = value
		case "sync_interval":
			d, err := time.ParseDuration(value)
			if err != nil || d <= 0 {
				return opts, fmt.Errorf("sync_interval must be a positive duration, got %q", value)
			}
			opts.SyncInterval = d
		case "class":
			if value != "fuse" {
				return opts, fmt.Errorf("class must be fuse, got %q", value)
			}
			opts.Class = value
		default:
			return opts, fmt.Errorf("unknown option %q", key)
		}
	}
	if opts.ReadOnly() && opts.Seed != "" {
		return opts, fmt.Errorf("seed cannot be combined with mode=ro")
	}
	return opts, nil
}
