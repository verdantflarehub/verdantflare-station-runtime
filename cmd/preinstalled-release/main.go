package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebuilder"
)

func main() {
	var in releasebuilder.Options
	var checkTargets bool
	flag.BoolVar(&checkTargets, "check-targets", false, "read-only target validation; no output directory")
	flag.StringVar(&in.Root, "root", "", "controlled material root")
	flag.StringVar(&in.Targets, "targets", "", "Runtime target registry relative to root")
	flag.StringVar(&in.Config, "config", "", "release preparation config relative to root")
	flag.StringVar(&in.Output, "out", "", "new output directory; existing content is never overwritten")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if checkTargets {
		if in.Config != "" || in.Output != "" {
			fmt.Fprintln(os.Stderr, "target inspection accepts only root and targets")
			os.Exit(2)
		}
		out, err := releasebuilder.InspectTargets(in.Root, in.Targets)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		if json.NewEncoder(os.Stdout).Encode(out) != nil {
			os.Exit(1)
		}
		return
	}
	out, err := releasebuilder.Build(in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(out) != nil {
		os.Exit(1)
	}
}
