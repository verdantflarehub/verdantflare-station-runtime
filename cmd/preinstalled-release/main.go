package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/verdantflarehub/verdantflare-station-runtime/internal/releasebuilder"
)

func main() {
	var in releasebuilder.Options
	var checkTargets bool
	flag.BoolVar(&checkTargets, "check-targets", false, "read-only target validation; no signing key or output directory")
	flag.StringVar(&in.Root, "root", "", "controlled material root")
	flag.StringVar(&in.Targets, "targets", "", "Runtime target registry relative to root")
	flag.StringVar(&in.Config, "config", "", "release preparation config relative to root")
	flag.StringVar(&in.Output, "out", "", "new output directory; existing content is never overwritten")
	flag.StringVar(&in.SigningKeyEnv, "signing-key-env", "STATION_PREINSTALLED_SIGNING_KEY", "controlled environment variable containing the existing base64 Ed25519 private key")
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
	out, err := releasebuilder.Build(in, time.Now().UTC(), os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(out) != nil {
		os.Exit(1)
	}
}
