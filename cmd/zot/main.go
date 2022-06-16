package main

import (
	"os"

	"zotregistry.io/zot/pkg/cli"
)

func main() {
	if err := cli.NewServerRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
