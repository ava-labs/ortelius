package main

import (
	"fmt"
	"os"

	"github.com/ava-labs/ortelius/client"
)

func main() {
	client := client.Client{}
	if len(os.Args) > 5 {
		client.Initialize(os.Args[4])
		isConsumer := false
		if os.Args[1] == "consumer" {
			isConsumer = true
		}
		client.Listen(isConsumer, os.Args[2], os.Args[3], os.Args[4], os.Args[5])
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "Usage: ortelius <producer | consumer> <topic> <URL> <dataType> <filter>\n")
	os.Exit(1)
}
