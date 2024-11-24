package main

import (
	"os"
	"stock_exchange/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		os.Exit(0)
	}
}
