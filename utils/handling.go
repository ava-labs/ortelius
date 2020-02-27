package utils

import (
	"fmt"
	"os"
)

// Die errors and exits
func Die(format string, v ...interface{}) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, v...))
	os.Exit(1)
}
