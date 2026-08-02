// Command appendstore-downgrade writes an older log from an appendstore
// database. Use it to go back to a build that predates log format version 5.
//
// Usage:
//
//	appendstore-downgrade [-to version] [-f] source destination
//
// The tool reads the live records of the source log and writes them to the
// destination in format version 3 or version 4. It writes nothing when a record
// holds data the target format cannot express.
//
// The tool refuses a destination that already exists. Use -f to replace one.
//
// The tool reads the source. It writes to the source only to discard an
// incomplete trailing record from an interrupted write, which is the same
// repair that opening the store performs.
//
// The tool is removed when support for versions 1 to 4 ends. See
// docs/stable-format.md.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sirrobot01/appendstore"
)

func main() {
	version := flag.Uint("to", 4, "target log format version (3 or 4)")
	overwrite := flag.Bool("f", false, "replace the destination when it already exists")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [-to version] [-f] source destination\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(2)
	}
	source, destination := flag.Arg(0), flag.Arg(1)
	options := appendstore.DowngradeOptions{Version: uint32(*version), Overwrite: *overwrite}
	if err := appendstore.Downgrade(source, destination, options); err != nil {
		fmt.Fprintf(os.Stderr, "appendstore-downgrade: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s in log format version %d\n", destination, *version)
}
