package main

import (
	"fmt"
	"os"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/shell"
)

func main() {
	dbName := ":memory:"
	if len(os.Args) > 1 {
		dbName = os.Args[1]
	}
	conn, err := sqlite.OpenConn(dbName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	shell.Run(conn)
	conn.Close()
}
