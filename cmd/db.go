package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gitlab.com/greyxor/slogor"
)

func main() {
	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.TimeOnly), slogor.ShowSource())))
	slog.Info("I'm an information message, everything's fine")
	slog.Warn("I'm a warning, that's ok.")
	slog.Error("I'm an error message, this is serious...")
	slog.Debug("Useful debug message.")
	slog.Error("Error with args", slogor.Err(errors.New("i'm an error")))
	slog.Warn("Warn with args", slog.Int("the_answer", 42))
	slog.Debug("Debug with args", slog.String("a_string", "🐛"))

	fmt.Println("")

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.Kitchen))))
	slog.Info("Example with kitchen time.")
	slog.Debug("Example with kitchen time.")

	fmt.Println("")

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.RFC3339Nano), slogor.ShowSource())))
	slog.Info("Example with RFC 3339 time and source path")

	fmt.Println("")

	slog.SetDefault(slog.New(slogor.NewHandler(os.Stderr, slogor.SetLevel(slog.LevelDebug), slogor.SetTimeFormat(time.Stamp))))
	slog.Error("Error with args", slogor.Err(errors.New("i'm an error")))
	slog.Warn("Warn with args", slog.Int("the_answer", 42))
	slog.Debug("Debug with args", slog.String("a_string", "🐛"))

	// tm, err := tmfsync.New(tmfsync.DOME_PRO)
	// if err != nil {
	// 	panic(err)
	// }

	// pos, _, err := tm.RetrieveLocalListTMFObject(nil, "productOfferingPrice", "", -1, -1)
	// if err != nil {
	// 	panic(err)
	// }

	// for i, k := range pos {
	// 	fmt.Println(i, k)
	// }

}
