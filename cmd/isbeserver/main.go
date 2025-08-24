package main

import (
	"flag" // Added
	"os"

	"log/slog"

	"github.com/gofiber/fiber/v2"
	echohandler "github.com/hesusruiz/domepdp/tmfserver/handler/echo"
	fiberhandler "github.com/hesusruiz/domepdp/tmfserver/handler/fiber"
	repository "github.com/hesusruiz/domepdp/tmfserver/repository"
	service "github.com/hesusruiz/domepdp/tmfserver/service"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	_ "github.com/mattn/go-sqlite3"
	"gitlab.com/greyxor/slogor"
)

func main() {
	// Configure slog logger
	var debugFlag bool
	flag.BoolVar(&debugFlag, "d", false, "Enable debug logging")
	flag.Parse()

	var logLevel slog.Level
	if debugFlag {
		logLevel = slog.LevelDebug
	} else {
		logLevel = slog.LevelInfo
	}

	handler := slogor.NewHandler(os.Stdout, slogor.SetLevel(logLevel))
	slog.SetDefault(slog.New(handler))

	// Connect to the database
	db, err := sqlx.Connect("sqlite3", "tmf.db")
	if err != nil {
		slog.Error("failed to connect to database", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()

	// Create the table if it doesn't exist
	_, err = db.Exec(repository.CreateTMFTableSQL)
	if err != nil {
		slog.Error("failed to create table", slog.Any("error", err))
		os.Exit(1)
	}

	// Create the service
	s := service.NewService(db)

	// Create and run the Fiber server
	go func() {
		app := fiber.New()
		h := fiberhandler.NewHandler(s)
		h.RegisterRoutes(app)
		slog.Info("Fiber server starting", slog.String("port", ":9991"))
		app.Listen(":9991")
	}()

	// Create and run the Echo server
	go func() {
		e := echo.New()
		h := echohandler.NewHandler(s)
		h.RegisterRoutes(e)
		slog.Info("Echo server starting", slog.String("port", ":9992"))
		e.Logger.Fatal(e.Start(":9992"))
	}()

	// Wait indefinitely
	select {}
}
