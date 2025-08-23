package main

import (
	"os"

	"log/slog"

	"github.com/gofiber/fiber/v2"
	handlerv5 "github.com/hesusruiz/domeproxy/internal/handler/v5"
	repositoryv5 "github.com/hesusruiz/domeproxy/internal/repository/v5"
	servicev5 "github.com/hesusruiz/domeproxy/internal/service/v5"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"gitlab.com/greyxor/slogor"
)

func main() {
	// Configure slog logger
	handler := slogor.NewHandler(os.Stdout)
	slog.SetDefault(slog.New(handler))

	// Connect to the database
	db, err := sqlx.Connect("sqlite3", "tmf.db")
	if err != nil {
		slog.Error("failed to connect to database", slog.Any("error", err))
		os.Exit(1)
	}
	defer db.Close()

	// Create the table if it doesn't exist
	_, err = db.Exec(repositoryv5.CreateTMFTableSQL)
	if err != nil {
		slog.Error("failed to create table", slog.Any("error", err))
		os.Exit(1)
	}

	// Create the server
	app := fiber.New()

	// Create the service
	s := servicev5.NewService(db)

	// Create the handler
	h := handlerv5.NewHandler(s)

	// Register the API routes
	h.RegisterRoutes(app)

	// Start the server
	slog.Info("Server starting", slog.String("port", ":9991"))
	app.Listen(":9991")
}