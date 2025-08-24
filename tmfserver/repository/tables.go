package repository

const CreateTMFTableSQL = `
CREATE TABLE IF NOT EXISTS tmf_object (
	"id" TEXT NOT NULL,
	"type" TEXT NOT NULL,
	"version" TEXT,
	"last_update" TEXT,
	"content" BLOB NOT NULL,
	"created_at" DATETIME NOT NULL,
	"updated_at" DATETIME NOT NULL,
	PRIMARY KEY ("id", "type", "version")
);
PRAGMA journal_mode = WAL;
`
