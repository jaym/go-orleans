package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4/pgxpool"
)

type migrateFunc func(*sql.DB) error

func setupDatabase(ctx context.Context, name string, f migrateFunc) (*pgxpool.Pool, error) {
	pgUser := os.Getenv("PG_USER")
	pgHost := os.Getenv("PG_HOST")
	pgPort := os.Getenv("PG_PORT")
	pgPass := os.Getenv("PG_PASSWORD")
	pgDB := os.Getenv("PG_DATABASE")

	if pgUser == "" {
		pgUser = "postgres"
	}

	if pgPass == "" {
		pgPass = "postgres"
	}

	if pgPort == "" {
		pgPort = "5432"
	}

	if pgHost == "" {
		pgHost = "127.0.0.1"
	}

	if pgDB == "" {
		pgDB = "postgres"
	}

	source := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?search_path=%s", pgUser, pgPass, pgHost, pgPort, pgDB, name)
	if os.Getenv("START_PG") == "true" {
		err := func() error {
			stdDb, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pgUser, pgPass, pgHost, pgPort, pgDB))
			if err != nil {
				return err
			}
			defer stdDb.Close()
			if _, err := stdDb.Exec("CREATE SCHEMA " + name); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
		stdDb, err := sql.Open("pgx", source)
		if err != nil {
			return nil, err
		}
		defer stdDb.Close()

		if err := f(stdDb); err != nil {
			return nil, err
		}
	}

	return pgxpool.Connect(ctx, source)
}
