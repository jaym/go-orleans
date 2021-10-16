// Code generated by sqlc. DO NOT EDIT.
// source: queries.sql

package internal

import (
	"context"
	"database/sql"
)

const listObservers = `-- name: ListObservers :many
SELECT observers.id, observers.owner_grain, observers.observable_name, observers.observer_grain, observers.expires, observer_values.val FROM observers
    INNER JOIN observer_values ON
        observers.id = observer_values.id
    WHERE
        owner_grain = $1 AND
        observable_name = $2
`

type ListObserversParams struct {
	OwnerGrain     string
	ObservableName string
}

type ListObserversRow struct {
	ID             int64
	OwnerGrain     string
	ObservableName string
	ObserverGrain  string
	Expires        sql.NullTime
	Val            []byte
}

func (q *Queries) ListObservers(ctx context.Context, arg ListObserversParams) ([]ListObserversRow, error) {
	rows, err := q.db.Query(ctx, listObservers, arg.OwnerGrain, arg.ObservableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListObserversRow
	for rows.Next() {
		var i ListObserversRow
		if err := rows.Scan(
			&i.ID,
			&i.OwnerGrain,
			&i.ObservableName,
			&i.ObserverGrain,
			&i.Expires,
			&i.Val,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const removeObservers = `-- name: RemoveObservers :exec
DELETE FROM observers
    WHERE
        owner_grain = $1 AND
        observer_grain = COALESCE($2, observer_grain) AND
        observable_name = COALESCE($3, observable_name)
`

type RemoveObserversParams struct {
	OwnerGrain     string
	ObserverGrain  string
	ObservableName string
}

func (q *Queries) RemoveObservers(ctx context.Context, arg RemoveObserversParams) error {
	_, err := q.db.Exec(ctx, removeObservers, arg.OwnerGrain, arg.ObserverGrain, arg.ObservableName)
	return err
}

const upsertObserver = `-- name: UpsertObserver :one
INSERT INTO observers (owner_grain, observable_name, observer_grain, expires) 
    VALUES($1,$2,$3,$4)
    ON CONFLICT ON CONSTRAINT observers_owner_grain_observable_name_observer_grain_key DO UPDATE
        SET expires = EXCLUDED.expires
    RETURNING id
`

type UpsertObserverParams struct {
	OwnerGrain     string
	ObservableName string
	ObserverGrain  string
	Expires        sql.NullTime
}

func (q *Queries) UpsertObserver(ctx context.Context, arg UpsertObserverParams) (int64, error) {
	row := q.db.QueryRow(ctx, upsertObserver,
		arg.OwnerGrain,
		arg.ObservableName,
		arg.ObserverGrain,
		arg.Expires,
	)
	var id int64
	err := row.Scan(&id)
	return id, err
}

const upsertObserverValue = `-- name: UpsertObserverValue :exec
INSERT INTO observer_values (observers_id, val)
    VALUES($1, $2)
    ON CONFLICT(observers_id) DO UPDATE
        SET val = EXCLUDED.val
`

type UpsertObserverValueParams struct {
	ObserversID int64
	Val         []byte
}

func (q *Queries) UpsertObserverValue(ctx context.Context, arg UpsertObserverValueParams) error {
	_, err := q.db.Exec(ctx, upsertObserverValue, arg.ObserversID, arg.Val)
	return err
}