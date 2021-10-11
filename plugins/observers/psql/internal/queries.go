package internal

import "context"

func (q *Queries) RemoveObserversFixed(ctx context.Context, ownerGrain string, observerGrain *string, observableName *string) error {
	// https://github.com/kyleconroy/sqlc/issues/937
	// COALESCE is not correctly handled by sqlc
	_, err := q.db.Exec(ctx, removeObservers, ownerGrain, observerGrain, observableName)
	return err
}
