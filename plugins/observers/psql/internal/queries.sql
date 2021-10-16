-- name: ListObservers :many
SELECT observers.*, observer_values.val FROM observers
    INNER JOIN observer_values ON
        observers.id = observer_values.id
    WHERE
        owner_grain = $1 AND
        observable_name = COALESCE($2, observable_name);

-- name: UpsertObserver :one
INSERT INTO observers (owner_grain, observable_name, observer_grain, expires) 
    VALUES($1,$2,$3,$4)
    ON CONFLICT ON CONSTRAINT observers_owner_grain_observable_name_observer_grain_key DO UPDATE
        SET expires = EXCLUDED.expires
    RETURNING id;

-- name: UpsertObserverValue :exec
INSERT INTO observer_values (observers_id, val)
    VALUES($1, $2)
    ON CONFLICT(observers_id) DO UPDATE
        SET val = EXCLUDED.val;

-- name: RemoveObservers :exec
DELETE FROM observers
    WHERE
        owner_grain = $1 AND
        observer_grain = COALESCE($2, observer_grain) AND
        observable_name = COALESCE($3, observable_name);
