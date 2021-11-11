-- name: LookupGrain :one
SELECT grains.* FROM grains
    WHERE
        grain_type = $1 AND
        grain_id = $2;

-- name: InsertGrain :exec
INSERT INTO grains (node_id, node_name, grain_type, grain_id) 
    VALUES($1,$2,$3,$4);

-- name: DeleteGrainActivation :exec
DELETE FROM grains
    WHERE
        node_id = $1 AND
        grain_type = COALESCE($2, grain_type) AND
        grain_id = COALESCE($3, grain_id);

-- name: DeleteNodeByID :exec
DELETE from nodes
    WHERE
        id = $1;

-- name: DeleteNodeByName :exec
DELETE from nodes
    WHERE
        name = $1;

-- name: DeleteNodeByCheckin :exec
DELETE from nodes
    WHERE
        last_checkin < $1;

-- name: UpdateNodeCheckin :execrows
UPDATE nodes
    SET last_checkin = NOW()
WHERE
    id = $1;

-- name: InsertNode :one
INSERT INTO nodes (name, last_checkin) VALUES($1, NOW()) RETURNING id;