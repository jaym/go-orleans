-- name: LookupGrain :one
SELECT grains.* FROM grains
    WHERE
        grain_type = $1 AND
        grain_id = $2;

-- name: InsertGrain :exec
INSERT INTO grains (node_name, grain_type, grain_id) 
    VALUES($1,$2,$3);

-- name: DeleteGrainActivation :exec
DELETE FROM grains
    WHERE
        node_name = $1 AND
        grain_type = COALESCE($2, grain_type) AND
        grain_id = COALESCE($3, grain_id);
