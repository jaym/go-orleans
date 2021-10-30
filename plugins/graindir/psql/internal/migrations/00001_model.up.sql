CREATE UNLOGGED TABLE grains(
    id   BIGSERIAL PRIMARY KEY,
    node_name TEXT NOT NULL,
    grain_type TEXT NOT NULL,
    grain_id TEXT NOT NULL,

    UNIQUE(grain_type, grain_id)
);

CREATE INDEX grain_directory_node_name_idx ON grains(node_name);
