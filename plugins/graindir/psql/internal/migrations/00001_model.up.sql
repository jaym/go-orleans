CREATE UNLOGGED TABLE nodes(
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    last_checkin TIMESTAMPTZ,

    UNIQUE(name)
);

CREATE UNLOGGED TABLE grains(
    id   BIGSERIAL PRIMARY KEY,
    node_id BIGINT NOT NULL,
    node_name TEXT NOT NULL,
    grain_type TEXT NOT NULL,
    grain_id TEXT NOT NULL,

    UNIQUE(grain_type, grain_id)
);

ALTER TABLE grains ADD CONSTRAINT grains_node_fk FOREIGN KEY (node_id) REFERENCES nodes(id)
        ON DELETE CASCADE;

CREATE INDEX grain_directory_node_id_idx ON grains(node_id);
