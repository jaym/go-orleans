CREATE TABLE observers(
      id   BIGSERIAL PRIMARY KEY,
      owner_grain TEXT NOT NULL,
      observable_name TEXT NOT NULL,
      observer_grain TEXT NOT NULL,
      expires TIMESTAMP,

      UNIQUE(owner_grain, observable_name, observer_grain)
);

CREATE INDEX observers_observer_grain_idx ON observers(observer_grain);

CREATE TABLE observer_values(
    id   BIGSERIAL PRIMARY KEY,
    observers_id BIGINT NOT NULL,
    val BYTEA,

    UNIQUE(observers_id),

    CONSTRAINT fk_observer_values_observers_id
        FOREIGN KEY(observers_id) REFERENCES observers(id) ON DELETE CASCADE
);

CREATE INDEX observer_values_observers_id_idx ON observer_values(observers_id);
