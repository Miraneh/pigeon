CREATE TABLE identities (
    id         BIGINT PRIMARY KEY,
    balance    BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE balance_locks (
    identity_id BIGINT NOT NULL REFERENCES identities (id),
    hour_bucket TIMESTAMPTZ NOT NULL,
    amount      BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (identity_id, hour_bucket)
);

CREATE INDEX idx_balance_locks_hour_bucket ON balance_locks (hour_bucket);
