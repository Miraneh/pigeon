CREATE TABLE balance_increases (
    identity_id     BIGINT NOT NULL REFERENCES identities (id),
    idempotency_key TEXT NOT NULL,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    balance_after   BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (identity_id, idempotency_key)
);
