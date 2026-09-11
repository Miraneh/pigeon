CREATE TABLE report_stats (
    identity_id   BIGINT NOT NULL REFERENCES identities (id),
    hour_bucket   TIMESTAMPTZ NOT NULL,
    messages_sent BIGINT NOT NULL DEFAULT 0 CHECK (messages_sent >= 0),
    amount_spent  BIGINT NOT NULL DEFAULT 0 CHECK (amount_spent >= 0),
    PRIMARY KEY (identity_id, hour_bucket)
);
