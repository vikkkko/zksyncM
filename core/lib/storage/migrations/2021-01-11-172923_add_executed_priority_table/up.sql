
-- Table for the executed priority operations (e.g. deposit).
CREATE TABLE executed_tx_and_priority_operations (
    priority_op_serialid BIGINT NOT NULL,
    deadline_block BIGINT NOT NULL,
    eth_block BIGINT NOT NULL,

    -- operation data
    tx jsonb NOT NULL,
    success bool NOT NULL,
    fail_reason TEXT,
    -- operation metadata
    primary_account_address bytea NOT NULL,
    nonce BIGINT NOT NULL,
    eth_sign_data JSONB,
    batch_id BIGINT,

    eth_or_tx_hash bytea NOT NULL PRIMARY KEY,
    -- sidechain block info
    block_number BIGINT NOT NULL,
    block_index INT,
    -- operation data
    operation jsonb NOT NULL,
    op_type BIGINT NOT NULL,
    -- operation metadata
    from_account bytea NOT NULL,
    to_account bytea ,
    created_at timestamp with time zone not null
);


CREATE INDEX executed_tx_and_priority_operations_block_number_index ON executed_tx_and_priority_operations (block_number);
CREATE INDEX executed_tx_and_priority_operations_hash_index ON executed_tx_and_priority_operations (eth_or_tx_hash);
CREATE INDEX executed_tx_and_priority_operations_from_account_index ON executed_tx_and_priority_operations (from_account);
CREATE INDEX executed_tx_and_priority_operations_to_account_index ON executed_tx_and_priority_operations (to_account);
CREATE INDEX executed_tx_and_priority_operations_serialid_index ON executed_tx_and_priority_operations (priority_op_serialid);
