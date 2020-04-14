create table avm_assets
(
    id             binary(32)        not null,
    chain_id       binary(32)        not null,

    name           varchar(255)      not null,
    symbol         varchar(255)      not null,
    alias          varchar(255)      not null,
    denomination   smallint unsigned not null,

    -- Denormalized data
    current_supply bigint unsigned   not null
);
create unique index avm_assets_id ON avm_assets (id);

create table avm_outputs
(
    id                       binary(32)        not null,
    transaction_id           binary(32)        not null,
    output_index             smallint unsigned not null,

    asset_id                 binary(32)        not null,
    output_type              int unsigned      not null,

    amount                   bigint unsigned   not null,
    locktime                 int unsigned      not null,
    threshold                int unsigned      not null,

    created_at               timestamp         not null,
    redeemed_at              timestamp         null,
    redeeming_transaction_id binary(32)        null
);
create index avm_outputs_asset_id ON avm_outputs (asset_id);
create unique index avm_outputs_tx_id_output_idx ON avm_outputs (transaction_id, output_index);

create table avm_output_addresses
(
    transaction_id      binary(32)        not null,
    output_index        smallint unsigned not null,
    address             binary(20)        not null,

    redeeming_signature varbinary(128)    null
);
create unique index avm_output_addrs_txid_output_idx on avm_outputs (transaction_id, output_index);
create unique index avm_output_addrs_txid_output_idx_addr on avm_output_addresses (transaction_id, output_index, address);

create table avm_transactions
(
    id                      binary(32)        not null,
    chain_id                binary(32)        not null,
    type                    varchar(255)      not null,

    canonical_serialization varbinary(16384)  not null,
    json_serialization      varbinary(16384)  not null,

    -- Metadata
    ingested_at             timestamp         not null,

    -- Denormalized data
    input_count             smallint unsigned not null,
    output_count            smallint unsigned not null,
    amount                  bigint unsigned   not null
);
create unique index avm_transactions_id ON avm_transactions (id);
create index avm_transactions_chain_id ON avm_transactions (chain_id);

create table addresses
(
    address    varchar(50) not null primary key,
    public_key binary(33)  null,
    created_at timestamp   not null default current_timestamp
);
