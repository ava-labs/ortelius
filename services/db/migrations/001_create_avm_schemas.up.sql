create table avm_assets
(
    internal_id    int               not null auto_increment primary key,

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
    internal_id              int               not null auto_increment primary key,

    transaction_id           binary(32)        not null,
    output_index             smallint unsigned not null,

    asset_id                 binary(32)        not null,
    output_type              int unsigned      not null,

    amount                   bigint unsigned   not null,
    locktime                 int unsigned      not null,
    threshold                int unsigned      not null,

    redeeming_transaction_id binary(32)        null
);
create unique index avm_outputs_tx_id_output_idx ON avm_outputs (transaction_id, output_index);

create table avm_output_addresses
(
    internal_id         int               not null auto_increment primary key,

    transaction_id      binary(32)        not null,
    output_index        smallint unsigned not null,
    address             binary(20)        not null,

    redeeming_signature varbinary(128)    null
);
create unique index avm_output_addrs_txid_output_idx on avm_outputs (transaction_id, output_index);
create unique index avm_output_addrs_txid_output_idx_addr on avm_output_addresses (transaction_id, output_index, address);

create table avm_transactions
(
    internal_id             int               not null auto_increment primary key,

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

