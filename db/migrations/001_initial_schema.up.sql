create table avm_outputs
(
    transaction_id           binary(32)        not null,
    output_index             smallint unsigned not null,

    asset_id                 binary(32)        not null,
    output_type              int unsigned      not null,

    amount                   bigint unsigned   not null,
    locktime                 int unsigned      not null,
    threshold                int unsigned      not null,

    redeeming_transaction_id binary(32)        null,

    PRIMARY KEY (transaction_id, output_index)
);

create table avm_output_addresses
(
    transaction_id      binary(32)        not null,
    output_index        smallint unsigned not null,
    address             binary(20)        not null,

    redeeming_signature varbinary(128)    null,
    PRIMARY KEY (transaction_id, output_index, address)
);

create index output_addresses_transaction_id_output_index_index
    on avm_output_addresses (transaction_id, output_index);

create table avm_transactions
(
    id                      binary(32)        not null primary key,
    network_id              smallint unsigned not null,
    chain_id                binary(32)        not null,

    canonical_serialization varbinary(16384)  not null,

    -- Denormalized data
    input_count             smallint unsigned not null,
    output_count            smallint unsigned not null,
    amount                  bigint unsigned   not null
);

