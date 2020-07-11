create table avm_assets
(
    id             varchar(50)       not null primary key,
    chain_id       varchar(50)       not null,

    name           varchar(255)      not null,
    symbol         varchar(255)      not null,
    alias          varchar(255)      not null,
    denomination   smallint unsigned not null,

    created_at     timestamp         not null default current_timestamp,

    -- Denormalized data
    current_supply bigint unsigned   not null
);

create table avm_outputs
(
    id                       varchar(50)       not null primary key,
    chain_id       varchar(50)       not null,

    transaction_id           varchar(50)       not null,
    output_index             smallint unsigned not null,

    asset_id                 varchar(50)       not null,
    output_type              int unsigned      not null,
    amount                   bigint unsigned   not null,
    locktime                 int unsigned      not null,
    threshold                int unsigned      not null,

    redeemed_at              timestamp         null,
    redeeming_transaction_id varchar(50)       not null default "",
    created_at               timestamp         not null default current_timestamp
);
create index avm_outputs_asset_id ON avm_outputs (asset_id);
create index avm_outputs_chain_id_id ON avm_outputs (chain_id, id);
create unique index avm_outputs_tx_id_output_idx ON avm_outputs (transaction_id, output_index);

create table avm_output_addresses
(
    output_id           varchar(50)    not null,
    address             varchar(50)    not null,
    redeeming_signature varbinary(128) null,
    created_at          timestamp      not null default current_timestamp
);
create index avm_output_addresses_output_id on avm_output_addresses (output_id);
create unique index avm_output_addresses_output_id_addr on avm_output_addresses (output_id, address);

create table avm_transactions
(
    id                      varchar(50)      not null primary key,
    chain_id                varchar(50)      not null,
    type                    varchar(255)     not null,

    canonical_serialization varbinary(64000) not null,

    created_at              timestamp        not null default current_timestamp
);
create unique index avm_transactions_id ON avm_transactions (id);
create index avm_transactions_chain_id ON avm_transactions (chain_id);
create index avm_transactions_created_at_idx ON avm_transactions (created_at);

create table addresses
(
    address    varchar(50) not null primary key,
    public_key binary(33)  null,
    created_at timestamp   not null default current_timestamp
);
