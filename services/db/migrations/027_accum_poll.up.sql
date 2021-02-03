create table accumulate_balances_received
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    total_amount      decimal(65) not null default 0,
    utxo_count        decimal(65) not null default 0,
    updated_at timestamp(6)       not null default current_timestamp(6)
);

create index `accumulate_balances_received_address_chain_id` on `accumulate_balances_received` (address,chain_id);
create index `accumulate_balances_received_chain_id` on `accumulate_balances_received` (chain_id);

create table accumulate_balances_sent
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    total_amount      decimal(65) not null default 0,
    utxo_count        decimal(65) not null default 0,
    updated_at timestamp(6)       not null default current_timestamp(6)
);

create table accumulate_balances_transactions
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    transaction_count decimal(65) not null default 0,
    updated_at timestamp(6)       not null default current_timestamp(6)
);


create table output_addresses_accumulate_out
(
    id             varchar(50) not null primary key,
    output_id      varchar(50) not null,
    address        varchar(50) not null,
    processed   smallint unsigned not null default 0,
    created_at timestamp(6)       not null default current_timestamp(6)
);

create index `output_addresses_accumulate_out_processed` on `output_addresses_accumulate_out` (processed asc, created_at asc);

create table output_addresses_accumulate_in
(
    id             varchar(50) not null primary key,
    output_id      varchar(50) not null,
    address        varchar(50) not null,
    processed   smallint unsigned not null default 0,
    created_at timestamp(6)       not null default current_timestamp(6)
);
create index `output_addresses_accumulate_in_processed` on `output_addresses_accumulate_in` (processed asc, created_at asc);

create table output_txs_accumulate
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    transaction_id    varchar(50) not null,
    processed         smallint unsigned not null default 0,
    created_at timestamp(6)       not null default current_timestamp(6)
);

create index `output_txs_accumulate_processed` on `output_txs_accumulate` (processed asc, created_at asc);

