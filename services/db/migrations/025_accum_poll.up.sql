create table accumulate_balances
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    transaction_count decimal(65) not null default 0,
    total_received    decimal(65) not null default 0,
    total_sent        decimal(65) not null default 0,
    utxo_count        decimal(65) not null default 0
);

create table output_addresses_accumulate
(
    id             varchar(50) not null,
    address        varchar(50) not null,
    processed_out  smallint unsigned not null default 0,
    processed_in   smallint unsigned not null default 0,
    created_at timestamp(6)       not null default current_timestamp(6),
    primary key (id,address)
);

create index `output_addresses_accumulate_processed_out` on `output_addresses_accumulate` (processed_out asc, created_at asc);
create index `output_addresses_accumulate_processed_in` on `output_addresses_accumulate` (processed_in asc, created_at asc);

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

