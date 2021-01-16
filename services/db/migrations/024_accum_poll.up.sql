create table accumulate_balances
(
    id                varchar(50) not null primary key,
    chain_id          varchar(50) not null,
    asset_id          varchar(50) not null,
    address           varchar(50) not null,
    transaction_count decimal(65) not null default 0,
    total_received    decimal(65) not null default 0,
    total_sent        decimal(65) not null default 0,
    balance           decimal(65) not null default 0,
    utxo_count        decimal(65) not null default 0
);

create table output_addresses_accumulate
(
    output_id  varchar(50)    not null,
    address    varchar(50)    not null,
    type       smallint unsigned not null,
    processed  smallint unsigned not null default 0,
    out_avail  smallint unsigned not null default 0,
    primary key (output_id, address, type)
);

create index `output_addresses_accumulate_processed_type` on `output_addresses_accumulate` (processed, type);
