create table `cvm_blocks`
(
    block decimal(65) not null primary key,
    created_at              timestamp        not null default current_timestamp
);

create table `cvm_transactions`
(
    id             varchar(50)     not null primary key,
    type           smallint        not null,
    blockchain_id  varchar(50)     not null,
    block          decimal(65)     not null,
    created_at                     timestamp       not null default current_timestamp
);

create table `cvm_addresses`
(
    id             varchar(50)     not null primary key,
    type           smallint        not null,
    idx            int unsigned    not null,
    transaction_id varchar(50)     not null,
    address        varchar(50)     not null,
    asset_id       varchar(50)     not null,
    amount         bigint unsigned not null default 0,
    nonce          bigint unsigned not null default 0,
    created_at     timestamp       not null default current_timestamp
);

create index cvm_address_transaction_id ON cvm_addresses (transaction_id);
