
create table `cvm_transactions`
(
    id             varchar(50)     not null primary key,
    blockchain_id  varchar(50)     not null,
    chain_id       varchar(50)     not null,
    created_at     timestamp       not null default current_timestamp
);

create table `cvm_address`
(
    type           smallint        not null,
    id             varchar(50)     not null,
    idx            int unsigned    not null,
    transaction_id varchar(50)     not null,
    address        varchar(50)     not null,
    asset_id       varchar(50)     not null,
    amount         bigint unsigned default 0,
    nonce          bigint unsigned default 0,
    created_at     timestamp       not null default current_timestamp,
    primary key (type, id)
);

create index cvm_address_transaction_id ON cvm_address (transaction_id);
