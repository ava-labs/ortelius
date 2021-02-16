create table `tx_pool`
(
    id             varchar(50)        not null primary key,
    network_id     bigint unsigned default 0,
    chain_id       varchar(50)        not null,
    msg_key        varchar(50)        not null,
    serialization  mediumblob,
    processed      smallint unsigned  not null default 0,
    topic          varchar(100)       not null,
    created_at     timestamp(6)       not null default current_timestamp(6)
);

create index tx_pool_processed ON tx_pool (topic, processed, created_at asc);
