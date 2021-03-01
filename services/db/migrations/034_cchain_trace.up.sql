create table `cvm_transactions_txdata_trace`
(
    hash           varchar(100)    not null,
    idx            bigint unsigned not null,
    to_addr        varchar(50)     not null,
    from_addr      varchar(50)     not null,
    call_type      varchar(255)    not null,
    type           varchar(255)    not null,
    serialization  mediumblob,
    created_at     timestamp(6)    not null default current_timestamp(6),
    primary key(hash,idx)
);
