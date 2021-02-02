create table `cvm_transactions_txdata`
(
    block          decimal(65)     not null,
    idx            bigint unsigned not null,
    hash           varchar(100)    not null,
    serialization  mediumblob,
    created_at                     timestamp       not null default current_timestamp,
    primary key(block,idx)
);

create index cvm_transactions_txdata_block ON cvm_transactions_txdata (block);
