create table `cvm_transactions_txdata`
(
    block          decimal(65)     not null,
    idx            bigint unsigned not null,
    hash           varchar(100)    not null,
    to             varchar(50)     not null,
    serialization  mediumblob,
    created_at     timestamp(6)       not null default current_timestamp(6),
    primary key(block,idx)
);

create index cvm_transactions_txdata_hash ON cvm_transactions_txdata (hash);
create index cvm_transactions_txdata_to ON cvm_transactions_txdata (to);
