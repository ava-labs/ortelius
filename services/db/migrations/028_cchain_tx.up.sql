create table `cvm_transactions_txdata`
(
    block          decimal(65)     not null,
    idx            bigint unsigned not null,
    hash           varchar(100)    not null,
    rcpt           varchar(50)     not null,
    nonce          bigint unsigned not null,
    serialization  mediumblob,
    created_at     timestamp(6)       not null default current_timestamp(6),
    primary key(block,idx)
);

create index cvm_transactions_txdata_hash ON cvm_transactions_txdata (hash);
create index cvm_transactions_txdata_rcpt ON cvm_transactions_txdata (rcpt);

alter table `cvm_transactions` add COLUMN `tx_time` timestamp(6) not null default current_timestamp(6);
alter table `cvm_transactions` add COLUMN `nonce` bigint unsigned not null default 0;
alter table `cvm_transactions` add COLUMN `hash` varchar(100)    not null default '';
alter table `cvm_transactions` add COLUMN `parent_hash` varchar(100)    not null default '';

create index cvm_transactions_hash ON cvm_transactions (hash);
create index cvm_transactions_parent_hash ON cvm_transactions (parent_hash);
