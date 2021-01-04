ALTER TABLE `avm_outputs` ADD COLUMN `stake` smallint unsigned default 0;

create table `transactions_validator`
(
    id         varchar(50)        not null primary key,
    node_id    varchar(50)        default '',
    start      bigint             unsigned default 0,
    end        bigint             unsigned default 0,
    created_at timestamp not null default current_timestamp
);

create table `transactions_block`
(
    id          varchar(50)        not null primary key,
    tx_block_id varchar(50)        default '',
    created_at  timestamp not null default current_timestamp
);

