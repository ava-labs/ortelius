create table `rewards`
(
    id                   varchar(50) not null primary key,
    block_id             varchar(50) not null,
    txid                 varchar(50) not null,
    shouldprefercommit   smallint unsigned default 0,
    created_at           timestamp not null default current_timestamp
);

create index rewards_block_id ON rewards (block_id);
create index rewards_txid ON rewards (txid);


