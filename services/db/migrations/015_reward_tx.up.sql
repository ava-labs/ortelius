create table `rewards`
(
    id                   varchar(50) not null primary key,
    txid                 varchar(50) not null,
    shouldprefercommit   smallint unsigned default 0,
    created_at           timestamp not null default current_timestamp
);

