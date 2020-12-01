create table `transactions_epoch`
(
    id         varchar(50)     not null primary key,
    epoch      bigint unsigned not null default 0,
    created_at timestamp       not null default current_timestamp
);
