create table `transaction_epoch`
(
    id         varchar(50)     not null primary key,
    epoch      bigint unsigned default 0,
    created_at timestamp       not null default current_timestamp
)