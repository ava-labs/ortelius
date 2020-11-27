create table `transaction_rewards`
(
    id              varchar(50) not null primary key,
    type            smallint         not null,
    created_at      timestamp        not null default current_timestamp
);



