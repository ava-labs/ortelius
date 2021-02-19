create table transactions_rewards_owners
(
    id varchar(50) not null primary key,
    chain_id varchar(50) not null,
    locktime bigint unsigned not null,
    threshold int unsigned not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create table transactions_rewards_owners_address
(
    id           varchar(50) not null,
    address      varchar(50) not null,
    output_index smallint unsigned not null,
    primary key (id, address)
);

create index transactions_rewards_owners_address_address on transactions_rewards_owners_address (address);
