create table address_chain
(
    address  varchar(50) not null,
    chain_id varchar(50) not null,
    created_at          timestamp      not null default current_timestamp,
    primary key (address,chain_id)
);
