create table avm_outputs_rewards
(
    id varchar(50) not null primary key,
    chain_id varchar(50) not null,
    transaction_id varchar(50) not null,
    output_index smallint unsigned not null,
    locktime bigint unsigned not null,
    threshold int unsigned not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create index avm_outputs_rewards_transaction_id ON avm_outputs_rewards (transaction_id);

