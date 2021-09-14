create table pvm_proposer
(
    id varchar(50) not null
        primary key,
    parent_id varchar(50) not null,
    blk_id varchar(50) not null,
    p_chain_height varchar(50) not null,
    proposer varchar(50) not null,
    time_stamp timestamp(6) default CURRENT_TIMESTAMP(6) not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create index pvm_proposer_blk_id ON pvm_proposer (blk_id);

