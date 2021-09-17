create table pvm_proposer
(
    id varchar(50) not null
        primary key,
    parent_id varchar(50),
    blk_id varchar(50),
    proposer_blk_id varchar(50),
    p_chain_height varchar(50),
    proposer varchar(50),
    time_stamp timestamp(6) default CURRENT_TIMESTAMP(6) not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

create index pvm_proposer_blk_id ON pvm_proposer (blk_id);
create index pvm_proposer_proposer_blk_id ON pvm_proposer (proposer_blk_id);

create index pvm_blocks_height on pvm_blocks (height);
