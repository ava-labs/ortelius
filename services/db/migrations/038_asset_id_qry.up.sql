create index avm_outputs_transaction_id_asset_id_output_type on avm_outputs (transaction_id, asset_id, output_type);
create index avm_transactions_created_at_chain_id_idx on avm_transactions (created_at, chain_id);
drop index avm_transactions_created_at_idx on avm_transactions;

create table cvm_logs
(
    id            varchar(50)                               not null primary key,
    block_hash    varchar(100)                              not null,
    tx_hash       varchar(100)                              not null,
    log_index     bigint unsigned                           not null,
    first_topic   varchar(256)                              not null,
    block         decimal(65)                               not null,
    Removed       smallint                                  not null,
    created_at    timestamp(6) default CURRENT_TIMESTAMP(6) not null,
    serialization MEDIUMBLOB
);

create index cvm_logs_block on cvm_logs (block);
create index cvm_logs_block_hash on cvm_logs (block_hash);

alter table pvm_blocks add column height bigint unsigned default 0 not null;


