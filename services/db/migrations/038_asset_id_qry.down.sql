drop index avm_outputs_transaction_id_asset_id_output_type on avm_outputs;
drop index avm_transactions_created_at_chain_id_idx on avm_transactions;
create index avm_transactions_created_at_idx on avm_transactions (created_at);

drop table cvm_logs;
alter table pvm_blocks drop column height;

