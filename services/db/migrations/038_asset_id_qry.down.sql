drop index avm_outputs_transaction_id_asset_id_output_type on avm_outputs;
drop table cvm_logs;
alter table pvm_blocks drop column height;