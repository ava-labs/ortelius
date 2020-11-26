create index `avm_outputs_asset_id_output_type` ON `avm_outputs` (asset_id,output_type);
drop index `avm_outputs_asset_id` on `avm_outputs`;
create index `pvm_blocks_parent_id` on `pvm_blocks` (parent_id);
CREATE INDEX `avm_transactions_chain_id_created_at_asc` ON `avm_transactions` (chain_id, created_at asc);

