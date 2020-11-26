create index `avm_outputs_asset_id` ON `avm_outputs` (asset_id);
drop index `avm_outputs_asset_id_output_type` on `avm_outputs`;
drop index `pvm_blocks_parent_id` on `pvm_blocks`;
DROP INDEX `avm_transactions_chain_id_created_at_asc` ON `avm_transactions`;

