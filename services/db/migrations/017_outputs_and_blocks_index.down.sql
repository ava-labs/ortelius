create index `avm_outputs_asset_id` ON `avm_outputs` (asset_id);
drop index `avm_outputs_asset_id_output_type` on `avm_outputs`;
drop index `pvm_blocks_parent_id` on `pvm_blocks`;

