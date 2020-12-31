CREATE INDEX avm_outputs_chain_id_created_at ON `avm_outputs` (chain_id, created_at desc);
CREATE INDEX avm_outputs_chain_id_asset_id_created_at ON `avm_outputs` (chain_id, asset_id, created_at desc);
