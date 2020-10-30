ALTER TABLE `avm_asset_address_counts` ADD COLUMN `chain_id` varchar(50) default null;
ALTER TABLE `avm_asset_aggregation` ADD COLUMN `chain_id` varchar(50) default null;
