drop table avm_asset_address_counts;
drop table avm_asset_aggregation;
drop table avm_asset_aggregation_state;

alter table `address_chain` ADD COLUMN `updated_at` timestamp(6) default CURRENT_TIMESTAMP(6) not null;
alter table `addresses` ADD COLUMN `updated_at` timestamp(6) default CURRENT_TIMESTAMP(6) not null;
alter table `addresses_bech32` ADD COLUMN `updated_at` timestamp(6) default CURRENT_TIMESTAMP(6) not null;
alter table `transactions_rewards_owners_address`  ADD COLUMN `updated_at` timestamp(6) default CURRENT_TIMESTAMP(6) not null;
alter table `avm_output_addresses` ADD COLUMN `updated_at` timestamp(6) default CURRENT_TIMESTAMP(6) not null;

