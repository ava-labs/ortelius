alter table `address_chain` drop COLUMN `updated_at`;
alter table `addresses` drop COLUMN `updated_at`;
alter table `addresses_bech32` drop COLUMN `updated_at`;
alter table `transactions_rewards_owners_address` drop COLUMN `updated_at`;
alter table `avm_output_addresses` drop COLUMN `updated_at`;

