ALTER TABLE `avm_transactions` MODIFY `canonical_serialization` MEDIUMBLOB;
ALTER TABLE `pvm_blocks` MODIFY `serialization` MEDIUMBLOB;
ALTER TABLE `avm_transactions` MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `avm_outputs`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `avm_outputs_redeeming`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `pvm_blocks`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `avm_assets`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `cvm_transactions`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `cvm_blocks`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `cvm_addresses`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `address_chain`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `addresses`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `avm_output_addresses`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `rewards`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `transactions_epoch`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `transactions_block`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);
alter table `transactions_validator`  MODIFY `created_at` timestamp(6) not null default current_timestamp(6);

alter table `cvm_transactions` add column `serialization` mediumblob;