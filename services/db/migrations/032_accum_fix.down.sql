drop index `output_addresses_accumulate_in_output_processed` on `output_addresses_accumulate_in`;
drop index `output_addresses_accumulate_in_output_id` on `output_addresses_accumulate_in`;
drop index `output_addresses_accumulate_out_output_id` on `output_addresses_accumulate_out`;
alter table `output_addresses_accumulate_in` drop COLUMN `output_procesed`;

alter table `output_addresses_accumulate_in` drop COLUMN `transaction_id`;
alter table `output_addresses_accumulate_out` drop COLUMN `transaction_id`;
alter table `output_addresses_accumulate_in` drop COLUMN output_index;
alter table `output_addresses_accumulate_out` drop COLUMN output_index;

drop table transactions_rewards_owners_outputs;
