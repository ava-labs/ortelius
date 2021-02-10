drop index `output_addresses_accumulate_in_output_processed` on `output_addresses_accumulate_in`;
drop index `output_addresses_accumulate_in_output_id` on `output_addresses_accumulate_in`;
alter table `output_addresses_accumulate_in` drop COLUMN `output_procesed`;
