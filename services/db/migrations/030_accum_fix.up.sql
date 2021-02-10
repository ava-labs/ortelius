alter table `output_addresses_accumulate_in` add COLUMN `output_processed` smallint unsigned not null default 0;
create index `output_addresses_accumulate_in_output_processed` on `output_addresses_accumulate_in` (output_processed asc, processed asc, created_at asc);
create index `output_addresses_accumulate_in_output_id` on `output_addresses_accumulate_in` (output_id asc);
