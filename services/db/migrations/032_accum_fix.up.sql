alter table `output_addresses_accumulate_in` add COLUMN `output_processed` smallint unsigned not null default 0;

alter table `output_addresses_accumulate_in` add COLUMN `transaction_id` varchar(50)  not null default '';
alter table `output_addresses_accumulate_out` add COLUMN `transaction_id` varchar(50)  not null default '';
alter table `output_addresses_accumulate_in` add COLUMN output_index smallint unsigned not null default 0;
alter table `output_addresses_accumulate_out` add COLUMN output_index smallint unsigned not null default 0;

create index `output_addresses_accumulate_in_output_processed` on `output_addresses_accumulate_in` (output_processed asc, processed asc, created_at asc);
create index `output_addresses_accumulate_in_output_id` on `output_addresses_accumulate_in` (output_id asc);
create index `output_addresses_accumulate_out_output_id` on `output_addresses_accumulate_out` (output_id asc);

create table transactions_rewards_owners_outputs
(
    id varchar(50) not null primary key,
    transaction_id varchar(50)       not null,
    output_index int unsigned not null,
    created_at timestamp(6) default CURRENT_TIMESTAMP(6) not null
);

