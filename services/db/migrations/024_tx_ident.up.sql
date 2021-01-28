alter table `avm_transactions` add COLUMN `network_id` bigint unsigned default 0;
alter table `avm_outputs` add column `stakeableout` smallint unsigned default 0;
alter table `avm_outputs` add column `genesisutxo` smallint unsigned default 0;

