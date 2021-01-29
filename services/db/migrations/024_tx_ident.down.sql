alter table `avm_transactions` drop COLUMN `network_id`;
alter table `avm_outputs` drop column `stakeableout`;
alter table `avm_outputs` drop column `genesisutxo`;

drop table addresses_bech32;
