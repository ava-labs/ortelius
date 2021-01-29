alter table `avm_transactions` add COLUMN `network_id` bigint unsigned default 0;
alter table `avm_outputs` add column `stakeableout` smallint unsigned default 0;
alter table `avm_outputs` add column `genesisutxo` smallint unsigned default 0;

create table addresses_bech32
(
    address        varchar(50) not null primary key,
    bech32_address varchar(100)
);

create index addresses_bech32_bech32_address ON addresses_bech32 (bech32_address);
