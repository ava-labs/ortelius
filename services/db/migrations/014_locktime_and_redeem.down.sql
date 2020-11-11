ALTER TABLE `avm_outputs` DROP COLUMN locktime;
ALTER TABLE `avm_outputs` ADD COLUMN locktime int unsigned not null;
ALTER TABLE `avm_outputs` DROP COLUMN stake_locktime;
ALTER TABLE `avm_outputs` ADD COLUMN redeemed_at timestamp null;
ALTER TABLE `avm_outputs` ADD COLUMN redeeming_transaction_id varchar(50) default '' not null;
create index avm_outputs_chain_id_redeeming_transaction_id ON avm_outputs (chain_id, redeeming_transaction_id);