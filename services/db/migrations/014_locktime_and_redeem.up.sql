ALTER TABLE `avm_outputs` MODIFY COLUMN locktime bigint unsigned not null;
ALTER TABLE `avm_outputs` ADD COLUMN stake_locktime bigint unsigned not null default 0;
ALTER TABLE `avm_outputs` DROP COLUMN redeemed_at;
ALTER TABLE `avm_outputs` DROP COLUMN redeeming_transaction_id;
drop index avm_outputs_chain_id_redeeming_transaction_id ON avm_outputs;
