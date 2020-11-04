ALTER TABLE `avm_transactions` ADD COLUMN `txfee` bigint unsigned default 0;
ALTER TABLE `avm_transactions` ADD COLUMN `genesis` smallint default 0;
