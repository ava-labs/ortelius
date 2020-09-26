ALTER TABLE `avm_outputs` ADD COLUMN `group_id` bigint unsigned default 0;
ALTER TABLE `avm_outputs` ADD COLUMN `payload` varbinary(4096);
