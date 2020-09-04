ALTER TABLE `avm_transactions` DROP COLUMN `memo`;
ALTER TABLE `avm_transactions` MODIFY `canonical_serialization` VARBINARY(64000);
