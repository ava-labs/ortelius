ALTER TABLE `avm_transactions` MODIFY `canonical_serialization` BLOB;
ALTER TABLE `avm_transactions` ADD COLUMN `memo` VARBINARY(1024);
