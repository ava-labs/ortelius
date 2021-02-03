drop index cvm_transactions_transaction_id ON cvm_transactions;
alter table `cvm_transactions` drop column `transaction_id`;
