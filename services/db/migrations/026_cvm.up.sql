alter table `cvm_transactions` add column `transaction_id` varchar(50) not null default '';
create index cvm_transactions_transaction_id ON cvm_transactions (transaction_id);
