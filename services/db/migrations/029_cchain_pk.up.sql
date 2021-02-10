create index cvm_transactions_txdata_block ON cvm_transactions_txdata (block, idx);
drop index cvm_transactions_txdata_hash ON cvm_transactions_txdata;
alter table cvm_transactions_txdata DROP PRIMARY KEY, ADD PRIMARY KEY(hash);
