CREATE INDEX avm_transactions_chain_id_created_at ON `avm_transactions` (chain_id, created_at desc);
DROP INDEX avm_transactions_chain_id ON `avm_transactions`;
