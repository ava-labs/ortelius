create index cvm_transactions_txdata_trace_to_addr on cvm_transactions_txdata_trace (to_addr);
create index cvm_transactions_txdata_trace_from_addr on cvm_transactions_txdata_trace (from_addr);
create index tx_pool_msg_key on tx_pool (msg_key);

