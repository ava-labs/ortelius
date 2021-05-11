drop table `node_index`;

drop index cvm_transactions_txdata_trace_from_addr_created_at
    on cvm_transactions_txdata_trace;
drop index cvm_transactions_txdata_trace_to_addr_created_at
    on cvm_transactions_txdata_trace;

create index cvm_transactions_txdata_trace_from_addr
    on cvm_transactions_txdata_trace (from_addr);

create index cvm_transactions_txdata_trace_to_addr
    on cvm_transactions_txdata_trace (to_addr);


drop index cvm_transactions_txdata_rcpt_created_at
    on cvm_transactions_txdata;

create index cvm_transactions_txdata_rcpt
    on cvm_transactions_txdata (rcpt);

alter table `rewards` drop COLUMN `processed`;
drop index rewards_processed on rewards;

