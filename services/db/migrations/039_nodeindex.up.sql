create table `node_index`
(
    instance varchar(100) not null,
    topic varchar(100) not null,
    idx   bigint unsigned default 0,
    primary key (instance, topic)
);

drop index cvm_transactions_txdata_trace_from_addr
    on cvm_transactions_txdata_trace;
drop index cvm_transactions_txdata_trace_to_addr
    on cvm_transactions_txdata_trace;

create index cvm_transactions_txdata_trace_from_addr_created_at
    on cvm_transactions_txdata_trace (from_addr, created_at);

create index cvm_transactions_txdata_trace_to_addr_created_at
    on cvm_transactions_txdata_trace (to_addr, created_at);

drop index cvm_transactions_txdata_rcpt
    on cvm_transactions_txdata;

create index cvm_transactions_txdata_rcpt_created_at
    on cvm_transactions_txdata (rcpt,created_at);

alter table `rewards` add COLUMN `processed` smallint unsigned default 0;
create index rewards_processed on rewards (processed, created_at);



