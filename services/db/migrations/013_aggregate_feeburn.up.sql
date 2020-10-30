create table aggregate_txfee
(
    aggregate_ts timestamp         primary key not null,
    tx_fee       DECIMAL(65)       default 0
);
