create table avm_outputs_redeeming
(
    id                       varchar(50)       not null primary key,
    redeemed_at              timestamp         null,
    redeeming_transaction_id varchar(50)       not null default "",
    created_at               timestamp         not null default current_timestamp
);
