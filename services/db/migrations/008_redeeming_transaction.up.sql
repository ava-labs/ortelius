create table avm_outputs_redeeming
(
    id                       varchar(50)       not null primary key,
    redeemed_at              timestamp         null,
    redeeming_transaction_id varchar(50)       not null,
    amount                   bigint unsigned   not null,
    output_index             int unsigned      not null,
    created_at               timestamp         not null default current_timestamp
);
create index avm_outputs_redeeming_redeeming_transaction_id ON avm_outputs_redeeming (redeeming_transaction_id);
