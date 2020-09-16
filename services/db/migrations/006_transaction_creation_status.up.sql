create table asset_aggregation_state
(
    id                       bigint unsigned not null primary key,
    created_at               timestamp         not null default current_timestamp,
    current_created_at       timestamp         not null default current_timestamp
);

create table asset_aggregation (
   aggregation_ts           timestamp     not null,
   asset_id                 varchar(50)   not null,
   transaction_volume       bigint unsigned   default 0,
   transaction_count        bigint unsigned   default 0,
   address_count            bigint unsigned   default 0,
   asset_count              bigint unsigned   default 0,
   output_count             bigint unsigned   default 0,
   PRIMARY KEY(aggregation_ts DESC, asset_id)
);

create index asset_aggregation_asset_id_created_at on asset_aggregation (asset_id, aggregation_ts DESC);