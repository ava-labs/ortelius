##
## Accounts
##

create table pvm_accounts
(
    internal_id int          not null auto_increment primary key,
    chain_id    binary(32)   not null,

    address          binary(20)   not null,
    pubkey    binary(33)   not null,
    balance  bigint unsigned not null,
    nonce   bigint unsigned not null
);
create index pvm_accounts_chain_id ON pvm_accounts (chain_id);
create unique index pvm_accounts_chain_id_address ON pvm_accounts (chain_id, address);

##
## Subnets
##

create table pvm_subnets
(
    internal_id int          not null auto_increment primary key,
    id          binary(32)   not null,
    chain_id    binary(32)   not null,
    network_id  int unsigned not null,
    threshold   int unsigned not null
);
create unique index pvm_subnets_id_idx ON pvm_subnets (id);

create table pvm_subnet_control_keys
(
    internal_id int        not null auto_increment primary key,
    subnet_id   binary(32) not null,
    address     binary(20) not null

);
create unique index pvm_subnet_control_keys_subnet_id_address_idx ON pvm_subnet_control_keys (subnet_id, address);

##
## Validators
##

create table pvm_validators
(
    internal_id int          not null auto_increment primary key,
    node_id          binary(32)   not null,
    subnet_id  int unsigned not null,
    start_time   datetime not null,
    end_time     datetime not null,
    weight bigint unsigned not null
);
create index pvm_validators_node_id ON pvm_validators (node_id);
create index pvm_validators_subnet_id ON pvm_validators (subnet_id);

##
## Chains
##

create table pvm_chains
(
    internal_id  int              not null auto_increment primary key,
    id           binary(32)       not null,
    transaction_id           binary(32)       not null,
    subnet_id    binary(32)       not null,

    name         varchar(255)     not null,
    vm_type      binary(32)     not null,
    genesis_data varbinary(16384) not null
);
create unique index pvm_chains_id_idx ON pvm_chains (id);

create table pvm_chains_fx_ids
(
    internal_id int        not null auto_increment primary key,
    chain_id    binary(32) not null,
    fx_id       binary(32) not null

);
create unique index pvm_chains_fx_ids_chain_id_fix_id_idx ON pvm_chains_fx_ids (chain_id, fx_id);

##
## Blocks
##

create table pvm_blocks
(
    internal_id  int              not null auto_increment primary key,
    id           binary(32)       not null,
    parent_id           binary(32)       not null,
    chain_id    binary(32)       not null
);

create table pvm_blocks_transactions
(
    internal_id  int              not null auto_increment primary key,
    block_id           binary(32)       not null,
    transaction_id           binary(32)       not null
);

##
## Import/Export
##
# create table pvm_imports
# (
#     internal_id    int               not null auto_increment primary key,
#     asset_id       binary(32)        not null,
#     amount         bigint unsigned   not null,
#
#     transaction_id binary(32)        not null,
#     output_index   smallint unsigned not null
#
# );
# create table pvm_exports
# (
#     internal_id int             not null auto_increment primary key,
#     asset_id    binary(32)      not null,
#     amount      bigint unsigned not null
# );

##
## Times
##
create table pvm_time_txs
(
    internal_id    int        not null auto_increment primary key,
    transaction_id binary(32) not null,
    timestamp      datetime   not null

);
create table pvm_transactions
(
    internal_id             int              not null auto_increment primary key,
    id                      binary(32)       not null,
    chain_id                binary(32)       not null,

    type                    smallint         not null,
    account                 binary(20)       not null,
    account_pubkey          binary(33),
    amount                  bigint unsigned  not null,
    nonce                   bigint unsigned     not null,
    signature          binary(65),

    canonical_serialization varbinary(16384) not null,
    json_serialization      varbinary(16384) not null,

    -- Metadata
    ingested_at             timestamp        not null
);
create unique index pvm_transactions_id_idx ON pvm_transactions (id);

