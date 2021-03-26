create table `node_index`
(
    instance varchar(100) not null,
    topic varchar(100) not null,
    idx   bigint unsigned default 0,
    primary key (instance, topic)
);


