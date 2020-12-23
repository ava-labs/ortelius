alter table `avm_outputs` add COLUMN `frozen` smallint default 0;
alter table `avm_outputs` drop COLUMN `mint` smallint default 0;