SET search_path TO public;
create extension pg_trgm;
create extension btree_gin;

create schema dds;
create table dds.entity
(
    urn text not null,
	loaded_by text not null,
	entity_name text not null,
	entity_type text not null,
	entity_name_short text,
	info text,
	search_data text,
    codes jsonb,
    grid jsonb,
	json_data jsonb,
	json_system jsonb,
    json_data_ui jsonb,
    htmls jsonb,
	links jsonb,
    notifications jsonb,
    tables jsonb,
	tags jsonb,
	filters jsonb,
	processed_dttm timestamp default now()
);
create index entity_urn_index on dds.entity (urn);
create index entity_name_trgm_index on dds.entity using gin (entity_name gin_trgm_ops);
create index entity_loaded_by_index on dds.entity using gin (loaded_by);
create index entity_type_index on dds.entity using gin (entity_type);
create index entity_search_data_trgm_gin_index on dds.entity using gin (search_data gin_trgm_ops);
create index entity_json_system ON dds.entity using gin (json_system);

create table dds.relation
(
	source text default 'non',
	destination text default 'non',
	type text not null,
	loaded_by text not null,
	attribute text default 'non',
	processed_dttm timestamp default now()
);
create index relation_source_index on dds.relation (source);
create index relation_destination_index on dds.relation (destination);
create index relation_attribute_index on dds.relation (attribute);
create index relation_loaded_by_index on dds.relation using gin (loaded_by);

create table dds.sample
(
	urn text not null,
	column_def jsonb not null,
	cnt_rows int not null,
	sample_data jsonb not null,
	processed_dttm timestamp default now()
);
create index sample_urn_index on dds.sample (urn);

create schema tuning;
create table tuning.breadcrumb
(
	urn text not null,
	breadcrumb_urn jsonb,
	breadcrumb_entity jsonb,
	loaded_by text not null,
	processed_dttm timestamp default now()
);
create index breadcrumb_urn_index on tuning.breadcrumb (urn);

create table tuning.relations_type
(
  source_type text,
  target_type text,
  attribute_type text,
  relation_type text,
  source_group_code text,
  target_group_code text,
  attribute_group_code text,
  loaded_by text,
  processed_dttm timestamp default now()
);

create table tuning.search_help
(
 type text not null,
 name text not null,
 title_code text not null,
 info_code text not null,
 loaded_by text,
 processed_dttm timestamp default now()
);

create table tuning.messages
(
 code text not null,
 lang text not null,
 text text not null,
 loaded_by text,
 processed_dttm timestamp default now()
);

create table tuning.dictionary
(
 type text not null,
 code text not null,
 message_code text not null,
 loaded_by text,
 processed_dttm timestamp default now()
);

create table tuning.search_system_x_type
(
	system_name text not null,
	type_name text not null,
    loaded_by text not null,
    processed_dttm timestamp default now()
);

create schema mart;
create table mart.entity
(
    load_dt date not null,
    urn text not null,
	loaded_by text not null,
	entity_name text not null,
	entity_type text not null,
	entity_name_short text,
	info text,
    grid jsonb,
	json_data jsonb,
	json_system jsonb,
    json_data_ui jsonb,
    codes jsonb,
	links jsonb,
    htmls jsonb,
    notifications jsonb,
    tables jsonb,
	search_data text,
	tags jsonb,
	processed_dttm timestamp default now()
);
