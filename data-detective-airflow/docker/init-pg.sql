SET search_path TO public;

-- test_pg_dump
CREATE TABLE dummy_test_pg_dump (
    id integer,
    name character varying(255)
);
INSERT INTO dummy_test_pg_dump (id,name) VALUES (1, 'dummy1');
INSERT INTO dummy_test_pg_dump (id,name) VALUES (2, 'dummy2');
INSERT INTO dummy_test_pg_dump (id,name) VALUES (3, 'dummy3');
INSERT INTO dummy_test_pg_dump (id,name) VALUES (4, 'dummy4');
INSERT INTO dummy_test_pg_dump (id,name) VALUES (5, 'dummy5');

create table test1 (
    test integer,
    test1 integer
);
INSERT INTO test1 (test,test1) VALUES (1, 1);


create table test2 (
    test integer,
    test1 integer
);

create table test_table (
    c1 text,
    c2 text,
    c3 text
);

create table test_processed_dttm (
    test integer,
    test1 integer,
    processed_dttm timestamp without time zone default now()
);


-- catalog
create schema dds;
create table dds.entity
(
    urn text not null,
	entity_name text not null,
	loaded_by text not null,
	entity_type text not null,
	json_data jsonb,
	json_system jsonb,
    codes jsonb,
    htmls jsonb,
    tables jsonb,
    notifications jsonb,
    grid jsonb,
    json_data_ui jsonb,
	entity_name_short text,
	search_data text,
	links jsonb,
	info text,
	processed_dttm timestamp default now()
);

create table dds.relation
(
	source text default 'non',
	destination text default 'non',
	type text not null,
	loaded_by text not null,
	attribute text default 'non',
	processed_dttm timestamp default now()
);

create table tuning.breadcrumb
(
	urn text not null,
	breadcrumb_urn jsonb,
	breadcrumb_entity jsonb,
	loaded_by text not null,
	processed_dttm timestamp default now()
);
