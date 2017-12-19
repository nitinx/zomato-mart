--DROP Tables
drop table zmt_categories;
drop table zmt_cities;
drop table zmt_collections;
drop table zmt_collections_ext;
drop table zmt_cuisines;
drop table zmt_establishments;
drop table zmt_locations;
drop table zmt_locations_ext;
drop table zmt_restaurants;
drop table zmt_restaurants_ext;


--CREATE Tables
create table zmt_categories
(
category_id integer not null,
category_name varchar2(100),
insert_dt date
);

create table zmt_cities
(
city_id integer not null,
city_name varchar2(50),
country_id integer,
country_name varchar2(50),
insert_dt date
);

create table zmt_collections
(
period integer not null,
city_id integer not null,
collection_id integer not null,
title varchar2(200),
description varchar2(500),
url varchar2(250),
share_url varchar2(100),
restaurant_count integer,
insert_dt date
);

create table zmt_collections_ext
(
period integer not null,
city_id integer not null,
collection_id integer not null,
restaurant_id integer not null,
search_parameters varchar2(200),
insert_dt date
);

create table zmt_cuisines
(
city_id integer not null,
cuisine_id integer not null,
cuisine_name varchar2(100),
insert_dt date
);

create table zmt_establishments
(
city_id integer not null,
establishment_id integer not null,
establishment_name varchar2(100),
insert_dt date
);

create table zmt_locations
(
entity_id integer not null,
entity_type varchar2(50),
title varchar2(100),
latitude varchar2(20),
longitude varchar2(20),
city_id integer,
city_name varchar2(50),
country_id integer,
country_name varchar2(50),
insert_dt date
);

create table zmt_locations_ext
(
period integer not null,
entity_id integer not null,
popularity varchar2(5),
nightlife_index varchar2(5),
top_cuisines varchar2(100),
popularity_res integer,
nightlife_res integer,
num_restaurant integer,
insert_dt date
);

create table zmt_restaurants
(
restaurant_id integer not null,
restaurant_name varchar2(100),
url varchar2(250),
loc_locality varchar2(50),
loc_city_id integer,
loc_latitude varchar2(20),
loc_longitude varchar2(20),
search_parameters varchar2(200),
insert_dt date
);

create table zmt_restaurants_ext
(
period integer not null,
restaurant_id integer not null,
cuisines varchar2(250),
average_cost_for_two integer,
user_rating_aggregate varchar2(5),
user_rating_text varchar2(20),
user_rating_votes integer,
has_online_delivery integer,
has_table_booking integer,
insert_dt date
);

select * from zmt_categories;
select * from zmt_cities;
select * from zmt_collections;
select * from zmt_collections_ext;
select * from zmt_cuisines;
select * from zmt_establishments;
select * from zmt_locations;
select * from zmt_locations_ext;
select * from zmt_restaurants;
select * from zmt_restaurants_ext;

