/*DROP Tables*/
drop table ZMT_CATEGORIES;
drop table ZMT_CITIES;
drop table ZMT_CUISINES;
drop table ZMT_ESTABLISHMENTS;
drop table ZMT_LOCATIONS;
drop table ZMT_LOCATIONS_EXT;
drop table ZMT_COLLECTIONS;
drop table ZMT_COLLECTIONS_EXT;
drop table ZMT_RESTAURANTS;
drop table ZMT_RESTAURANTS_EXT;
drop table ZMT_PARAMETERS;

/*CREATE Tables*/
create table ZMT_CATEGORIES
(
category_id integer not null,
category_name varchar(100),
insert_dt date
);

create table ZMT_CITIES
(
city_id integer not null,
city_name varchar(50),
country_id integer,
country_name varchar(50),
insert_dt date
);

create table ZMT_CUISINES
(
city_id integer not null,
cuisine_id integer not null,
cuisine_name varchar(100),
insert_dt date
);

create table ZMT_ESTABLISHMENTS
(
city_id integer not null,
establishment_id integer not null,
establishment_name varchar(100),
insert_dt date
);

create table ZMT_LOCATIONS
(
entity_id integer not null,
entity_type varchar(50),
title varchar(100),
latitude varchar(20),
longitude varchar(20),
city_id integer,
city_name varchar(50),
country_id integer,
country_name varchar(50),
insert_dt date
);

create table ZMT_LOCATIONS_EXT
(
period integer not null,
entity_id integer not null,
popularity varchar(5),
nightlife_index varchar(5),
top_cuisines varchar(100),
popularity_res integer,
nightlife_res integer,
num_restaurant integer,
insert_dt date
);

create table ZMT_COLLECTIONS
(
period integer not null,
city_id integer not null,
collection_id integer not null,
title varchar(200),
description varchar(500),
url varchar(250),
share_url varchar(100),
restaurant_count integer,
insert_dt date
);

create table ZMT_COLLECTIONS_EXT
(
period integer not null,
city_id integer not null,
collection_id integer not null,
restaurant_id integer not null,
search_parameters varchar(200),
insert_dt date
);

create table ZMT_RESTAURANTS
(
restaurant_id integer not null,
restaurant_name varchar(100),
url varchar(250),
loc_locality varchar(50),
loc_city_id integer,
loc_latitude varchar(20),
loc_longitude varchar(20),
search_parameters varchar(200),
insert_dt date
);

create table ZMT_RESTAURANTS_EXT
(
period integer not null,
restaurant_id integer not null,
cuisines varchar(250),
average_cost_for_two integer,
user_rating_aggregate varchar(5),
user_rating_text varchar(20),
user_rating_votes integer,
has_online_delivery integer,
has_table_booking integer,
insert_dt date
);

create table ZMT_PARAMETERS
(
city_name varchar(50),
locality varchar(50),
active_flag varchar(1),
insert_dt date
);

select * from ZMT_CATEGORIES;
select * from ZMT_CITIES;
select * from ZMT_CUISINES;
select * from ZMT_ESTABLISHMENTS;
select * from ZMT_LOCATIONS;
select * from ZMT_LOCATIONS_EXT;
select * from ZMT_COLLECTIONS;
select * from ZMT_COLLECTIONS_EXT;
select * from ZMT_RESTAURANTS;
select * from ZMT_RESTAURANTS_EXT;
select * from ZMT_PARAMETERS;

