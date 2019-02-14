# 28 Nov 2017 | Zomato Client

"""Zomato Client
Library that:
 1. From database, retrieves parameters that restrict data fetched from Zomato.com
 2. Fetches data from Zomato.com via Zomato's public APIs
 3. Populates the data into the Zomato datamart

 API Documentation: https://developers.zomato.com/api#headline1
"""

import requests
import logging
import json
from mylibrary.db_oracle import OracleClient
from time import gmtime, strftime

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

# Define Oracle Variables
DB = OracleClient()
db_conn = DB.db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()

log = logging.getLogger(__name__)


class ZomatoDBInsertOracle:

    def insert_categories(self, category_id, category_name):
        """Insert into ZMT_CATEGORIES"""
        log.debug("insert_categories() | <START>")

        db_cur_two.execute("insert into ZMT_CATEGORIES values (:category_id, :category_name, SYSDATE)",
                           category_id=category_id,
                           category_name=category_name)
        db_conn.commit()

        log.debug("insert_categories() | <END>")

    def insert_cities(self, city_id, city_name, country_id, country_name):
        """Insert into ZMT_CITIES"""
        log.debug("insert_cities() | <START>")

        db_cur_two.execute("insert into ZMT_CITIES values (:city_id, :city_name, :country_id, :country_name, "
                           "SYSDATE)",
                           city_id=city_id,
                           city_name=city_name,
                           country_id=country_id,
                           country_name=country_name)
        db_conn.commit()

        log.debug("insert_cities() | <END>")

    def insert_cuisines(self, city_id, cuisine_id, cuisine_name):
        """Insert into ZMT_CUISINES"""
        log.debug("insert_cuisines() | <START>")

        db_cur_two.execute("insert into ZMT_CUISINES values (:city_id, :cuisine_id, :cuisine_name, "
                           "SYSDATE)",
                           city_id=city_id,
                           cuisine_id=cuisine_id,
                           cuisine_name=cuisine_name)
        db_conn.commit()

        log.debug("insert_cuisines() | <END>")

    def insert_establishments(self, city_id, establishment_id, establishment_name):
        """Insert into ZMT_ESTABLISHMENTS"""
        log.debug("insert_establishments() | <START>")

        db_cur_two.execute("insert into ZMT_ESTABLISHMENTS values (:city_id, :establishment_id, "
                           ":establishment_name, SYSDATE)",
                           city_id=city_id,
                           establishment_id=establishment_id,
                           establishment_name=establishment_name)
        db_conn.commit()

        log.debug("insert_establishments() | <END>")

    def insert_collections(self, city_id, collection_id, title, description, url, share_url, res_count):
        """Insert into ZMT_COLLECTIONS"""
        log.debug("insert_collections() | <START>")

        db_cur_one.execute("insert into ZMT_COLLECTIONS values (TO_CHAR(SYSDATE, 'YYYYMM'), :city_id, "
                           ":collection_id, :title, :description, :url, :share_url, :res_count, SYSDATE)",
                           city_id=city_id,
                           collection_id=collection_id,
                           title=title,
                           description=description,
                           url=url,
                           share_url=share_url,
                           res_count=res_count)
        db_conn.commit()

        log.debug("insert_collections() | <END>")

    def insert_collections_ext(self, city_id, collection_id, restaurant_id, search_parameters):
        """Insert into ZMT_COLLECTIONS_EXT"""
        log.debug("insert_collections_ext() | <START>")

        db_cur_one.execute(
            "insert into ZMT_COLLECTIONS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :city_id, "
            ":collection_id, :restaurant_id, :search_parameters, SYSDATE)",
            city_id=city_id,
            collection_id=collection_id,
            restaurant_id=restaurant_id,
            search_parameters=search_parameters)
        db_conn.commit()

        log.debug("insert_collections_ext() | <END>")

    def insert_locations(self, entity_id, entity_type, title, latitude, longitude, city_id, city_name, country_id,
                         country_name):
        """Insert into ZMT_LOCATIONS"""
        log.debug("insert_locations() | <START>")

        db_cur_one.execute("insert into ZMT_LOCATIONS values (:entity_id, :entity_type, :title, :latitude, :longitude, "
                           ":city_id, :city_name, :country_id, :country_name, SYSDATE)",
                           entity_id=entity_id,
                           entity_type=entity_type,
                           title=title,
                           latitude=latitude,
                           longitude=longitude,
                           city_id=city_id,
                           city_name=city_name,
                           country_id=country_id,
                           country_name=country_name)
        db_conn.commit()

        log.debug("insert_locations() | <END>")

    def insert_locations_ext(self, entity_id, popularity, nightlife_index, top_cuisines, popularity_res, nightlife_res,
                             num_restaurant):
        """Insert into ZMT_LOCATIONS_EXT"""
        log.debug("insert_locations_ext() | <START>")

        db_cur_one.execute("insert into ZMT_LOCATIONS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :entity_id, :popularity, "
                           ":nightlife_index, :top_cuisines, :popularity_res, :nightlife_res, :num_restaurant, "
                           "SYSDATE)",
                           entity_id=entity_id,
                           popularity=popularity,
                           nightlife_index=nightlife_index,
                           top_cuisines=top_cuisines,
                           popularity_res=popularity_res,
                           nightlife_res=nightlife_res,
                           num_restaurant=num_restaurant)
        db_conn.commit()

        log.debug("insert_locations_ext() | <END>")

    def insert_restaurants(self, restaurant_id, restaurant_name, url, locality, city_id, latitude, longitude,
                           search_parameters):
        """Insert into ZMT_RESTAURANTS"""
        log.debug("insert_restaurants() | <START>")

        db_cur_two.execute("insert into ZMT_RESTAURANTS values (:restaurant_id, :restaurant_name, "
                           ":url, :locality, :city_id, :latitude, :longitude, :search_parameters, "
                           "SYSDATE, NULL)",
                           restaurant_id=restaurant_id,
                           restaurant_name=restaurant_name,
                           url=url,
                           locality=locality,
                           city_id=city_id,
                           latitude=latitude,
                           longitude=longitude,
                           search_parameters=search_parameters)
        db_conn.commit()

        log.debug("insert_restaurants() | <END>")

    def insert_restaurants_ext(self, restaurant_id, cuisines, average_cost_for_two, user_rating_aggregate,
                               user_rating_text, user_rating_votes, has_online_delivery, has_table_booking):
        """Insert into ZMT_RESTAURANTS_EXT"""
        log.debug("insert_restaurants_ext() | <START>")

        try:
            db_cur_two.execute("insert into ZMT_RESTAURANTS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :restaurant_id, "
                               ":cuisines, :average_cost_for_two, :user_rating_aggregate, :user_rating_text, "
                               ":user_rating_votes, :has_online_delivery, :has_table_booking, SYSDATE)",
                               restaurant_id=restaurant_id,
                               cuisines=cuisines,
                               average_cost_for_two=average_cost_for_two,
                               user_rating_aggregate=user_rating_aggregate,
                               user_rating_text=user_rating_text,
                               user_rating_votes=user_rating_votes,
                               has_online_delivery=has_online_delivery,
                               has_table_booking=has_table_booking)
            db_conn.commit()
        except:
            pass

        log.debug("insert_restaurants_ext() | <END>")


class ZomatoDBUpdateOracle:

    def update_restaurants(self, restaurant_id, establishment_id):
        """Update ZMT_RESTAURANTS"""
        log.debug("update_restaurants() | <START>")

        db_cur_two.execute("update ZMT_RESTAURANTS set ESTABLISHMENT_ID = :establishment_id "
                           "where RESTAURANT_ID = :restaurant_id)",
                           restaurant_id=restaurant_id,
                           establishment_id=establishment_id)
        db_conn.commit()

        log.debug("update_restaurants() | <END>")


class ZomatoDBSelectOracle:

    def select_locality_stats(self):
        """Select ZMT_RESTAURANTS"""
        log.debug("select_locality_stats() | <START>")

        loc_analytics = []

        db_cur_one.execute("SELECT locality, period, rstrnt_cnt_oth, rstrnt_cnt_top, "
                           "       round( (rstrnt_cnt_top / (rstrnt_cnt_top + rstrnt_cnt_oth) * 100),0) rstrnt_pct_top, "
                           "       avg_cost_for_two, avg_rtng_all, top_rtng_all "
                           "  FROM (SELECT zre.period, zr.loc_locality AS locality, "
                           "               SUM(CASE "
                           "                        WHEN to_number(zre.user_rating_aggregate,'9.9') >= 4 "
                           "                        THEN 1 "
                           "                        ELSE 0 END) AS rstrnt_cnt_top, "
                           "               SUM(CASE "
                           "                        WHEN to_number(zre.user_rating_aggregate,'9.9') < 4 "
                           "                        THEN 1 "
                           "                        ELSE 0 END) AS rstrnt_cnt_oth, "
                           "               round(AVG(zre.average_cost_for_two),-2) AS avg_cost_for_two, "
                           "               round(AVG(to_number(zre.user_rating_aggregate,'9.9') ),1) AS avg_rtng_all, "
                           "               round(MAX(to_number(zre.user_rating_aggregate,'9.9') ),1) AS top_rtng_all"
                           "          FROM zmt_restaurants zr, zmt_restaurants_ext zre, zmt_parameters zp  "
                           "         WHERE zr.restaurant_id = zre.restaurant_id "
                           "           AND zr.loc_locality = zp.locality "
                           "           AND zp.active_flag = 'Y' "
                           "      GROUP BY zr.loc_locality, zre.period ) "
                           "ORDER BY locality, period")

        for row in db_cur_one:
            loc_analytics.append(row)

        log.debug("select_locality_stats() | <END>")
        return loc_analytics
