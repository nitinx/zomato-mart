# 28 Nov 2017 | Zomato Main Library

"""Zomato Client
Library that:
 1. From database, retrieves parameters that restrict data fetched from Zomato.com
 2. Fetches data from Zomato.com via Zomato's public APIs
 3. Populates the data into the Zomato datamart

 API Documentation: https://developers.zomato.com/api#headline1
"""

import logging
from mylibrary.db_oracle import OracleClient
from mylibrary.zmt_requests import ZomatoRequests
from mylibrary.zmt_db_oracle import ZomatoDBInsertOracle

# Define Oracle Variables
DB = OracleClient()
db_conn = DB.db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()

ZmtInsert = ZomatoDBInsertOracle()

log = logging.getLogger(__name__)


class ZomatoClient(object):

    def __init__(self, headers):
        self.headers = headers
        self.ZmtRequest = ZomatoRequests(self.headers)

    def get_categories(self):
        """Refresh Zomato Categories data"""
        log.debug("get_categories() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_categories where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")
        for values in db_cur_one:
            if values[0] is 0:
                log.info("get_categories() | Data stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = self.ZmtRequest.get_categories()

                db_cur_two.execute("truncate table ZMT_CATEGORIES")

                # Loop through response and populate table
                for category in range(len(response['categories'])):
                    log.info("get_categories() | Adding Category: "
                             + response['categories'][category]['categories']['name'])
                    ZmtInsert.insert_categories(response['categories'][category]['categories']['id'],
                                                response['categories'][category]['categories']['name'])

            else:
                log.info("get_categories() | Data is current. Refresh skipped.")

        log.debug("get_categories() | <END>")

    def get_cities(self, query):
        """Refresh Zomato Cities data"""
        log.debug("get_cities() | <START>")

        # Request data
        response = self.ZmtRequest.get_cities(query)

        # Check if data exists. Populate table if yes, ignore response otherwise.
        db_cur_one.execute("select count(*) from ZMT_CITIES where CITY_NAME = :name", name=query)
        for values in db_cur_one:
            if values[0] is 0:
                log.info("get_cities() | Adding City: " + query)

                ZmtInsert.insert_cities(response['location_suggestions'][0]['id'],
                                        response['location_suggestions'][0]['name'],
                                        response['location_suggestions'][0]['country_id'],
                                        response['location_suggestions'][0]['country_name'])

        log.debug("get_cities() | <END>")
        return str(response['location_suggestions'][0]['id'])

    def get_cuisines(self, city_id):
        """Refresh Zomato Cuisines data"""
        log.debug("get_cuisines() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_cuisines where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")
        for values in db_cur_one:
            if values[0] is 0:
                log.info("get_cuisines() | Data is stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = self.ZmtRequest.get_cuisiness(city_id)

                db_cur_two.execute("truncate table ZMT_CUISINES")

                # Loop through response and populate table
                for cuisine in range(len(response['cuisines'])):
                    log.info("get_cuisines() | Adding Cuisine: "
                             + response['cuisines'][cuisine]['cuisine']['cuisine_name'])
                    ZmtInsert.insert_cuisines(city_id,
                                              response['cuisines'][cuisine]['cuisine']['cuisine_id'],
                                              response['cuisines'][cuisine]['cuisine']['cuisine_name'])

            else:
                log.info("get_cuisines() | Data is current. Refresh skipped.")

        log.debug("get_cuisines() | <END>")
        return 0

    def get_establishments(self, city_id):
        """Refresh Zomato Establishments data"""
        log.debug("get_establishments() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_establishments where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")

        for values in db_cur_one:
            if values[0] is 0:
                log.info("get_establishments() | Data is stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = self.ZmtRequest.get_establishments(city_id)
                db_cur_two.execute("truncate table ZMT_ESTABLISHMENTS")

                # Loop through response and populate table
                for establishment in range(len(response['establishments'])):
                    log.info("get_establishments() | Adding Establishment: "
                             + response['establishments'][establishment]['establishment']['name'])
                    ZmtInsert.insert_establishments(city_id,
                                                    response['establishments'][establishment]['establishment']['id'],
                                                    response['establishments'][establishment]['establishment']['name'])

            else:
                log.info("get_establishments() | Data is current. Refresh skipped.")

        log.debug("get_establishments() | <END>")
        return 0

    def get_collections(self, city_id):
        """Refresh Zomato Collections data"""
        log.debug("get_collections() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from ZMT_COLLECTIONS where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                           "CITY_ID = :city_id",
                           city_id=city_id)
        for values in db_cur_one:
            if values[0] is 0:
                log.info("get_collections() | Data stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = self.ZmtRequest.get_collections(city_id)
                db_cur_one.execute("delete from ZMT_COLLECTIONS where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                                   "CITY_ID = :city_id",
                                   city_id=city_id)

                # Loop through response and populate table
                for collection in range(len(response['collections'])):
                    log.info("get_collections() | Adding Collection: "
                             + response['collections'][collection]['collection']['title'])
                    ZmtInsert.insert_collections(city_id,
                                                 response['collections'][collection]['collection']['collection_id'],
                                                 response['collections'][collection]['collection']['title'],
                                                 response['collections'][collection]['collection']['description'],
                                                 response['collections'][collection]['collection']['url'],
                                                 response['collections'][collection]['collection']['share_url'],
                                                 response['collections'][collection]['collection']['res_count'])

            else:
                log.info("get_collections() | Data is current. Refresh skipped.")

        log.debug("get_collections() | <END>")
        return 0

    def get_locations(self, query):
        """Refresh Zomato Locations data"""
        log.debug("get_locations() | <START>")

        # Request data and cleanup table
        response = self.ZmtRequest.get_locations(query)
        db_cur_one.execute("delete from ZMT_LOCATIONS where ENTITY_ID = :entity_id ",
                           entity_id=str(response['location_suggestions'][0]['entity_id']))

        # Populate table
        log.info("get_locations() | Adding Location: " + response['location_suggestions'][0]['title'])
        ZmtInsert.insert_locations(response['location_suggestions'][0]['entity_id'],
                                   response['location_suggestions'][0]['entity_type'],
                                   response['location_suggestions'][0]['title'],
                                   response['location_suggestions'][0]['latitude'],
                                   response['location_suggestions'][0]['longitude'],
                                   response['location_suggestions'][0]['city_id'],
                                   response['location_suggestions'][0]['city_name'],
                                   response['location_suggestions'][0]['country_id'],
                                   response['location_suggestions'][0]['country_name'])

        log.debug("get_locations() | <END>")

        return str(response['location_suggestions'][0]['entity_id']), response['location_suggestions'][0]['entity_type']

    def get_location_details(self, entity_id, entity_type):
        """Refresh Zomato Location Details data"""
        log.debug("get_locations_details() | <START>")

        # Request data and cleanup table
        response = self.ZmtRequest.get_location_details(entity_id, entity_type)
        db_cur_one.execute("delete from ZMT_LOCATIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                           "ENTITY_ID = :entity_id", entity_id=entity_id)

        # Populate table
        log.debug(str(response['location']['entity_id'])
                  + ' ' + response['location']['entity_type']
                  + ' ' + str(response['popularity'])
                  + ' ' + str(response['nightlife_index'])
                  + ' ' + str(response['top_cuisines'])
                  + ' ' + str(response['popularity_res'])
                  + ' ' + str(response['nightlife_res'])
                  + ' ' + str(response['num_restaurant']))
        ZmtInsert.insert_locations_ext(entity_id,
                                       response['popularity'],
                                       response['nightlife_index'],
                                       str(response['top_cuisines']),
                                       response['popularity_res'],
                                       response['nightlife_res'],
                                       response['num_restaurant'])
        log.debug("get_locations_details() | <END>")
        return 0

    def get_search_bylocation(self, query, entity_id, entity_type):
        """Search Zomato Restaurants by Location"""
        log.debug("get_search_bylocation() | <START>")

        search_parameters = ('entity_id=' + entity_id + '&entity_type=' + entity_type + '&q=' + query)
        results_start = 0
        results_end = 100
        results_shown = 20

        # Due to API restriction, request restricted to <= 20 records
        while results_start < results_end:
            response = self.ZmtRequest.get_search(search_parameters, str(results_start), str(results_shown))

            # results_found = response['results_found']
            results_start = response['results_start']
            results_shown = response['results_shown']

            log.debug("Results Start:" + str(results_start))
            log.debug("Results Shown:" + str(results_shown))

            # Loop through response and populate table
            for restaurant in range(len(response['restaurants'])):
                log.debug(str(response['restaurants'][restaurant]['restaurant']['id'])
                          + ' ' + response['restaurants'][restaurant]['restaurant']['name']
                          + ' ' + response['restaurants'][restaurant]['restaurant']['url']
                          + ' ' + response['restaurants'][restaurant]['restaurant']['location']['locality']
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['city_id'])
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['latitude'])
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['longitude'])
                          + ' ' + response['restaurants'][restaurant]['restaurant']['cuisines']
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['average_cost_for_two'])
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['user_rating']
                                      ['aggregate_rating'])
                          + ' ' + response['restaurants'][restaurant]['restaurant']['user_rating']['rating_text']
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['user_rating']['votes'])
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['has_online_delivery'])
                          + ' ' + str(response['restaurants'][restaurant]['restaurant']['has_table_booking']))

                # Check if Restaurant data exists. Populate table if no, ignore otherwise.
                db_cur_one.execute("select count(*) from ZMT_RESTAURANTS where RESTAURANT_ID = :restaurant_id",
                                   restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])
                for values in db_cur_one:
                    if values[0] is 0:
                        log.info("get_search_bylocation() | Adding Restaurant: "
                                 + response['restaurants'][restaurant]['restaurant']['name'] + ', '
                                 + response['restaurants'][restaurant]['restaurant']['location']['locality'])
                        ZmtInsert.insert_restaurants(response['restaurants'][restaurant]['restaurant']['id'],
                                                     response['restaurants'][restaurant]['restaurant']['name'],
                                                     response['restaurants'][restaurant]['restaurant']['url'],
                                                     response['restaurants'][restaurant]['restaurant']['location']
                                                     ['locality'],
                                                     response['restaurants'][restaurant]['restaurant']['location']
                                                     ['city_id'],
                                                     response['restaurants'][restaurant]['restaurant']['location']
                                                     ['latitude'],
                                                     response['restaurants'][restaurant]['restaurant']['location']
                                                     ['longitude'],
                                                     search_parameters)

                # Cleanup current month's data, if any
                db_cur_one.execute("""delete from ZMT_RESTAURANTS_EXT 
                                            where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') 
                                              and RESTAURANT_ID = :restaurant_id""",
                                   restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])

                # Populate table
                ZmtInsert.insert_restaurants_ext(response['restaurants'][restaurant]['restaurant']['id'],
                                                 response['restaurants'][restaurant]['restaurant']['cuisines'],
                                                 response['restaurants'][restaurant]['restaurant']
                                                 ['average_cost_for_two'],
                                                 response['restaurants'][restaurant]['restaurant']['user_rating']
                                                 ['aggregate_rating'],
                                                 response['restaurants'][restaurant]['restaurant']['user_rating']
                                                 ['rating_text'],
                                                 response['restaurants'][restaurant]['restaurant']['user_rating']
                                                 ['votes'],
                                                 response['restaurants'][restaurant]['restaurant']
                                                 ['has_online_delivery'],
                                                 response['restaurants'][restaurant]['restaurant']['has_table_booking'])

            results_start = results_start + 20

            # Determine request limit
            if results_end - results_start < 20:
                results_shown = results_end - results_start

        log.debug("get_search_bylocation() | <END>")
        return 0

    def get_search_bycollection(self, query):
        """Search Zomato Restaurants by Collections"""
        log.debug("get_search_bycollection() | <START>")

        # Cleanup current month's data, if any
        # db_cur_one.execute("delete from ZMT_COLLECTIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from ZMT_COLLECTIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')")

        for count in db_cur_one:
            if count[0] is 0:
                log.info("get_search_bycollection() | Data stale/unavailable. Refreshing...")

                # Loop through Collection list
                db_cur_two.execute("select distinct CITY_ID, COLLECTION_ID from ZMT_COLLECTIONS order by CITY_ID, "
                                   "COLLECTION_ID")
                for values in db_cur_two:
                    collection_id = values[1]
                    search_parameters = ('collection_id=' + str(collection_id) + '&q=' + query)
                    results_start = 0
                    results_end = 100
                    results_shown = 20

                    # Due to API restriction, request restricted to <= 20 records
                    while results_start < results_end:
                        response = self.ZmtRequest.get_search(search_parameters, str(results_start), str(results_shown))

                        # results_found = response['results_found']
                        results_start = response['results_start']
                        results_shown = response['results_shown']

                        log.debug("Results Start:" + str(results_start))
                        log.debug("Results Shown:" + str(results_shown))

                        # Loop through response and populate table
                        for restaurant in range(len(response['restaurants'])):
                            log.debug(str(response['restaurants'][restaurant]['restaurant']['location']['city_id'])
                                      + ' ' + str(collection_id)
                                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['id']))
                            ZmtInsert.insert_collections_ext(response['restaurants'][restaurant]['restaurant']
                                                             ['location']['city_id'],
                                                             collection_id,
                                                             response['restaurants'][restaurant]['restaurant']['id'],
                                                             search_parameters)

                        results_start = results_start + 20

                        # Determine request limit
                        if results_end - results_start < 20:
                            results_shown = results_end - results_start

            else:
                log.info("get_collections_ext() | Data is current. Refresh skipped.")

        log.debug("get_search_bycollection() | <END>")
        return 0

    def get_restaurant_bycollection(self):
        """Retrieve Zomato Restaurants data for Collections"""
        log.debug("get_restaurant_bycollection() | <START>")

        # Determine Restaurants for which data is not available
        db_cur_one.execute("""select distinct RESTAURANT_ID 
                            from ZMT_COLLECTIONS_EXT 
                           where RESTAURANT_ID not in (select distinct RESTAURANT_ID from ZMT_RESTAURANTS)
                        order by RESTAURANT_ID""")

        # Loop through Restaurant list, request data and populate tables
        for values in db_cur_one:
            res_id = values[0]
            search_parameters = ('res_id=' + str(res_id))
            response = self.ZmtRequest.get_restaurant(search_parameters)

            log.debug(str(response['id'])
                      + ' ' + response['name']
                      + ' ' + response['url']
                      + ' ' + response['location']['locality']
                      + ' ' + str(response['location']['city_id'])
                      + ' ' + str(response['location']['latitude'])
                      + ' ' + str(response['location']['longitude'])
                      + ' ' + response['cuisines']
                      + ' ' + str(response['average_cost_for_two'])
                      + ' ' + str(response['user_rating']['aggregate_rating'])
                      + ' ' + response['user_rating']['rating_text']
                      + ' ' + str(response['user_rating']['votes'])
                      + ' ' + str(response['has_online_delivery'])
                      + ' ' + str(response['has_table_booking']))

            log.info("get_restaurant_bycollection() | Adding Restaurant: " + response['name'] + ', '
                     + response['location']['locality'])
            ZmtInsert.insert_restaurants_ext(str(response['id']),
                                             response['name'],
                                             response['url'],
                                             response['location']['locality'],
                                             str(response['location']['city_id']),
                                             str(response['location']['latitude']),
                                             str(response['location']['longitude']),
                                             search_parameters)
            ZmtInsert.insert_restaurants_ext(str(response['id']),
                                             response['cuisines'],
                                             str(response['average_cost_for_two']),
                                             str(response['user_rating']['aggregate_rating']),
                                             response['user_rating']['rating_text'],
                                             str(response['user_rating']['votes']),
                                             str(response['has_online_delivery']),
                                             str(response['has_table_booking']))

        log.debug("get_restaurant_bycollection() | <END>")
        return 0
