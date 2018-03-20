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


class ZomatoParameters:

    def getparam_city_names(self):
        """Retrieve Parameter | City Names"""
        log.debug("getparam_city_names() | <START>")
        city = ''

        # Retrieve Parameter | City Names
        db_cur_one.execute("select count(distinct CITY_NAME) from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
        for count in db_cur_one:
            if count[0] is 0:
                log.info("Parameter: CITY_NAME missing. Please define.")
            else:
                db_cur_two.execute("select distinct CITY_NAME from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
                for city_name in db_cur_two:
                    city = city_name[0]

        log.info("PARAMETER City: " + city)
        log.debug("getparam_city_names() | <END>")
        return city

    def getparam_localities(self):
        """Retrieve Parameter | Localities"""
        log.debug("getparam_localities() | <START>")
        localities = []

        # Retrieve Parameter | Localities
        db_cur_one.execute("select count(distinct LOCALITY) from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
        for count in db_cur_one:
            if count[0] is 0:
                log.info("Parameter: LOCALITY missing. Please define.")

            else:
                db_cur_two.execute("select distinct LOCALITY from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
                for locality in db_cur_two:
                    localities.append(locality[0])

        log.info("PARAMETER Locality: " + str(localities))
        log.debug("getparam_localities() | <END>")
        return localities


class ZomatoClient:

    def get_categories(self, headers):
        """Refresh Zomato Categories data"""
        log.info("get_categories() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_categories where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")
        for values in db_cur_one:
            if values[0] is 0:
                log.info("Data stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = requests.get(base_url + '/categories', params='', headers=headers).json()
                db_cur_two.execute("truncate table ZMT_CATEGORIES")

                # Loop through response and populate table
                for category in range(len(response['categories'])):
                    log.info("Adding Category: " + response['categories'][category]['categories']['name'])
                    db_cur_two.execute("insert into ZMT_CATEGORIES values (:category_id, :category_name, SYSDATE)",
                                       category_id=response['categories'][category]['categories']['id'],
                                       category_name=response['categories'][category]['categories']['name'])
                db_conn.commit()

            else:
                log.info("Data is current. Refresh skipped.")

        log.info("get_categories() | <END>")

    def get_cities(self, headers, query):
        """Refresh Zomato Cities data"""
        log.info("get_cities() | <START>")

        # Request data
        response = requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=headers).json()

        # Check if data exists. Populate table if yes, ignore response otherwise.
        db_cur_one.execute("select count(*) from ZMT_CITIES where CITY_NAME = :name", name=query)
        for values in db_cur_one:
            if values[0] is 0:
                log.info("Adding City: " + query)

                db_cur_two.execute("insert into ZMT_CITIES values (:city_id, :city_name, :country_id, :country_name, "
                                   "SYSDATE)",
                                   city_id=response['location_suggestions'][0]['id'],
                                   city_name=response['location_suggestions'][0]['name'],
                                   country_id=response['location_suggestions'][0]['country_id'],
                                   country_name=response['location_suggestions'][0]['country_name'])
                db_conn.commit()

        log.info("get_cities() | <END>")
        return str(response['location_suggestions'][0]['id'])

    def get_cuisines(self, headers, city_id):
        """Refresh Zomato Cuisines data"""
        log.info("get_cuisines() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_cuisines where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")
        for values in db_cur_one:
            if values[0] is 0:
                log.info("Data is stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = requests.get(base_url + '/cuisines?city_id=' + city_id, params='', headers=headers).json()
                db_cur_two.execute("truncate table ZMT_CUISINES")

                # Loop through response and populate table
                for cuisine in range(len(response['cuisines'])):
                    log.info("Adding Cuisine: " + response['cuisines'][cuisine]['cuisine']['cuisine_name'])

                    db_cur_two.execute("insert into ZMT_CUISINES values (:city_id, :cuisine_id, :cuisine_name, "
                                       "SYSDATE)",
                                       city_id=city_id,
                                       cuisine_id=response['cuisines'][cuisine]['cuisine']['cuisine_id'],
                                       cuisine_name=response['cuisines'][cuisine]['cuisine']['cuisine_name'])
                db_conn.commit()
            else:
                log.info("Data is current. Refresh skipped.")

        log.info("get_cuisines() | <END>")
        return 0

    def get_establishments(self, headers, city_id):
        """Refresh Zomato Establishments data"""
        log.info("get_establishments() | <START>")

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute("select COUNT(*) from zmt_establishments where TO_CHAR(INSERT_DT,'YYYY') = "
                           "TO_CHAR(SYSDATE, 'YYYY')")

        for values in db_cur_one:
            if values[0] is 0:
                log.info("Data is stale/unavailable. Refreshing...")

                # Request data and cleanup table
                response = requests.get(base_url + '/establishments?city_id=' + city_id, params='',
                                        headers=headers).json()
                db_cur_two.execute("truncate table ZMT_ESTABLISHMENTS")

                # Loop through response and populate table
                for establishment in range(len(response['establishments'])):
                    log.info("Adding Establishment: "
                             + response['establishments'][establishment]['establishment']['name'])

                    db_cur_two.execute("insert into ZMT_ESTABLISHMENTS values (:city_id, :establishment_id, "
                                       ":establishment_name, SYSDATE)",
                                       city_id=city_id,
                                       establishment_id=response['establishments'][establishment]['establishment']
                                       ['id'],
                                       establishment_name=response['establishments'][establishment]['establishment']
                                       ['name'])
                db_conn.commit()
            else:
                log.info("Data is current. Refreshing...")

        log.info("get_establishments() | <END>")
        return 0

    def get_collections(self, headers, city_id):
        """Refresh Zomato Collections data"""
        log.info("get_collections() | <START>")

        # Request data and cleanup table
        response = requests.get(base_url + '/collections?city_id=' + city_id, params='', headers=headers).json()
        db_cur_one.execute("delete from ZMT_COLLECTIONS where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                           "CITY_ID = :city_id",
                           city_id=city_id)

        # Loop through response and populate table
        for collection in range(len(response['collections'])):
            log.info("Adding Collection: " + response['collections'][collection]['collection']['title'])

            db_cur_one.execute("insert into ZMT_COLLECTIONS values (TO_CHAR(SYSDATE, 'YYYYMM'), :city_id, "
                               ":collection_id, :title, :description, :url, :share_url, :res_count, SYSDATE)",
                               city_id=city_id,
                               collection_id=response['collections'][collection]['collection']['collection_id'],
                               title=response['collections'][collection]['collection']['title'],
                               description=response['collections'][collection]['collection']['description'],
                               url=response['collections'][collection]['collection']['url'],
                               share_url=response['collections'][collection]['collection']['share_url'],
                               res_count=response['collections'][collection]['collection']['res_count'])
        db_conn.commit()
        log.info("get_collections() | <END>")
        return 0

    def get_locations(self, headers, query):
        """Refresh Zomato Locations data"""
        log.info("get_locations() | <START>")

        # Request data and cleanup table
        response = requests.get(base_url + '/locations?query=' + query + '&count=1', params='', headers=headers).json()
        db_cur_one.execute("delete from ZMT_LOCATIONS where ENTITY_ID = :entity_id ",
                           entity_id=str(response['location_suggestions'][0]['entity_id']))

        # Populate table
        log.info("Adding Location: " + response['location_suggestions'][0]['title'])

        db_cur_one.execute("insert into ZMT_LOCATIONS values (:entity_id, :entity_type, :title, :latitude, :longitude, "
                           ":city_id, :city_name, :country_id, :country_name, SYSDATE)",
                           entity_id=response['location_suggestions'][0]['entity_id'],
                           entity_type=response['location_suggestions'][0]['entity_type'],
                           title=response['location_suggestions'][0]['title'],
                           latitude=response['location_suggestions'][0]['latitude'],
                           longitude=response['location_suggestions'][0]['longitude'],
                           city_id=response['location_suggestions'][0]['city_id'],
                           city_name=response['location_suggestions'][0]['city_name'],
                           country_id=response['location_suggestions'][0]['country_id'],
                           country_name=response['location_suggestions'][0]['country_name'])
        db_conn.commit()
        log.info("get_locations() | <END>")

        return str(response['location_suggestions'][0]['entity_id']), response['location_suggestions'][0]['entity_type']

    def get_location_details(self, headers, entity_id, entity_type):
        """Refresh Zomato Location Details data"""
        log.info("get_locations_details() | <START>")

        # Request data and cleanup table
        response = requests.get(base_url + '/location_details?entity_id=' + entity_id + '&entity_type=' + entity_type,
                                params='', headers=headers).json()
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

        db_cur_one.execute("insert into ZMT_LOCATIONS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :entity_id, :popularity, "
                           ":nightlife_index, :top_cuisines, :popularity_res, :nightlife_res, :num_restaurant, "
                           "SYSDATE)",
                           entity_id=entity_id,
                           popularity=response['popularity'],
                           nightlife_index=response['nightlife_index'],
                           top_cuisines=str(response['top_cuisines']),
                           popularity_res=response['popularity_res'],
                           nightlife_res=response['nightlife_res'],
                           num_restaurant=response['num_restaurant'])
        db_conn.commit()

        log.info("get_locations_details() | <END>")
        return 0

    def get_search_bylocation(self, headers, query, entity_id, entity_type):
        """Search Zomato Restaurants by Location"""
        log.info("get_search_bylocation() | <START>")

        search_parameters = ('entity_id=' + entity_id + '&entity_type=' + entity_type + '&q=' + query)
        results_start = 0
        results_end = 100
        results_shown = 20

        # Due to API restriction, request restricted to <= 20 records
        while results_start < results_end:
            response = requests.get(base_url + '/search?' + search_parameters + '&start=' + str(results_start) +
                                    '&count=' + str(results_shown) + '&sort=rating&order=desc', params='',
                                    headers=headers).json()

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
                        log.info("Adding Restaurant: " + response['restaurants'][restaurant]['restaurant']['name']
                                 + ', ' + response['restaurants'][restaurant]['restaurant']['location']['locality'])

                        db_cur_two.execute("insert into ZMT_RESTAURANTS values (:restaurant_id, :restaurant_name, "
                                           ":url, :locality, :city_id, :latitude, :longitude, :search_parameters, "
                                           "SYSDATE)",
                                           restaurant_id=response['restaurants'][restaurant]['restaurant']['id'],
                                           restaurant_name=response['restaurants'][restaurant]['restaurant']['name'],
                                           url=response['restaurants'][restaurant]['restaurant']['url'],
                                           locality=response['restaurants'][restaurant]['restaurant']['location']
                                           ['locality'],
                                           city_id=response['restaurants'][restaurant]['restaurant']['location']
                                           ['city_id'],
                                           latitude=response['restaurants'][restaurant]['restaurant']['location']
                                           ['latitude'],
                                           longitude=response['restaurants'][restaurant]['restaurant']['location']
                                           ['longitude'],
                                           search_parameters=search_parameters)

                # Cleanup current month's data, if any
                db_cur_one.execute("""delete from ZMT_RESTAURANTS_EXT 
                                            where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') 
                                              and RESTAURANT_ID = :restaurant_id""",
                                   restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])

                # Populate table
                db_cur_one.execute("insert into ZMT_RESTAURANTS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), "
                                   ":restaurant_id, :cuisines, :average_cost_for_two, :user_rating_aggregate, "
                                   ":user_rating_text, :user_rating_votes, :has_online_delivery, :has_table_booking, "
                                   "SYSDATE)",
                                   restaurant_id=response['restaurants'][restaurant]['restaurant']['id'],
                                   cuisines=response['restaurants'][restaurant]['restaurant']['cuisines'],
                                   average_cost_for_two=response['restaurants'][restaurant]['restaurant']
                                   ['average_cost_for_two'],
                                   user_rating_aggregate=response['restaurants'][restaurant]['restaurant']
                                   ['user_rating']['aggregate_rating'],
                                   user_rating_text=response['restaurants'][restaurant]['restaurant']['user_rating']
                                   ['rating_text'],
                                   user_rating_votes=response['restaurants'][restaurant]['restaurant']['user_rating']
                                   ['votes'],
                                   has_online_delivery=response['restaurants'][restaurant]['restaurant']
                                   ['has_online_delivery'],
                                   has_table_booking=response['restaurants'][restaurant]['restaurant']
                                   ['has_table_booking'])
            results_start = results_start + 20

            # Determine request limit
            if results_end - results_start < 20:
                results_shown = results_end - results_start
        db_conn.commit()

        log.info("get_search_bylocation() | <END>")
        return 0

    def get_search_bycollection(self, headers, query):
        """Search Zomato Restaurants by Collections"""
        log.info("get_search_bycollection() | <START>")

        # Cleanup current month's data, if any
        db_cur_one.execute("delete from ZMT_COLLECTIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')")

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
                response = requests.get(base_url + '/search?' + search_parameters + '&start=' + str(results_start)
                                        + '&count=' + str(results_shown) + '&sort=rating&order=desc', params='',
                                        headers=headers).json()

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

                    db_cur_one.execute("insert into ZMT_COLLECTIONS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :city_id, "
                                       ":collection_id, :restaurant_id, :search_parameters, SYSDATE)",
                                       city_id=response['restaurants'][restaurant]['restaurant']['location']['city_id'],
                                       collection_id=collection_id,
                                       restaurant_id=response['restaurants'][restaurant]['restaurant']['id'],
                                       search_parameters=search_parameters)
                results_start = results_start + 20

                # Determine request limit
                if results_end - results_start < 20:
                    results_shown = results_end - results_start
        db_conn.commit()

        log.info("get_search_bycollection() | <END>")
        return 0

    def get_restaurant_bycollection(self, headers):
        """Retrieve Zomato Restaurants data for Collections"""
        log.info("get_restaurant_bycollection() | <START>")

        # Determine Restaurants for which data is not available
        db_cur_one.execute("""select distinct RESTAURANT_ID 
                            from ZMT_COLLECTIONS_EXT 
                           where RESTAURANT_ID not in (select distinct RESTAURANT_ID from ZMT_RESTAURANTS)
                        order by RESTAURANT_ID""")

        # Loop through Restaurant list, request data and populate tables
        for values in db_cur_one:
            res_id = values[0]
            search_parameters = ('res_id=' + str(res_id))
            response = requests.get(base_url + '/restaurant?' + search_parameters, params='', headers=headers).json()

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

            log.info("Adding Restaurant: " + response['name'] + ', ' + response['location']['locality'])
            db_cur_two.execute("insert into ZMT_RESTAURANTS values (:restaurant_id, :restaurant_name, :url, "
                               ":locality, :city_id, :latitude, :longitude, :search_parameters, SYSDATE)",
                               restaurant_id=str(response['id']),
                               restaurant_name=response['name'],
                               url=response['url'],
                               locality=response['location']['locality'],
                               city_id=str(response['location']['city_id']),
                               latitude=str(response['location']['latitude']),
                               longitude=str(response['location']['longitude']),
                               search_parameters=search_parameters)
            db_cur_two.execute("insert into ZMT_RESTAURANTS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :restaurant_id, "
                               ":cuisines, :average_cost_for_two, :user_rating_aggregate, :user_rating_text, "
                               ":user_rating_votes, :has_online_delivery, :has_table_booking, SYSDATE)",
                               restaurant_id=str(response['id']),
                               cuisines=response['cuisines'],
                               average_cost_for_two=str(response['average_cost_for_two']),
                               user_rating_aggregate=str(response['user_rating']['aggregate_rating']),
                               user_rating_text=response['user_rating']['rating_text'],
                               user_rating_votes=str(response['user_rating']['votes']),
                               has_online_delivery=str(response['has_online_delivery']),
                               has_table_booking=str(response['has_table_booking']))
            db_conn.commit()

        log.info("get_restaurant_bycollection() | <END>")
        return 0


class ZomatoAlerts:

    def compose_alert(self, locality):
        """Compose Alert"""
        log.debug("compose_alert() " + locality + " | <START>")
        alert_body = ""

        # Retrieve Parameter | City Names
        db_cur_one.execute("select ZR.LOC_LOCALITY, ZR.RESTAURANT_NAME, ZR_EXT.USER_RATING_AGGREGATE, "
                           "       ZR_EXT.AVERAGE_COST_FOR_TWO, ZR_EXT.CUISINES, ZR.URL"
                           "  from ZMT_RESTAURANTS ZR, ZMT_RESTAURANTS_EXT ZR_EXT"
                           " where ZR.RESTAURANT_ID = ZR_EXT.RESTAURANT_ID"
                           "   and TO_CHAR(ZR.INSERT_DT, 'YYYYMM') = TO_CHAR(SYSDATE, 'YYYYMM')"
                           "   and ZR_EXT.PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')"
                           "   and ZR.LOC_LOCALITY like :locality", locality=locality)
        for values in db_cur_one:
            res_locality = values[0]
            res_name = values[1]
            res_user_rating = values[2]
            res_cost_for_two = values[3]
            res_cuisines = values[4]
            res_url = values[5]
            alert_body += '<tr>' \
                          + '<td>' + res_locality + '</td>' \
                          + '<td>' + '<a href=' + res_url + '>' + res_name + '</a>' + '</td>' \
                          + '<td>' + str(res_user_rating) + '</td>' \
                          + '<td>' + str(res_cost_for_two) + '</td>' \
                          + '<td>' + res_cuisines + '</td>' \
                          + '</tr>'

        alert_body += '</table></body>'

        log.debug("compose_alert() " + locality + " | <END>")
        return alert_body

    def send_alert(self, api_key, alert_body, locality):
        """Send Alert"""
        log.debug("send_alert() " + locality + " | <START>")

        alert_header = "<head>" \
                       "  <style>" \
                       "    table {font-family: arial, sans-serif; border-collapse: collapse; width: 100%; } " \
                       "    td, th {border: 1px solid #dddddd; text-align:  left; padding: 8px; } " \
                       "    tr:nth-child(even) {background-color: #dddddd; } " \
                       "  </style>" \
                       "</head>" \
                       "<body>" \
                       "  <table style='width:100%'>" \
                       "    <tr>" \
                       "      <th>Locality</th>" \
                       "      <th>Restaurant Name</th>" \
                       "      <th>Rating</th>" \
                       "      <th>Cost For Two</th>" \
                       "      <th>Cuisines</th>" \
                       "    </tr>"

        requests.post(
            "https://api.mailgun.net/v3/sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org/messages",
            auth=("api", api_key),
            data={"from": "Mailgun Sandbox <postmaster@sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org>",
                  "to": "Nitin Pai <pai.nitin+mailgun@gmail.com>",
                  "subject": "Zomato Alert | " + locality,
                  "html": alert_header + alert_body})

        log.debug("send_alert() " + locality + " | <END>")

        return 0
