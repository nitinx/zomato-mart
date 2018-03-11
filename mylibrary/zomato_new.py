# 28 Nov 2017 | Zomato Data Mart

"""Zomato Datamart
Program that:
 1. Fetches data from Zomato.com via Zomato's public APIs
 2. Populates the data into the Zomato datamart
 3. Maintains history at a monthly time grain
 4. Fetch is currently restricted via parameters

 API Documentation: https://developers.zomato.com/api#headline1
"""

import requests
import json
import logging
#from mylibrary.nxcommon import NXKey
from mylibrary.nxcommon import NXOracle
from mylibrary.http import BaseHTTPClient
from time import gmtime, strftime

log = logging.getLogger(__name__)

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

# Define Oracle Variables
db_conn = NXOracle().db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()


class ZomatoClient(BaseHTTPClient):

    def __init__(self, user_key):
        super(ZomatoClient, self).__init__(
            "https://developers.zomato.com/api/v2.1",
            headers={"user-key": user_key}
        )

    def get_categories(self):
        return self.get("/categories")

    def get_cities(self, query):
        return self.get("/cities")

    def get_cuisines(self):
        return self.get("/cuisines")

    def get_establishments(self):
        return self.get("/establishments")

    def get_locations(self):
        return self.get("/locations")

    def get_location_details(self):
        return self.get("/location_details")

    def get_search(self):
        return self.get("/search")

    def get_collections(self):
        return self.get("/collections")

    def get_restaurant(self):
        return self.get("/restaurant")


class SuperiorZomatoClient(ZomatoClient):

    def __init__(self, *args, **kwargs):
        super(SuperiorZomatoClient, self).__init__(*args, **kwargs)

    def get_categories(self):
        """Refresh Zomato Categories data"""
        log.info("get_categories() | <START>")

        log.debug("Calling get_categories method.")
        refined = {}

        # Check if data exists / is stale (> 1 month)
        db_cur_one.execute(
            "select COUNT(*) from zmt_categories where TO_CHAR(INSERT_DT,'YYYY') = TO_CHAR(SYSDATE, 'YYYY')")
        for values in db_cur_one:
            if values[0] is 0:
                log.info("Data stale/unavailable. Refreshing...")

                # Cleanup table and request data
                db_cur_two.execute("truncate table ZMT_CATEGORIES")

                response = super(SuperiorZomatoClient, self).get_categories()
                for cat in response["categories"]:
                    _id = cat["categories"]["id"]
                    _name = cat["categories"]["name"]
                    refined[_id] = _name

                    log.info("Adding Category: " + cat["categories"]["name"])

                    db_cur_two.execute("insert into ZMT_CATEGORIES values (:category_id, :category_name, SYSDATE)",
                                       category_id=cat["categories"]["id"],
                                       category_name=cat["categories"]["name"])
                db_conn.commit()

            else:
                log.info("Data is current. Refresh skipped.")

        log.info("get_categories() | <END>")
        return refined

    def get_cities(self, query):
        """Refresh Zomato Cities data"""
        log.info("get_cities() | <START>")

        # Request data
        response = super(SuperiorZomatoClient, self).get_cities(query)
        print(response)
        print(response['location_suggestions']['name'])

        #response = requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=headers).json()

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

'''
def get_cities(headers, query):
    """Refresh Zomato Cities data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <START>")

    # Request data
    response = requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=headers).json()

    # Check if data exists. Populate table if yes, ignore response otherwise.
    db_cur_one.execute("select count(*) from ZMT_CITIES where CITY_NAME = :name", name=query)
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] Adding City: " + query)
            db_cur_two.execute("insert into ZMT_CITIES values (:city_id, :city_name, :country_id, :country_name, "
                               "SYSDATE)",
                               city_id=response['location_suggestions'][0]['id'],
                               city_name=response['location_suggestions'][0]['name'],
                               country_id=response['location_suggestions'][0]['country_id'],
                               country_name=response['location_suggestions'][0]['country_name'])
            db_conn.commit()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <END>")
    return str(response['location_suggestions'][0]['id'])


def get_cuisines(headers, city_id):
    """Refresh Zomato Cuisines data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <START>")

    # Check if data exists / is stale (> 1 month)
    db_cur_one.execute("select COUNT(*) from zmt_cuisines where TO_CHAR(INSERT_DT,'YYYY') = TO_CHAR(SYSDATE, 'YYYY')")
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] Data is stale/unavailable. "
                                                            "Refreshing...")

            # Request data and cleanup table
            response = requests.get(base_url + '/cuisines?city_id=' + city_id, params='', headers=headers).json()
            db_cur_two.execute("truncate table ZMT_CUISINES")

            # Loop through response and populate table
            for cuisine in range(len(response['cuisines'])):
                print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Adding Cuisine: "
                      + response['cuisines'][cuisine]['cuisine']['cuisine_name'])
                db_cur_two.execute("insert into ZMT_CUISINES values (:city_id, :cuisine_id, :cuisine_name, SYSDATE)",
                                   city_id=city_id,
                                   cuisine_id=response['cuisines'][cuisine]['cuisine']['cuisine_id'],
                                   cuisine_name=response['cuisines'][cuisine]['cuisine']['cuisine_name'])
            db_conn.commit()
        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] Data is current. Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <END>")
    return 0


def get_establishments(headers, city_id):
    """Refresh Zomato Establishments data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] <START>")

    # Check if data exists / is stale (> 1 month)
    db_cur_one.execute("select COUNT(*) from zmt_establishments where TO_CHAR(INSERT_DT,'YYYY') = "
                       "TO_CHAR(SYSDATE, 'YYYY')")

    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] Data is stale/unavailable. "
                                                            "Refreshing...")

            # Request data and cleanup table
            response = requests.get(base_url + '/establishments?city_id=' + city_id, params='', headers=headers).json()
            db_cur_two.execute("truncate table ZMT_ESTABLISHMENTS")

            # Loop through response and populate table
            for establishment in range(len(response['establishments'])):
                print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] Adding Establishment: "
                      + response['establishments'][establishment]['establishment']['name'])
                db_cur_two.execute("insert into ZMT_ESTABLISHMENTS values (:city_id, :establishment_id, "
                                   ":establishment_name, SYSDATE)",
                                   city_id=city_id,
                                   establishment_id=response['establishments'][establishment]['establishment']['id'],
                                   establishment_name=response['establishments'][establishment]['establishment'][
                                       'name'])
            db_conn.commit()
        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] Data is current. "
                                                            "Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] <END>")
    return 0


def get_collections(headers, city_id):
    """Refresh Zomato Collections data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] <START>")

    # Request data and cleanup table
    response = requests.get(base_url + '/collections?city_id=' + city_id, params='', headers=headers).json()
    db_cur_one.execute("delete from ZMT_COLLECTIONS where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and CITY_ID = :city_id",
                       city_id=city_id)

    # Loop through response and populate table
    for collection in range(len(response['collections'])):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] Adding Collection: "
              + response['collections'][collection]['collection']['title'])
        db_cur_one.execute("insert into ZMT_COLLECTIONS values (TO_CHAR(SYSDATE, 'YYYYMM'), :city_id, :collection_id, "
                           ":title, :description, :url, :share_url, :res_count, SYSDATE)",
                           city_id=city_id,
                           collection_id=response['collections'][collection]['collection']['collection_id'],
                           title=response['collections'][collection]['collection']['title'],
                           description=response['collections'][collection]['collection']['description'],
                           url=response['collections'][collection]['collection']['url'],
                           share_url=response['collections'][collection]['collection']['share_url'],
                           res_count=response['collections'][collection]['collection']['res_count'])
    db_conn.commit()
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] <END>")
    return 0


def get_locations(headers, query):
    """Refresh Zomato Locations data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] <START>")

    # Request data and cleanup table
    response = requests.get(base_url + '/locations?query=' + query + '&count=1', params='', headers=headers).json()
    db_cur_one.execute("delete from ZMT_LOCATIONS where ENTITY_ID = :entity_id ",
                       entity_id=str(response['location_suggestions'][0]['entity_id']))

    # Populate table
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] Adding Location: "
          + response['location_suggestions'][0]['title'])
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
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] <END>")

    return str(response['location_suggestions'][0]['entity_id']), response['location_suggestions'][0]['entity_type']


def get_location_details(headers, entity_id, entity_type, debug_mode):
    """Refresh Zomato Location Details data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_location_details()] <START>")

    # Request data and cleanup table
    response = requests.get(base_url + '/location_details?entity_id=' + entity_id + '&entity_type=' + entity_type,
                            params='', headers=headers).json()
    db_cur_one.execute("delete from ZMT_LOCATIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                       "ENTITY_ID = :entity_id", entity_id=entity_id)

    # Populate table
    if debug_mode is 'Y':
        print(str(response['location']['entity_id'])
              + ' ' + response['location']['entity_type']
              + ' ' + str(response['popularity'])
              + ' ' + str(response['nightlife_index'])
              + ' ' + str(response['top_cuisines'])
              + ' ' + str(response['popularity_res'])
              + ' ' + str(response['nightlife_res'])
              + ' ' + str(response['num_restaurant']))
    db_cur_one.execute("insert into ZMT_LOCATIONS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :entity_id, :popularity, "
                       ":nightlife_index, :top_cuisines, :popularity_res, :nightlife_res, :num_restaurant, SYSDATE)",
                       entity_id=entity_id,
                       popularity=response['popularity'],
                       nightlife_index=response['nightlife_index'],
                       top_cuisines=str(response['top_cuisines']),
                       popularity_res=response['popularity_res'],
                       nightlife_res=response['nightlife_res'],
                       num_restaurant=response['num_restaurant'])
    db_conn.commit()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_location_details()] <END>")
    return 0


def get_search_bylocation(headers, query, entity_id, entity_type, debug_mode):
    """Search Zomato Restaurants by Location"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] <START>")

    search_parameters = ('entity_id=' + entity_id + '&entity_type=' + entity_type + '&q=' + query)
    results_start = 0
    results_end = 100
    results_shown = 20

    # Due to API restriction, request restricted to <= 20 records
    while results_start < results_end:
        response = requests.get(base_url + '/search?' + search_parameters + '&start=' + str(results_start) + '&count='
                                + str(results_shown) + '&sort=rating&order=desc', params='', headers=headers).json()

        # results_found = response['results_found']
        results_start = response['results_start']
        results_shown = response['results_shown']
        if debug_mode is 'Y':
            print("Results Start:" + str(results_start))
            print("Results Shown:" + str(results_shown))

        # Loop through response and populate table
        for restaurant in range(len(response['restaurants'])):
            if debug_mode is 'Y':
                print(str(response['restaurants'][restaurant]['restaurant']['id'])
                      + ' ' + response['restaurants'][restaurant]['restaurant']['name']
                      + ' ' + response['restaurants'][restaurant]['restaurant']['url']
                      + ' ' + response['restaurants'][restaurant]['restaurant']['location']['locality']
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['city_id'])
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['latitude'])
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['location']['longitude'])
                      + ' ' + response['restaurants'][restaurant]['restaurant']['cuisines']
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['average_cost_for_two'])
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['user_rating']['aggregate_rating'])
                      + ' ' + response['restaurants'][restaurant]['restaurant']['user_rating']['rating_text']
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['user_rating']['votes'])
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['has_online_delivery'])
                      + ' ' + str(response['restaurants'][restaurant]['restaurant']['has_table_booking']))

            # Check if Restaurant data exists. Populate table if no, ignore otherwise.
            db_cur_one.execute("select count(*) from ZMT_RESTAURANTS where RESTAURANT_ID = :restaurant_id",
                               restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])
            for values in db_cur_one:
                if values[0] is 0:
                    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] Adding Restaurant: "
                          + response['restaurants'][restaurant]['restaurant']['name'] + ', '
                          + response['restaurants'][restaurant]['restaurant']['location']['locality'])
                    db_cur_two.execute("insert into ZMT_RESTAURANTS values (:restaurant_id, :restaurant_name, :url, "
                                       ":locality, :city_id, :latitude, :longitude, :search_parameters, SYSDATE)",
                                       restaurant_id=response['restaurants'][restaurant]['restaurant']['id'],
                                       restaurant_name=response['restaurants'][restaurant]['restaurant']['name'],
                                       url=response['restaurants'][restaurant]['restaurant']['url'],
                                       locality=response['restaurants'][restaurant]['restaurant']['location'][
                                           'locality'],
                                       city_id=response['restaurants'][restaurant]['restaurant']['location']['city_id'],
                                       latitude=response['restaurants'][restaurant]['restaurant']['location'][
                                           'latitude'],
                                       longitude=response['restaurants'][restaurant]['restaurant']['location'][
                                           'longitude'],
                                       search_parameters=search_parameters)

            # Cleanup current month's data, if any
            db_cur_one.execute("""delete from ZMT_RESTAURANTS_EXT 
                                        where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') 
                                          and RESTAURANT_ID = :restaurant_id""",
                               restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])

            # Populate table
            db_cur_one.execute("insert into ZMT_RESTAURANTS_EXT values (TO_CHAR(SYSDATE, 'YYYYMM'), :restaurant_id, "
                               ":cuisines, :average_cost_for_two, :user_rating_aggregate, :user_rating_text, "
                               ":user_rating_votes, :has_online_delivery, :has_table_booking, SYSDATE)",
                               restaurant_id=response['restaurants'][restaurant]['restaurant']['id'],
                               cuisines=response['restaurants'][restaurant]['restaurant']['cuisines'],
                               average_cost_for_two=response['restaurants'][restaurant]['restaurant']
                               ['average_cost_for_two'],
                               user_rating_aggregate=response['restaurants'][restaurant]['restaurant']['user_rating']
                               ['aggregate_rating'],
                               user_rating_text=response['restaurants'][restaurant]['restaurant']['user_rating']
                               ['rating_text'],
                               user_rating_votes=response['restaurants'][restaurant]['restaurant']['user_rating'][
                                   'votes'],
                               has_online_delivery=response['restaurants'][restaurant]['restaurant'][
                                   'has_online_delivery'],
                               has_table_booking=response['restaurants'][restaurant]['restaurant']['has_table_booking'])
        results_start = results_start + 20

        # Determine request limit
        if results_end - results_start < 20:
            results_shown = results_end - results_start
    db_conn.commit()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] <END>")
    return 0


def get_search_bycollection(headers, query, debug_mode):
    """Search Zomato Restaurants by Collections"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bycollection()] <START>")

    # Cleanup current month's data, if any
    db_cur_one.execute("delete from ZMT_COLLECTIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')")

    # Loop through Collection list
    db_cur_two.execute("select distinct CITY_ID, COLLECTION_ID from ZMT_COLLECTIONS order by CITY_ID, COLLECTION_ID")
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
            if debug_mode is 'Y':
                print("Results Start:" + str(results_start))
                print("Results Shown:" + str(results_shown))

            # Loop through response and populate table
            for restaurant in range(len(response['restaurants'])):
                if debug_mode is 'Y':
                    print(str(response['restaurants'][restaurant]['restaurant']['location']['city_id'])
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

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bycollection()] <END>")
    return 0


def get_restaurant_bycollection(headers, debug_mode):
    """Retrieve Zomato Restaurants data for Collections"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collection_restaurant()] <START>")

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

        if debug_mode is 'Y':
            print(str(response['id'])
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
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] Adding Restaurant: "
              + response['name'] + ', '
              + response['location']['locality'])
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

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collection_restaurant()] <END>")
    return 0


def main():
    """Run App"""

    # Initialize variables
    headers = {'Accept': 'application/json', 'user-key': get_user_key()}
    debug_mode = 'N'
    city = ''
    localities = []

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")

    # Retrieve Parameter | City Names
    db_cur_one.execute("select count(distinct CITY_NAME) from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
    for count in db_cur_one:
        if count[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Parameter: CITY_NAME missing. Please define. ")
        else:
            db_cur_two.execute("select distinct CITY_NAME from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
            for city_name in db_cur_two:
                city = city_name[0]

    # Retrieve Parameter | Localities
    db_cur_one.execute("select count(distinct LOCALITY) from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
    for count in db_cur_one:
        if count[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Parameter: LOCALITY missing. Please define. ")
        else:
            db_cur_two.execute("select distinct LOCALITY from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
            for locality in db_cur_two:
                localities.append(locality[0])

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] PARAMETER City: " + city)
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] PARAMETER Localities: " + str(localities))
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] PARAMETER Debug Mode: " + debug_mode)

    # Fetch Category data
    get_categories(headers)

    # Fetch City data
    city_id = get_cities(headers, city)
    get_cuisines(headers, city_id)
    get_establishments(headers, city_id)

    # Fetch Location/Restaurant data
    for locality in range(len(localities)):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Processing Locality: " + localities[locality])
        entity = get_locations(headers, localities[locality])
        get_location_details(headers, entity[0], entity[1], debug_mode)
        get_search_bylocation(headers, localities[locality], entity[0], entity[1], debug_mode)

    # Fetch Collection/Restaurant data
    get_collections(headers, city_id)
    get_search_bycollection(headers, city, debug_mode)
    get_restaurant_bycollection(headers, debug_mode)

    # Close Oracle Connections
    db_cur_one.close()
    db_cur_two.close()
    db_conn.close()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")


if __name__ == '__main__':
    main()
'''