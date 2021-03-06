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
from nxcommon import NXKey
#from nxcommon import NXOracle
from nxcommon import NXmysql
from time import gmtime, strftime

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

# Define Oracle Variables
db_conn = NXmysql().db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()


def get_user_key():
    """Get the Zomato API Key"""
    return NXKey().key_zomato()[0]['API_KEY']


def get_categories(headers):
    """Refresh Zomato Categories data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] <START>")

    # Check if data exists / is stale (> 1 month)
    db_cur_one.execute("select COUNT(*) from ZMT_CATEGORIES where DATE_FORMAT(INSERT_DT,'%Y') "
                       "= DATE_FORMAT(CURRENT_DATE(), '%Y')")
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Data stale/unavailable. "
                                                            "Refreshing...")

            # Request data and cleanup table
            response = requests.get(base_url + '/categories', params='', headers=headers).json()
            db_cur_two.execute("truncate table ZMT_CATEGORIES")

            # Loop through response and populate table
            for category in range(len(response['categories'])):
                print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Adding Category: "
                      + response['categories'][category]['categories']['name'])
                db_cur_two.execute("insert into ZMT_CATEGORIES(category_id, category_name, insert_dt) values (%s, %s, "
                                   "CURRENT_DATE())",
                                   (response['categories'][category]['categories']['id'],
                                    response['categories'][category]['categories']['name']))
            db_conn.commit()

        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Data is current. Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] <END>")


def get_cities(headers, query):
    """Refresh Zomato Cities data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <START>")

    # Request data
    response = requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=headers).json()

    # Check if data exists. Populate table if yes, ignore response otherwise.
    db_cur_one.execute("select count(*) from ZMT_CITIES where city_name = '%s'" % (query))
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] Adding City: " + query)
            db_cur_two.execute("insert into ZMT_CITIES(city_id, city_name, country_id, country_name, insert_dt) values "
                               "(%s, %s, %s, %s, CURRENT_DATE()) ",
                               (response['location_suggestions'][0]['id'],
                                response['location_suggestions'][0]['name'],
                                response['location_suggestions'][0]['country_id'],
                                response['location_suggestions'][0]['country_name']))
            db_conn.commit()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <END>")
    return str(response['location_suggestions'][0]['id'])


def get_cuisines(headers, city_id):
    """Refresh Zomato Cuisines data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <START>")

    # Check if data exists / is stale (> 1 month)
    db_cur_one.execute("select COUNT(*) from ZMT_CUISINES where DATE_FORMAT(INSERT_DT,'%Y') "
                       "= DATE_FORMAT(CURRENT_DATE(), '%Y')")
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
                db_cur_two.execute("insert into ZMT_CUISINES(city_id, cuisine_id, cuisine_name, insert_dt) values "
                                   "(%s, %s, %s, CURRENT_DATE())",
                                   (city_id,
                                    response['cuisines'][cuisine]['cuisine']['cuisine_id'],
                                    response['cuisines'][cuisine]['cuisine']['cuisine_name']))
            db_conn.commit()
        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] Data is current. Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <END>")
    return 0


def get_establishments(headers, city_id):
    """Refresh Zomato Establishments data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] <START>")

    # Check if data exists / is stale (> 1 month)
    db_cur_one.execute("select COUNT(*) from ZMT_ESTABLISHMENTS where DATE_FORMAT(INSERT_DT,'%Y') = "
                       "DATE_FORMAT(CURRENT_DATE(), '%Y')")

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
                db_cur_two.execute("insert into ZMT_ESTABLISHMENTS(city_id, establishment_id, establishment_name, "
                                   "insert_dt) values(%s, %s, %s, CURRENT_DATE())",
                                   (city_id,
                                    response['establishments'][establishment]['establishment']['id'],
                                    response['establishments'][establishment]['establishment']['name']))
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
    db_cur_one.execute("delete from ZMT_COLLECTIONS where PERIOD = DATE_FORMAT(CURRENT_DATE(), '%%Y%%m') and "
                       "CITY_ID = %s" % city_id)

    # Loop through response and populate table
    for collection in range(len(response['collections'])):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] Adding Collection: "
              + response['collections'][collection]['collection']['title'])
        db_cur_one.execute("insert into ZMT_COLLECTIONS(period, city_id, collection_id, title, description, url, "
                           "share_url, restaurant_count, insert_dt) values(DATE_FORMAT(CURRENT_DATE(), '%%Y%%m'), %s, "
                           "%s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                           (city_id,
                            response['collections'][collection]['collection']['collection_id'],
                            response['collections'][collection]['collection']['title'],
                            response['collections'][collection]['collection']['description'],
                            response['collections'][collection]['collection']['url'],
                            response['collections'][collection]['collection']['share_url'],
                            response['collections'][collection]['collection']['res_count']))
    db_conn.commit()
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] <END>")
    return 0


def get_locations(headers, query):
    """Refresh Zomato Locations data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] <START>")

    # Request data and cleanup table
    response = requests.get(base_url + '/locations?query=' + query + '&count=1', params='', headers=headers).json()
    db_cur_one.execute("delete from ZMT_LOCATIONS where ENTITY_ID = %s" %
                       str(response['location_suggestions'][0]['entity_id']))

    # Populate table
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] Adding Location: "
          + response['location_suggestions'][0]['title'])
    db_cur_one.execute("insert into ZMT_LOCATIONS(entity_id, entity_type, title, latitude, longitude, city_id, "
                       "city_name, country_id, country_name, insert_dt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, "
                       "CURRENT_DATE())",
                       (response['location_suggestions'][0]['entity_id'],
                        response['location_suggestions'][0]['entity_type'],
                        response['location_suggestions'][0]['title'],
                        response['location_suggestions'][0]['latitude'],
                        response['location_suggestions'][0]['longitude'],
                        response['location_suggestions'][0]['city_id'],
                        response['location_suggestions'][0]['city_name'],
                        response['location_suggestions'][0]['country_id'],
                        response['location_suggestions'][0]['country_name']))
    db_conn.commit()
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] <END>")

    return str(response['location_suggestions'][0]['entity_id']), response['location_suggestions'][0]['entity_type']


def get_location_details(headers, entity_id, entity_type, debug_mode):
    """Refresh Zomato Location Details data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_location_details()] <START>")

    # Request data and cleanup table
    response = requests.get(base_url + '/location_details?entity_id=' + entity_id + '&entity_type=' + entity_type,
                            params='', headers=headers).json()
    db_cur_one.execute("delete from ZMT_LOCATIONS_EXT where period = DATE_FORMAT(CURRENT_DATE(), '%%Y%%m') and "
                       "entity_id = %s" % (entity_id))

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
    db_cur_one.execute("insert into ZMT_LOCATIONS_EXT(period, entity_id, popularity, nightlife_index, top_cuisines, "
                       "popularity_res, nightlife_res, num_restaurant, insert_dt) values (DATE_FORMAT(CURRENT_DATE(), "
                       "'%%Y%%m'), %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                       (entity_id,
                        response['popularity'],
                        response['nightlife_index'],
                        str(response['top_cuisines']),
                        response['popularity_res'],
                        response['nightlife_res'],
                        response['num_restaurant']))
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
            db_cur_one.execute("select count(*) from ZMT_RESTAURANTS where restaurant_id = %s" %
                               (response['restaurants'][restaurant]['restaurant']['id']))
            for values in db_cur_one:
                if values[0] is 0:
                    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] Adding Restaurant: "
                          + response['restaurants'][restaurant]['restaurant']['name'] + ', '
                          + response['restaurants'][restaurant]['restaurant']['location']['locality'])
                    db_cur_two.execute("insert into ZMT_RESTAURANTS(restaurant_id, restaurant_name, url, loc_locality, "
                                       "loc_city_id, loc_latitude, loc_longitude, search_parameters, insert_dt) values "
                                       "(%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                                       (response['restaurants'][restaurant]['restaurant']['id'],
                                        response['restaurants'][restaurant]['restaurant']['name'],
                                        response['restaurants'][restaurant]['restaurant']['url'],
                                        response['restaurants'][restaurant]['restaurant']['location']['locality'],
                                        response['restaurants'][restaurant]['restaurant']['location']['city_id'],
                                        response['restaurants'][restaurant]['restaurant']['location']['latitude'],
                                        response['restaurants'][restaurant]['restaurant']['location']['longitude'],
                                        search_parameters))

            # Cleanup current month's data, if any
            db_cur_one.execute("""delete from ZMT_RESTAURANTS_EXT 
                                        where PERIOD = DATE_FORMAT(CURRENT_DATE(), '%%Y%%m') 
                                          and RESTAURANT_ID = %s""" %
                               (response['restaurants'][restaurant]['restaurant']['id']))

            # Populate table
            db_cur_one.execute("insert into ZMT_RESTAURANTS_EXT(period, restaurant_id, cuisines, average_cost_for_two, "
                               "user_rating_aggregate, user_rating_text, user_rating_votes, has_online_delivery, "
                               "has_table_booking, insert_dt) values (DATE_FORMAT(CURRENT_DATE(), '%%Y%%m'), %s, %s, "
                               "%s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                               (response['restaurants'][restaurant]['restaurant']['id'],
                                response['restaurants'][restaurant]['restaurant']['cuisines'],
                                response['restaurants'][restaurant]['restaurant']['average_cost_for_two'],
                                response['restaurants'][restaurant]['restaurant']['user_rating']['aggregate_rating'],
                                response['restaurants'][restaurant]['restaurant']['user_rating']['rating_text'],
                                response['restaurants'][restaurant]['restaurant']['user_rating']['votes'],
                                response['restaurants'][restaurant]['restaurant']['has_online_delivery'],
                                response['restaurants'][restaurant]['restaurant']['has_table_booking']))
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
    db_cur_one.execute("delete from ZMT_COLLECTIONS_EXT where PERIOD = DATE_FORMAT(CURRENT_DATE(), '%%Y%%m')")

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
                db_cur_one.execute("insert into ZMT_COLLECTIONS_EXT(period, city_id, collection_id, restaurant_id, "
                                   "search_parameters, insert_dt) values (DATE_FORMAT(CURRENT_DATE(), '%%Y%%m'), "
                                   "%s, %s, %s, %s, CURRENT_DATE())",
                                   (response['restaurants'][restaurant]['restaurant']['location']['city_id'],
                                    collection_id,
                                    response['restaurants'][restaurant]['restaurant']['id'],
                                    search_parameters))
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
        db_cur_two.execute("insert into ZMT_RESTAURANTS(restaurant_id, restaurant_name, url, loc_locality, "
                           "loc_city_id, loc_latitude, loc_longitude, search_parameters, insert_dt) values (%s, %s, "
                           "%s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                           (response['id'],
                            response['name'],
                            response['url'],
                            response['location']['locality'],
                            response['location']['city_id'],
                            str(response['location']['latitude']),
                            str(response['location']['longitude']),
                            search_parameters))
        db_cur_two.execute("insert into ZMT_RESTAURANTS_EXT(period, "
                           "restaurant_id, cuisines, average_cost_for_two, user_rating_aggregate, user_rating_text, "
                           "user_rating_votes, has_online_delivery, has_table_booking, insert_dt) values "
                           "(DATE_FORMAT(CURRENT_DATE(), '%%Y%%m'), %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE())",
                           (response['id'],
                            response['cuisines'],
                            response['average_cost_for_two'],
                            response['user_rating']['aggregate_rating'],
                            response['user_rating']['rating_text'],
                            response['user_rating']['votes'],
                            response['has_online_delivery'],
                            response['has_table_booking']))
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
