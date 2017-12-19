# 28 Nov 2017 | Zomato Data Mart

"""Zomato Datamart
Program that:
 1. Fetches data from Zomato.com via Zomato's public APIs
 2. Populates the data into the Zomato datamart
 3. Maintains history at a monthly time grain
 4. Fetch is currently restricted via parameters to Bangalore and its localities

 API Documentation: https://developers.zomato.com/api#headline1
"""

import requests
import json
from nxcommon import NXKey
from nxcommon import NXOracle
from time import gmtime, strftime

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

# Define Oracle Variables
db_conn = NXOracle().db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()


def get_user_key():
    """Get the Zomato API Key"""
    return NXKey().key_zomato()[0]['API_KEY']


def get_categories(headers, print_flag):
    """Refresh Zomato Categories data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] <START>")

    # Check if data exists / is stale
    db_cur_one.execute("select COUNT(*) from zmt_categories where TO_CHAR(INSERT_DT,'YYYY') = TO_CHAR(SYSDATE, 'YYYY')")
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Data stale/unavailable. "
                                                            "Refreshing...")
            response = requests.get(base_url + '/categories', params='', headers=headers).json()
            db_cur_two.execute("truncate table ZMT_CATEGORIES")

            for category in range(len(response['categories'])):
                if print_flag is 'Y':
                    print(str(response['categories'][category]['categories']['id'])
                          + ' ' + response['categories'][category]['categories']['name'])
                db_cur_two.execute("insert into ZMT_CATEGORIES values (:category_id, :category_name, SYSDATE)",
                                   category_id=response['categories'][category]['categories']['id'],
                                   category_name=response['categories'][category]['categories']['name'])
            db_conn.commit()

        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] Data is current. Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_categories()] <END>")


def get_cities(headers, query, print_flag):
    """Refresh Zomato Cities data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <START>")

    response = requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=headers).json()

    # Check if data exists
    db_cur_one.execute("select count(*) from ZMT_CITIES where CITY_NAME = :name", name=query)
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] Adding City: " + query)

            if print_flag is 'Y':
                print(str(response['location_suggestions'][0]['country_id'])
                      + ' ' + str(response['location_suggestions'][0]['country_name'])
                      + ' ' + str(response['location_suggestions'][0]['id'])
                      + ' ' + response['location_suggestions'][0]['name'])

            db_cur_two.execute("insert into ZMT_CITIES values (:city_id, :city_name, :country_id, :country_name, "
                               "SYSDATE)",
                               city_id=response['location_suggestions'][0]['id'],
                               city_name=response['location_suggestions'][0]['name'],
                               country_id=response['location_suggestions'][0]['country_id'],
                               country_name=response['location_suggestions'][0]['country_name'])
            db_conn.commit()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cities()] <END>")
    return str(response['location_suggestions'][0]['id'])


def get_cuisines(headers, city_id, print_flag):
    """Refresh Zomato Cuisines data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <START>")

    # Check if data exists / is stale
    db_cur_one.execute("select COUNT(*) from zmt_cuisines where TO_CHAR(INSERT_DT,'YYYY') = TO_CHAR(SYSDATE, 'YYYY')")
    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] Data is stale/unavailable. "
                                                            "Refreshing...")

            response = requests.get(base_url + '/cuisines?city_id=' + city_id, params='', headers=headers).json()

            db_cur_two.execute("truncate table ZMT_CUISINES")

            for cuisine in range(len(response['cuisines'])):
                if print_flag is 'Y':
                    print(str(response['cuisines'][cuisine]['cuisine']['cuisine_id'])
                          + ' ' + response['cuisines'][cuisine]['cuisine']['cuisine_name'])
                db_cur_two.execute("insert into ZMT_CUISINES values (:city_id, :cuisine_id, :cuisine_name, SYSDATE)",
                                   city_id=city_id,
                                   cuisine_id=response['cuisines'][cuisine]['cuisine']['cuisine_id'],
                                   cuisine_name=response['cuisines'][cuisine]['cuisine']['cuisine_name'])
            db_conn.commit()
        else:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] Data is current. Refresh skipped.")

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_cuisines()] <END>")
    return 0


def get_establishments(headers, city_id, print_flag):
    """Refresh Zomato Establishments data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] <START>")

    # Check if data exists / is stale
    db_cur_one.execute("select COUNT(*) from zmt_establishments where TO_CHAR(INSERT_DT,'YYYY') = "
                       "TO_CHAR(SYSDATE, 'YYYY')")

    for values in db_cur_one:
        if values[0] is 0:
            print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_establishments()] Data is stale/unavailable. "
                                                            "Refreshing...")

            response = requests.get(base_url + '/establishments?city_id=' + city_id, params='', headers=headers).json()

            db_cur_two.execute("truncate table ZMT_ESTABLISHMENTS")

            for establishment in range(len(response['establishments'])):
                if print_flag is 'Y':
                    print(str(response['establishments'][establishment]['establishment']['id'])
                          + ' ' + response['establishments'][establishment]['establishment']['name'])
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


def get_collections(headers, city_id, print_flag):
    """Refresh Zomato Collections data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collections()] <START>")

    db_cur_one.execute("delete from ZMT_COLLECTIONS where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and CITY_ID = :city_id",
                       city_id=city_id)

    response = requests.get(base_url + '/collections?city_id=' + city_id, params='', headers=headers).json()

    for collection in range(len(response['collections'])):
        if print_flag is 'Y':
            print(str(response['collections'][collection]['collection']['collection_id'])
                  + ' ' + str(response['collections'][collection]['collection']['res_count'])
                  + ' ' + response['collections'][collection]['collection']['title']
                  + ' ' + response['collections'][collection]['collection']['description']
                  + ' ' + response['collections'][collection]['collection']['url']
                  + ' ' + response['collections'][collection]['collection']['share_url'])
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


def get_locations(headers, query, print_flag):
    """Refresh Zomato Locations data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_locations()] <START>")

    response = requests.get(base_url + '/locations?query=' + query + '&count=1', params='', headers=headers).json()

    db_cur_one.execute("truncate table ZMT_LOCATIONS")

    if print_flag is 'Y':
        print(str(response['location_suggestions'][0]['entity_id'])
              + ' ' + response['location_suggestions'][0]['entity_type']
              + ' ' + response['location_suggestions'][0]['title']
              + ' ' + str(response['location_suggestions'][0]['latitude'])
              + ' ' + str(response['location_suggestions'][0]['longitude'])
              + ' ' + str(response['location_suggestions'][0]['city_id'])
              + ' ' + response['location_suggestions'][0]['city_name']
              + ' ' + str(response['location_suggestions'][0]['country_id'])
              + ' ' + response['location_suggestions'][0]['country_name'])

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


def get_location_details(headers, entity_id, entity_type, print_flag):
    """Refresh Zomato Location Details data"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_location_details()] <START>")

    response = requests.get(base_url + '/location_details?entity_id=' + entity_id + '&entity_type=' + entity_type,
                            params='', headers=headers).json()

    if print_flag is 'Y':
        print(str(response['location']['entity_id'])
              + ' ' + response['location']['entity_type']
              + ' ' + str(response['popularity'])
              + ' ' + str(response['nightlife_index'])
              + ' ' + str(response['top_cuisines'])
              + ' ' + str(response['popularity_res'])
              + ' ' + str(response['nightlife_res'])
              + ' ' + str(response['num_restaurant']))
    db_cur_one.execute("delete from ZMT_LOCATIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') and "
                       "ENTITY_ID = :entity_id", entity_id=entity_id)
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


def get_search_bylocation(headers, query, entity_id, entity_type, print_flag):
    """Search Zomato Restaurants by Location"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] <START>")

    search_parameters = ('entity_id=' + entity_id + '&entity_type=' + entity_type + '&q=' + query)
    results_start = 0
    results_end = 100
    results_shown = 20

    while results_start < results_end:
        response = requests.get(base_url + '/search?' + search_parameters + '&start=' + str(results_start) + '&count='
                                + str(results_shown) + '&sort=rating&order=desc', params='', headers=headers).json()

        print(response)

        # results_found = response['results_found']
        results_start = response['results_start']
        results_shown = response['results_shown']
        if print_flag is 'Y':
            print("Results Start:" + str(results_start))
            print("Results Shown:" + str(results_shown))

        for restaurant in range(len(response['restaurants'])):
            if print_flag is 'Y':
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
            db_cur_one.execute("""delete from ZMT_RESTAURANTS_EXT 
                                        where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM') 
                                          and RESTAURANT_ID = :restaurant_id""",
                               restaurant_id=response['restaurants'][restaurant]['restaurant']['id'])

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
        if results_end - results_start < 20:
            results_shown = results_end - results_start
    db_conn.commit()
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bylocation()] <END>")
    return 0


def get_search_bycollection(headers, query, print_flag):
    """Search Zomato Restaurants by Collections"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bycollection()] <START>")

    db_cur_one.execute("delete from ZMT_COLLECTIONS_EXT where PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')")

    db_cur_two.execute("select distinct CITY_ID, COLLECTION_ID from ZMT_COLLECTIONS order by CITY_ID, COLLECTION_ID")
    for values in db_cur_two:
        collection_id = values[1]
        search_parameters = ('collection_id=' + str(collection_id) + '&q=' + query)
        results_start = 0
        results_end = 100
        results_shown = 20

        while results_start < results_end:
            response = requests.get(base_url + '/search?' + search_parameters + '&start=' + str(results_start)
                                    + '&count=' + str(results_shown) + '&sort=rating&order=desc', params='',
                                    headers=headers).json()

            # results_found = response['results_found']
            results_start = response['results_start']
            results_shown = response['results_shown']
            if print_flag is 'Y':
                print("Results Start:" + str(results_start))
                print("Results Shown:" + str(results_shown))

            for restaurant in range(len(response['restaurants'])):
                if print_flag is 'Y':
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
            if results_end - results_start < 20:
                results_shown = results_end - results_start

    db_conn.commit()
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_search_bycollection()] <END>")
    return 0


def get_restaurant_bycollection(headers, print_flag):
    """Retrieve Zomato Restaurants data for Collections"""
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [get_collection_restaurant()] <START>")

    db_cur_one.execute("""select distinct RESTAURANT_ID 
                        from ZMT_COLLECTIONS_EXT 
                       where RESTAURANT_ID not in (select distinct RESTAURANT_ID from ZMT_RESTAURANTS)
                    order by RESTAURANT_ID""")
    for values in db_cur_two:
        res_id = values[0]
        search_parameters = ('res_id=' + str(res_id))
        response = requests.get(base_url + '/restaurant?' + search_parameters, params='', headers=headers).json()

        if print_flag is 'Y':
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

    headers = {'Accept': 'application/json', 'user-key': get_user_key()}
    city = 'Bangalore'
    localities = ['Sarjapur Road', 'HSR', 'Koramangala', 'Indiranagar', 'Kadubeesanahalli']
    print_flag = 'Y'

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] City: " + city)
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Localities: " + str(localities))
    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Print Flag: " + print_flag)

    # Fetch Category data
    '''get_categories(headers, print_flag)

    # Fetch City related data
    city_id = get_cities(headers, city, print_flag)
    get_cuisines(headers, city_id, print_flag)
    get_establishments(headers, city_id, print_flag)'''

    # Fetch Location related data
    for locality in range(len(localities)):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Processing Locality: " + localities[locality])
        entity = get_locations(headers, localities[locality], print_flag)
        #get_location_details(headers, entity[0], entity[1], print_flag)
        get_search_bylocation(headers, localities[locality], entity[0], entity[1], print_flag)

    # Fetch Collection related data
    '''get_collections(headers, city_id, print_flag)
    get_search_bycollection(headers, city, print_flag)
    get_restaurant_bycollection(headers, print_flag)'''

    # Close Oracle Connections
    db_cur_one.close()
    db_cur_two.close()
    db_conn.close()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")


if __name__ == '__main__':
    main()
