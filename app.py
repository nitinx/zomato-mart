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
import logging
import json
from mylibrary.nxcommon import APIKey
from mylibrary.nxcommon import DBOracle
#from mylibrary.http import BaseHTTPClient
from mylibrary.zomato import ZomatoClient
from time import gmtime, strftime

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

# Define Oracle Variables
DB = DBOracle()
db_conn = DB.db_login()
#db_conn = DBOracle().db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()

log = logging.getLogger(__name__)


def get_user_key():
    """Get the Zomato API Key"""
    return APIKey().key_zomato()[0]['API_KEY']


if __name__ == '__main__':
    fmt_string = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    fmtr = logging.Formatter(fmt=fmt_string)
    sh = logging.StreamHandler()
    sh.setFormatter(fmtr)
    my_lib_logger = logging.getLogger("mylibrary")
    my_lib_logger.addHandler(sh)
    my_lib_logger.setLevel("DEBUG")

    zom = ZomatoClient()

    # Initialize variables
    headers = {'Accept': 'application/json', 'user-key': get_user_key()}
    debug_mode = 'N'
    city = ''
    localities = []

    #print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")
    log.info("main() | <START>")
    log.debug("main() | <START>")

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
    zom.get_categories(headers)

    # Fetch City data
    city_id = zom.get_cities(headers, city)
    zom.get_cuisines(headers, city_id)
    zom.get_establishments(headers, city_id)

    # Fetch Location/Restaurant data
    for locality in range(len(localities)):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Processing Locality: " + localities[locality])
        entity = zom.get_locations(headers, localities[locality])
        zom.get_location_details(headers, entity[0], entity[1], debug_mode)
        zom.get_search_bylocation(headers, localities[locality], entity[0], entity[1], debug_mode)

    # Fetch Collection/Restaurant data
    zom.get_collections(headers, city_id)
    zom.get_search_bycollection(headers, city, debug_mode)
    zom.get_restaurant_bycollection(headers, debug_mode)

    # Close Oracle Connections
    db_cur_one.close()
    db_cur_two.close()
    db_conn.close()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")
