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
from mylibrary.apikey import APIKey
from mylibrary.db_oracle import OracleClient
from mylibrary.zomato import ZomatoParameters
from mylibrary.zomato import ZomatoClient
from time import gmtime, strftime


log = logging.getLogger(__name__)


if __name__ == '__main__':

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")

    # Initialize variables
    debug_mode = 'N'
    city = ''
    localities = []

    # Logger | Initialize
    fmt_string = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    fmtr = logging.Formatter(fmt=fmt_string)
    sh = logging.StreamHandler()
    sh.setFormatter(fmtr)
    my_lib_logger = logging.getLogger("mylibrary")
    my_lib_logger.addHandler(sh)

    # Logger | Set Level
    my_lib_logger.setLevel("INFO")

    # Initialize Zomato API Key Objects
    ZomatoAPIKey = APIKey()
    api_key = ZomatoAPIKey.key_zomato()[0]['API_KEY']
    headers = {'Accept': 'application/json', 'user-key': api_key}

    # Initialize Zomato Objects
    ZmtParams = ZomatoParameters()
    ZmtClient = ZomatoClient()

    # Retrieve Parameters
    city = ZmtParams.getparam_city_names()
    localities = ZmtParams.getparam_localities()

    # Fetch Category data
    ZmtClient.get_categories(headers)

    # Fetch City data
    city_id = ZmtClient.get_cities(headers, city)
    ZmtClient.get_cuisines(headers, city_id)
    ZmtClient.get_establishments(headers, city_id)

    # Fetch Location/Restaurant data
    for locality in range(len(localities)):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Processing Locality: " + localities[locality])
        entity = ZmtClient.get_locations(headers, localities[locality])
        ZmtClient.get_location_details(headers, entity[0], entity[1], debug_mode)
        ZmtClient.get_search_bylocation(headers, localities[locality], entity[0], entity[1], debug_mode)

    # Fetch Collection/Restaurant data
    ZmtClient.get_collections(headers, city_id)
    ZmtClient.get_search_bycollection(headers, city, debug_mode)
    ZmtClient.get_restaurant_bycollection(headers, debug_mode)

    # Close Oracle Connections
    db_cur_one.close()
    db_cur_two.close()
    db_conn.close()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")
