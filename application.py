# 28 Nov 2017 | Zomato Data Mart

"""Zomato Datamart
Program that:
 1. Fetches data from Zomato.com via Zomato's public APIs
 2. Populates the data into the Zomato datamart
 3. Maintains history at a monthly time grain
 4. Fetch is currently restricted via parameters

 API Documentation: https://developers.zomato.com/api#headline1
"""

import logging
import requests
import json
from mylibrary.nxcommon import NXKey
#from mylibrary.http import BaseHTTPClient
from mylibrary.zomato import SuperiorZomatoClient
from time import gmtime, strftime


def get_user_key():
    """Get the Zomato API Key"""
    return NXKey().key_zomato()[0]['API_KEY']

'''
# Define Oracle Variables
db_conn = NXOracle().db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()


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
    #db_cur_one.close()
    #db_cur_two.close()
    #db_conn.close()

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")


if __name__ == '__main__':
    main()
'''

if __name__ == "__main__":

    # ------- OLD CODE

    # cli = BaseHTTPClient(
    #     "https://developers.zomato.com/api/v2.1",
    #     headers={"user-key": "96de02583dbb02fae87e834846ee7ee5"}
    # )

    # result = cli.get("/categories")
    # print(result)

    # ------- NEW CODE

    # A formatter defines how each log entry should be formatted
    # You need to create a `Formatter` instance with the format
    fmt_string = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    fmtr = logging.Formatter(fmt=fmt_string)

    # A stream handler logs to the console.
    # On each logHandler (like StreamHandler), you can set a formatter,
    # log_level, etc. We'll set only a formatter here
    sh = logging.StreamHandler()
    sh.setFormatter(fmtr)

    # getLogger(<module-name>) will fetch the `logger` instance associated
    # with that module. This is a `singleton` which means, in your python
    # process, no matter from which module you do `getLogger(<name>)`, you'll
    # get the logger associated with the supplied <name>

    # Example, in `mylibrary\zomato.py` if I did getLogger("mylibrary"),
    # I'll still get `mylibrary` logger (exact same instance as the one below)

    # 1. Get the logger
    my_lib_logger = logging.getLogger("mylibrary")
    # 2. Attach the stream handler
    my_lib_logger.addHandler(sh)
    # 3. Set the level for the `mylibrary` logger as `DEBUG`. This
    #    automatically applies to submodules as well, unless you explicitly
    #    getLogger("mylibrary.<something>") and setLevel(to-some-other-level)
    my_lib_logger.setLevel("DEBUG")

    zom = SuperiorZomatoClient(get_user_key())
    #cat = zom.get_categories()
    city_id = zom.get_cities("Bangalore")
