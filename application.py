# 28 Nov 2017 | Zomato Data Mart

"""Zomato Datamart
Application that:
 1. Fetches data from Zomato.com via Zomato's public APIs
 2. Populates the data into the Zomato datamart
 3. Maintains history at a monthly time grain
 4. Fetch is restricted via parameters

 API Documentation: https://developers.zomato.com/api#headline1
"""

import logging
from mylibrary.apikey import APIKey
from mylibrary.zomato import ZomatoParameters
from mylibrary.zomato import ZomatoClient
from mylibrary.zomato import ZomatoAlerts
from time import gmtime, strftime

log = logging.getLogger(__name__)

if __name__ == '__main__':

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <START>")

    # Initialize variables
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
    api_key_zomato = ZomatoAPIKey.retrieve_key("zomato")[0]['API_KEY']
    headers = {'Accept': 'application/json', 'user-key': api_key_zomato}

    # Initialize Mailgun API Key Objects
    MailgunAPIKey = APIKey()
    api_key_mailgun = MailgunAPIKey.retrieve_key("mailgun")[0]['API_KEY']

    # Initialize Zomato Objects
    ZmtParams = ZomatoParameters()
    ZmtClient = ZomatoClient()
    ZmtAlert = ZomatoAlerts()

    # Retrieve Parameters
    '''city = ZmtParams.getparam_city_names()
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
        ZmtClient.get_location_details(headers, entity[0], entity[1])
        ZmtClient.get_search_bylocation(headers, localities[locality], entity[0], entity[1])

    # Fetch Collection/Restaurant data
    ZmtClient.get_collections(headers, city_id)
    ZmtClient.get_search_bycollection(headers, city)
    ZmtClient.get_restaurant_bycollection(headers)'''

    #ZmtAlert.compose_alert()
    ZmtAlert.send_alert(api_key_mailgun, ZmtAlert.compose_alert())

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")
