# 28 Nov 2017 | Zomato Data Mart

"""Zomato Datamart
Application that:
 1. Fetches data from Zomato.com via Zomato's public APIs
 2. Populates the data into the Zomato datamart
 3. Maintains history at a monthly time grain
 4. Fetch is restricted via parameters
 5. Sends out new restaurant alerts to subscribers

 API Documentation: https://developers.zomato.com/api#headline1
"""

import logging
from mylibrary.apikey import APIKey
from mylibrary.zmt_parameters import ZomatoParameters
from mylibrary.zmt_client import ZomatoClient
from mylibrary.zmt_alerts import ZomatoAlerts
from mylibrary.zmt_analytics import ZomatoAnalytics
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
    my_lib_logger.setLevel("DEBUG")

    # Initialize Zomato API Key Objects
    ZomatoAPIKey = APIKey()
    api_key_zomato = ZomatoAPIKey.retrieve_key("zomato")[0]['API_KEY']
    headers = {'Accept': 'application/json', 'user-key': api_key_zomato}

    # Initialize Mailgun API Key Objects
    MailgunAPIKey = APIKey()
    api_key_mailgun = MailgunAPIKey.retrieve_key("mailgun")[0]['API_KEY']

    # Initialize Zomato Objects
    ZmtParams = ZomatoParameters()
    ZmtClient = ZomatoClient(headers)
    ZmtAlert = ZomatoAlerts()
    ZmtPlot = ZomatoAnalytics()

    # Retrieve Parameters
    city = ZmtParams.getparam_city_names()
    localities = ZmtParams.getparam_localities()

    # Fetch Category data
    ZmtClient.get_categories()

    # Fetch City data
    city_id = ZmtClient.get_cities(city)
    ZmtClient.get_cuisines(city_id)
    ZmtClient.get_establishments(city_id)

    # Fetch Location/Restaurant data
    for locality in range(len(localities)):
        print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] Processing Locality: " + localities[locality])
        entity = ZmtClient.get_locations(localities[locality])
        ZmtClient.get_location_details(entity[0], entity[1])
        ZmtClient.get_search_bylocation(localities[locality], entity[0], entity[1])

    # Fetch Collection/Restaurant data
    #ZmtClient.get_collections(city_id)
    #ZmtClient.get_search_bycollection(city)
    #ZmtClient.get_restaurant_bycollection()

    # Send New Restaurant Alert(s)
    for locality in range(len(localities)):
        ZmtAlert.send_alert(api_key_mailgun, ZmtAlert.compose_alert('%' + localities[locality] + '%'),
                            localities[locality])

    ZmtAlert.send_analytics(api_key_mailgun, ZmtPlot.plot_locality_stats())

    print(strftime("%Y-%b-%d %H:%M:%S", gmtime()) + " | [main()] <END>")
