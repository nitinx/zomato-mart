# 31 Mar 2018 | Zomato Parameters

"""Zomato Parameters
Fetches user defined parameters (including city, localities, etc) from a relational database. These parameters are
used to restrict data fetched from Zomato.com
"""

import logging
from mylibrary.db_oracle import OracleClient

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
                log.info("getparam_city_names() | Parameter: CITY_NAME missing. Please define.")
            else:
                db_cur_two.execute("select distinct CITY_NAME from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
                for city_name in db_cur_two:
                    city = city_name[0]

        log.info("getparam_city_names() | PARAMETER City: " + city)
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
                log.info("getparam_localities() | Parameter: LOCALITY missing. Please define.")

            else:
                db_cur_two.execute("select distinct LOCALITY from ZMT_PARAMETERS where ACTIVE_FLAG = 'Y'")
                for locality in db_cur_two:
                    localities.append(locality[0])

        log.info("getparam_localities() | PARAMETER Locality: " + str(localities))
        log.debug("getparam_localities() | <END>")
        return localities
