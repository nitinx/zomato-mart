# 31 Mar 2018 | Zomato Alerts

"""Zomato Alerts
Composes and sends out New Restaurant alerts by locality to subscribers
"""

import logging
import requests
from mylibrary.db_oracle import OracleClient

# Define Oracle Variables
DB = OracleClient()
db_conn = DB.db_login()
db_cur_one = db_conn.cursor()
db_cur_two = db_conn.cursor()

log = logging.getLogger(__name__)


class ZomatoAlerts:

    def compose_alert(self, locality):
        """Compose Alert"""
        log.debug("compose_alert() " + locality + " | <START>")
        alert_body = ""

        # Check if data exists
        db_cur_one.execute("select COUNT(*)"
                           "  from ZMT_RESTAURANTS ZR, ZMT_RESTAURANTS_EXT ZR_EXT"
                           " where ZR.RESTAURANT_ID = ZR_EXT.RESTAURANT_ID"
                           "   and TO_CHAR(ZR.INSERT_DT, 'YYYYMM') = TO_CHAR(SYSDATE, 'YYYYMM')"
                           "   and ZR_EXT.PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')"
                           "   and ZR.LOC_LOCALITY like :locality", locality=locality)

        for count in db_cur_one:
            if count[0] is 0:
                log.info("compose_alert() | " + locality + " | Data unavailable. Alert skipped.")
                alert_body = "0"
            else:
                db_cur_two.execute("select ZR.LOC_LOCALITY, ZR.RESTAURANT_NAME, ZR_EXT.USER_RATING_AGGREGATE, "
                                   "       ZR_EXT.AVERAGE_COST_FOR_TWO, ZR_EXT.CUISINES, ZR.URL"
                                   "  from ZMT_RESTAURANTS ZR, ZMT_RESTAURANTS_EXT ZR_EXT"
                                   " where ZR.RESTAURANT_ID = ZR_EXT.RESTAURANT_ID"
                                   "   and TO_CHAR(ZR.INSERT_DT, 'YYYYMM') = TO_CHAR(SYSDATE, 'YYYYMM')"
                                   "   and ZR_EXT.PERIOD = TO_CHAR(SYSDATE, 'YYYYMM')"
                                   "   and ZR.LOC_LOCALITY like :locality", locality=locality)
                for values in db_cur_two:
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

        if alert_body != "0":
            # Check if subscribers exists
            db_cur_one.execute("select COUNT(*) from ZMT_ALERTS")

            for count in db_cur_one:
                if count[0] is 0:
                    log.info("compose_alert() | " + locality + " | No subscribers. Alert skipped.")
                    alert_body = "0"
                else:
                    db_cur_two.execute("select SUBS_NAME, SUBS_MAIL_ID from ZMT_ALERTS")
                    for values in db_cur_two:
                        subs_name = values[0]
                        subs_mail_id = values[1]

                        requests.post(
                            "https://api.mailgun.net/v3/sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org/messages",
                            auth=("api", api_key),
                            data={"from": "Mailgun Sandbox "
                                          "<postmaster@sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org>",
                                  "to": subs_name + " <" + subs_mail_id + ">",
                                  "subject": "Zomato Alert | " + locality,
                                  "html": alert_header + alert_body})

        log.debug("send_alert() " + locality + " | <END>")
        return 0

    def send_analytics(self, api_key, mail_body):
        """Send Analytics"""
        log.debug("send_analytics() | <START>")
        mail_header = "<head>" \
                      "  <style>" \
                      "    table {font-family: arial, sans-serif; border-collapse: collapse; width: 100%; } " \
                      "    td, th {border: 1px solid #dddddd; text-align:  left; padding: 8px; } " \
                      "    tr:nth-child(even) {background-color: #dddddd; } " \
                      "  </style>" \
                      "</head>" \

        if mail_body != "0":
            # Check if subscribers exists
            db_cur_one.execute("select COUNT(*) from ZMT_ALERTS")

            for count in db_cur_one:
                if count[0] is 0:
                    log.info("compose_alert() | " + locality + " | No subscribers. Alert skipped.")
                    mail_body = "0"
                else:
                    db_cur_two.execute("select SUBS_NAME, SUBS_MAIL_ID from ZMT_ALERTS")
                    for values in db_cur_two:
                        subs_name = values[0]
                        subs_mail_id = values[1]

                        requests.post(
                            "https://api.mailgun.net/v3/sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org/messages",
                            auth=("api", api_key),
                            files=[("inline", open("plot.png", 'rb'))],
                            data={"from": "Mailgun Sandbox "
                                          "<postmaster@sandboxd7ddf28978bc465596fa4cad095cb3ac.mailgun.org>",
                                  "to": subs_name + " <" + subs_mail_id + ">",
                                  "subject": "Zomato | Rating Analytics | Across Localities",
                                  "html": '<html><img src="cid:plot.png"></html>'})

        log.debug("send_analytics() | <END>")
        return 0
