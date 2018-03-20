# 11 Mar 2018 | API Key Retrieval

import json
import logging
from time import gmtime, strftime

base_dir = 'e:\\GitHub\\python\\keys\\'
log = logging.getLogger(__name__)


class APIKey:

    def retrieve_key(self, category):
        log.debug("[API Key] " + category + " | Retrieval Initiated")

        # Open KEY files
        with open(base_dir + category + '.key') as key_file:
            key = json.load(key_file)

        log.debug("[API Key] " + str(key) + " >")
        log.debug("[API Key] " + category + " | Retrieval Completed")
        return key
