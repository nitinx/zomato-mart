# 31 Mar 2018 | Zomato Requests

"""Zomato Requests
Library that requests data from Zomato's API endpoints

API Documentation: https://developers.zomato.com/api#headline1
"""

import requests
import logging
import json

# Define Zomato Base URL
base_url = "https://developers.zomato.com/api/v2.1"

log = logging.getLogger(__name__)


class ZomatoRequests(object):

    def __init__(self, headers):
        self.headers = headers

    def get_categories(self):
        """Get Categories"""
        return requests.get(base_url + '/categories', params='', headers=self.headers).json()

    def get_cities(self, query):
        """Get Cities"""
        return requests.get(base_url + '/cities?q=' + query + '&count=1', params='', headers=self.headers).json()

    def get_cuisines(self, city_id):
        """Get Cuisines"""
        return requests.get(base_url + '/cuisines?city_id=' + city_id, params='', headers=self.headers).json()

    def get_establishments(self, city_id):
        """Get Establishments"""
        return requests.get(base_url + '/establishments?city_id=' + city_id, params='', headers=self.headers).json()

    def get_collections(self, city_id):
        """Get Collections"""
        return requests.get(base_url + '/collections?city_id=' + city_id, params='', headers=self.headers).json()

    def get_locations(self, query):
        """Get Locations"""
        return requests.get(base_url + '/locations?query=' + query + '&count=1', params='', headers=self.headers).json()

    def get_location_details(self, entity_id, entity_type):
        """Get Location Details"""
        return requests.get(base_url + '/location_details?entity_id=' + entity_id + '&entity_type=' + entity_type,
                            params='', headers=self.headers).json()

    def get_search(self, search_parameters, results_start, results_shown):
        """Get Search"""
        return requests.get(base_url + '/search?' + search_parameters + '&start=' + results_start + '&count='
                            + results_shown + '&sort=rating&order=desc', params='', headers=self.headers).json()

    def get_restaurant(self, search_parameters):
        """Get Restaurant"""
        return requests.get(base_url + '/restaurant?' + search_parameters, params='', headers=self.headers).json()
