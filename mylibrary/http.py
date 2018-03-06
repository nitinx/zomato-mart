import logging
import requests

log = logging.getLogger(__name__)


class BaseHTTPClient(object):

    def __init__(self, base_url, headers):
        self._base_url = base_url
        self._headers = {'Accept': 'application/json'}
        self._headers.update(headers)

    def get(self, url, *args, **kwargs):
        res_url = self._base_url + url
        cur_headers = kwargs.get("headers", {})
        cur_headers.update(self._headers)
        kwargs["headers"] = cur_headers
        return requests.get(res_url, *args, **kwargs).json()
