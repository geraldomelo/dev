import json
import requests
from requests.exceptions import HTTPError
from .safebox import Safebox


class Pier():

    def __init__(self):
        self.url = Safebox.get_secret('API-PIER-URL')
        self.__client_id = Safebox.get_secret('API-PIER-CLIENTID')
        self.__access_token = Safebox.get_secret('API-PIER-TOKEN')
        self.__headers = {
            "client_id": self.__client_id,
            "access_token": self.__access_token, 
            "Content-Type": "application/json",
        }
        
    def _get_content(self, response, format_json=True):
        if format_json:
            return json.loads(response.content)
        return response.content

    def get(self, url, params={}, body={}, headers={}, format_json=True):
        headers = {
            **self.__headers,
            **headers
        }
        try:
            res = requests.get(self.url + url, headers=headers, data=body, params=params)
            res.raise_for_status()
        except HTTPError as http_error:
            content = self._get_content(http_error.response, format_json)
            return http_error.response.status_code, content
        else:
            return res.status_code, self._get_content(res, format_json)
        
    def post(self, url, params={}, body={}, headers={}, format_json=False):
        headers = {
            **self.__headers,
            **headers
        }
        try:
            res = requests.post(self.url + url, headers=headers, data=body, params=params)
            res.raise_for_status()
        except HTTPError as http_error:
            content = self._get_content(http_error.response, format_json)
            return http_error.response.status_code, content
        else:
            return res.status_code, self._get_content(res, format_json)


