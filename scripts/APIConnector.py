import os
import requests
import dotenv

dotenv.load_dotenv()


class APIConnector:
    """ APIConnector Class

        Operating on Rick and Morty API data
        Retrieving data directly from API
        Paginating data to retrieve everything
    """
    def __init__(self):
        self.api_url = os.getenv('API_URL')

    @staticmethod
    def create_headers():
        """ Create headers to retrieve json data from API """
        headers = {
            'Content-Type': 'application/json'
        }

        return headers

    def parse_json(self, endpoint, page):
        """ Parse json data from API
            Send request to Rick and Morty API with headers included """
        response = requests.get(f'{self.api_url}/{endpoint}/?page={page}', headers=self.create_headers())

        return response.json()

    def get_paginated_data(self, endpoint):
        """ Paginating data

            Go through all the pages of the data
            Retrieve it, and store in the list which is later returned """
        all_data = []
        page = 1

        while results := self.parse_json(endpoint, page).get('results', []):
            all_data.extend(results)
            page += 1

            if not results:
                break

        return all_data
