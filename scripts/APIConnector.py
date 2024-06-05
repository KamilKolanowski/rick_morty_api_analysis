import requests
import dotenv
import os

dotenv.load_dotenv()


class APIConnector:
    def __init__(self):
        self.api_url = os.getenv('API_URL')

    @staticmethod
    def create_headers():
        headers = {
            'Content-Type': 'application/json'
        }

        return headers

    def parse_json(self, endpoint, page):
        response = requests.get(f'{self.api_url}/{endpoint}/?page={page}', headers=self.create_headers())

        return response.json()

    def get_paginated_data(self, endpoint):
        all_data = []
        page = 1

        while results := self.parse_json(endpoint, page).get('results', []):
            all_data.extend(results)
            page += 1

            if not results:
                break

        return all_data
