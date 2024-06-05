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
        page = 1
        all_data = []

        while True:
            response = self.parse_json(endpoint, page)
            results = response.get('results', [])

            if not results:
                break

            all_data.extend(results)
            page += 1

        return all_data
