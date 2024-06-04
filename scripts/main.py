from typing import List, Any
from APIConnector import APIConnector
import polars as pl
import os


class DataOperations:
    def __init__(self):
        self.api_conn = APIConnector(os.getenv('API_URL'))

    def get_characters(self):
        characters_source: list[Any] = self.api_conn.get_paginated_data('character')
        characters_data = pl.DataFrame(characters_source)

        characters_cols_list = ["id", "name", "status", "species", "gender", "location", "episode"]
        characters_selected = characters_data.select(characters_cols_list)
        characters = (characters_selected
                      .with_columns(characters_selected['location'].map_elements(lambda x: x['name']
        if x is not None else None))
                      .explode('episode')
                      .with_columns(pl.col('episode')
                                    .str
                                    .replace('https://rickandmortyapi.com/api/episode/', '')
                                    .cast(pl.Int64))
                      )

        return characters

    def get_locations(self):
        locations_source: list[Any] = self.api_conn.get_paginated_data('location')
        locations_data = pl.DataFrame(locations_source)

        return locations_data

    def get_episodes(self):
        episodes_source: list[Any] = self.api_conn.get_paginated_data('episode')
        episodes_data = pl.DataFrame(episodes_source)

        return episodes_data

    @staticmethod
    def count_appearances_in_episodes(characters):
        characters_in_episodes = (characters
                                  .groupby('name')
                                  .count()
                                  .sort('count')
                                  )

        return characters_in_episodes

    @staticmethod
    def get_joined_characters_episodes_locations(characters, episodes, location):
        return (characters
                .join(episodes,
                      left_on=['episode'],
                      right_on=['id'],
                      how='inner')
                .join(location,
                      left_on=['location'],
                      right_on=['name'],
                      how='inner')
                .select('name',
                        'status',
                        'species',
                        'gender',
                        'location',
                        pl.col('type').alias('location_type'),
                        'dimension',
                        pl.col('episode_right').alias('episode'),
                        'air_date',
                        )
                )

    def write_results(self):
        characters_episodes_locations = self.get_joined_characters_episodes_locations(self.get_characters(),
                                                                                 self.get_episodes(),
                                                                                 self.get_locations())

        characters_episodes_locations.write_csv('../results/characters_episodes_locations.csv')

        appearances_in_episodes = self.count_appearances_in_episodes(self.get_characters())
        appearances_in_episodes.write_csv('../results/appearances_in_episodes.csv')


if __name__ == "__main__":
    do = DataOperations()
    do.write_results()




