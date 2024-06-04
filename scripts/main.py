from typing import List, Any
from APIConnector import APIConnector
import polars as pl
import pprint
import os

if __name__ == "__main__":
    api_conn = APIConnector(os.getenv('API_URL'))

    def get_characters():
        characters_source: list[Any] = api_conn.get_paginated_data('character')
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

    def get_episodes():
        episodes_source: list[Any] = api_conn.get_paginated_data('episode')
        episodes_data = pl.DataFrame(episodes_source)

        return episodes_data

    def count_appearances_in_episodes(characters):
        characters_in_episodes = (characters
                          .groupby('name')
                          .count()
                          .sort('count')
                          )

        return characters_in_episodes

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

    def get_locations():
        locations_source: list[Any] = api_conn.get_paginated_data('location')
        locations_data = pl.DataFrame(locations_source)

        return locations_data


    result = get_joined_characters_episodes_locations(get_characters(), get_episodes(), get_locations())
    result.write_csv('../results/characters_episodes_locations.csv')
