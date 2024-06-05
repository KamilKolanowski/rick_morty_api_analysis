from APIConnector import APIConnector
import polars as pl


class DataOperations:
    def __init__(self):
        self.api_conn = APIConnector()
        self.characters = self.get_dataframe('character')
        self.episodes = self.get_dataframe('episode')
        self.locations = self.get_dataframe('location')
        self.filtered_characters = self.filter_characters()

    def get_dataframe(self, endpoint):
        return pl.DataFrame(self.api_conn.get_paginated_data(endpoint))

    def filter_characters(self):
        characters_data = self.characters

        characters_cols_list = ["id", "name", "status",
                                "species", "gender",
                                "location", "episode"]

        characters_selected = characters_data.select(characters_cols_list)

        characters = (characters_selected
                      .with_columns(characters_selected['location']
                                    .map_elements(lambda x: x['name'] if x is not None else None))
                      .explode('episode')
                      .with_columns(pl.col('episode')
                                    .str
                                    .replace('https://rickandmortyapi.com/api/episode/', '')
                                    .cast(pl.Int64))
                      )

        return characters

    @staticmethod
    def count_appearances_in_episodes(characters):
        characters_in_episodes = (
                                  characters
                                  .group_by('name')
                                  .agg(pl.count().alias('appearances_in_episodes'))
                                  .sort('appearances_in_episodes', descending=True)
                                  )

        return characters_in_episodes

    @staticmethod
    def get_joined_characters_episodes_locations(characters, episodes, location):
        sel_list = ['name', 'status', 'species', 'gender', 'location',
                    pl.col('type').alias('location_type'), 'dimension',
                    pl.col('episode_right').alias('episode'), 'air_date']

        return (characters
                .join(episodes,
                      left_on=['episode'],
                      right_on=['id'],
                      how='inner')
                .join(location,
                      left_on=['location'],
                      right_on=['name'],
                      how='inner')
                .select(sel_list)
                )

    @staticmethod
    def get_no_of_characters_per_location(characters):
        return (
            characters
            .drop('episode')
            .unique()
            .group_by('location')
            .agg(pl.count('name').alias('characters_per_location'))
            .sort('characters_per_location', descending=True)
        )

    @staticmethod
    def get_no_of_characters_per_season(characters):
        return (
            characters
            .select(['name', 'episode'])
            .unique()
            .with_columns(pl.col('episode').str.slice(0, 3).alias('season'))
            .group_by('season')
            .agg(pl.count('name').alias('no_of_characters'))
            .sort('season')
        )

    @staticmethod
    def get_episodes_per_year(episodes):
        return (
            episodes
            .select(['episode', 'air_date'])
            .with_columns(
                pl.col('air_date')
                .str.extract(r', (\d{4})', 1)
                .alias('year')
            )
            .group_by('year')
            .agg(pl.count('episode').alias('no_of_episodes_per_year'))
            .sort('year', descending=True)
        )

    def write_results(self):
        main_dir = '../results'
        characters_episodes_locations = self.get_joined_characters_episodes_locations(self.filtered_characters,
                                                                                      self.episodes,
                                                                                      self.locations)

        characters_episodes_locations.write_csv(f'{main_dir}/characters_episodes_locations.csv')

        appearances_in_episodes = self.count_appearances_in_episodes(self.filtered_characters)
        appearances_in_episodes.write_csv(f'{main_dir}/appearances_in_episodes.csv')

        characters_per_location = self.get_no_of_characters_per_location(self.filtered_characters)
        characters_per_location.write_csv(f'{main_dir}/characters_per_location.csv')

        characters_per_season = self.get_no_of_characters_per_season(characters_episodes_locations)
        characters_per_season.write_csv(f'{main_dir}/characters_per_season.csv')

        episodes_per_year = self.get_episodes_per_year(self.episodes)
        episodes_per_year.write_csv(f'{main_dir}/episodes_per_year.csv')


if __name__ == "__main__":
    do = DataOperations()
    do.write_results()
