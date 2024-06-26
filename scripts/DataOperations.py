import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
from APIConnector import APIConnector


class DataOperations:
    """ DataOperations Class

        Performing ETL processes on Rick and Morty data
        Retrieving data from API - APIConnector.py
        Performing transformations using Polars
        Saving data to files
        Drawing plots using seaborn and matplotlib

    """
    def __init__(self):
        self.api_conn = APIConnector()
        self.characters = self.get_dataframe('character')
        self.episodes = self.get_dataframe('episode')
        self.locations = self.get_dataframe('location')
        self.filtered_characters = self.filter_characters()

    def get_dataframe(self, endpoint):
        """ Generic approach to retrieve data from API and save it in dataframe """
        return pl.DataFrame(self.api_conn.get_paginated_data(endpoint))

    def filter_characters(self):
        """ Filter characters data for specific columns,
        with exploding struct, and array columns """
        characters_data = self.characters

        characters_cols_list = ["id", "name", "status",
                                "species", "gender",
                                "location", "episode"]

        characters_selected = characters_data.select(characters_cols_list)

        characters = (characters_selected
                      .with_columns(pl.col('location').struct.field('name').alias('location'))
                      .explode('episode')
                      .with_columns(pl.col('episode')
                                    .str
                                    .replace('https://rickandmortyapi.com/api/episode/', '')
                                    .cast(pl.Int64))
                      )

        return characters

    @staticmethod
    def count_appearances_in_episodes(characters):
        """ Counting how many times each character appeared in episodes """
        characters_in_episodes = (
                                  characters
                                  .select('name', 'episode')
                                  .unique()
                                  .with_columns(pl.col('name').alias('Name'))
                                  .group_by('Name')
                                  .agg(pl.count('episode').alias('Appearances in episodes'))
                                  .sort('Appearances in episodes', descending=True)
                                  .limit(5)
                                  )

        return characters_in_episodes

    @staticmethod
    def get_joined_characters_episodes_locations(characters, episodes, location):
        """ Joining all available tables into one full table """
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
        """ Get number of characters per each location
            Limiting data to only 5 rows """
        return (
            characters
            .select('name', 'location')
            .unique()
            .with_columns(pl.col('location').alias('Location'))
            .group_by('Location')
            .agg(pl.count('name').alias('Characters'))
            .sort('Characters', descending=True)
            .limit(5)
        )

    @staticmethod
    def get_no_of_characters_per_season(characters):
        """ Get number of characters per each season
                    Limiting data to only 5 rows """
        return (
            characters
            .select(['name', 'episode'])
            .unique()
            .with_columns(pl.col('episode').str.slice(0, 3).alias('Season'))
            .group_by('Season')
            .agg(pl.count('name').alias('Characters'))
            .sort('Season')
            .limit(5)
        )

    @staticmethod
    def get_no_of_episodes_per_year(episodes):
        """ Get number of episodes per each year """
        return (
            episodes
            .select(['episode', 'air_date'])
            .with_columns(
                pl.col('air_date')
                .str.extract(r', (\d{4})', 1)
                .alias('Year')
            )
            .group_by('Year')
            .agg(pl.count('episode').alias('Episodes'))
            .sort('Year', descending=True)
        )

    @staticmethod
    def draw_bar_plot(x_axis, y_axis, data, title):
        """ Generic method to draw bar plot with Rick and Morty color palette """
        rick_and_morty_palette = ['#FF7F50', '#87CEEB', '#ADFF2F',
                                  '#FFD700', '#DA70D6', '#8A2BE2', '#00FA9A']
        data = data.to_pandas()

        plot = sns.barplot(x=x_axis,
                           y=y_axis,
                           hue=x_axis,
                           legend=False,
                           data=data,
                           palette=rick_and_morty_palette)

        for p in plot.patches:
            plot.annotate(format(p.get_height(), '.0f'),
                          (p.get_x() + p.get_width() / 2., p.get_height()),
                          ha='center', va='center',
                          xytext=(0, 9),
                          textcoords='offset points')

        plt.xticks(rotation=35, ha='right')
        plt.title(title)
        plt.show()

    def get_results(self):
        """ Getting final results, saving them to files, and drawing bar plots """
        main_dir = '../results/csv_files'

        characters_episodes_locations = (
            self.get_joined_characters_episodes_locations(self.filtered_characters,
                                                          self.episodes,
                                                          self.locations))

        appearances_in_episodes = self.count_appearances_in_episodes(self.filtered_characters)
        characters_per_location = self.get_no_of_characters_per_location(self.filtered_characters)
        characters_per_season = self.get_no_of_characters_per_season(characters_episodes_locations)
        episodes_per_year = self.get_no_of_episodes_per_year(self.episodes)

        characters_episodes_locations.write_csv(f'{main_dir}/characters_episodes_locations.csv')
        appearances_in_episodes.write_csv(f'{main_dir}/appearances_in_episodes.csv')
        characters_per_location.write_csv(f'{main_dir}/characters_per_location.csv')
        characters_per_season.write_csv(f'{main_dir}/characters_per_season.csv')
        episodes_per_year.write_csv(f'{main_dir}/episodes_per_year.csv')

        self.draw_bar_plot('Name',
                           'Appearances in episodes',
                           appearances_in_episodes,
                           'TOP 5 characters appearances')

        self.draw_bar_plot('Location',
                           'Characters',
                           characters_per_location,
                           'TOP 5 inhabited locations')

        self.draw_bar_plot('Season',
                           'Characters',
                           characters_per_season,
                           'Characters per season')

        self.draw_bar_plot('Year',
                           'Episodes',
                           episodes_per_year,
                           'Episodes per year')
