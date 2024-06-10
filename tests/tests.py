import unittest
import polars as pl


class RickAndMortyTests(unittest.TestCase):

    def test_appearances_in_episodes(self):
        data = [51, 51, 48, 48, 44]
        result = sum(data)

        testing_data = pl.read_csv('../results/csv_files/appearances_in_episodes.csv').rows()
        test_sum = 0

        for record in testing_data:
            test_sum += record[1]

        self.assertEqual(result, test_sum)

    def test_characters_per_location(self):
        data = [219, 98, 62, 27, 26]
        result = sum(data)

        testing_data = pl.read_csv('../results/csv_files/characters_per_location.csv').rows()
        test_sum = 0

        for record in testing_data:
            test_sum += record[1]

        self.assertEqual(result, test_sum)

    def test_characters_per_season(self):
        data = [268, 234, 245, 224, 210]
        result = sum(data)

        testing_data = pl.read_csv('../results/csv_files/characters_per_season.csv').rows()
        test_sum = 0

        for record in testing_data:
            test_sum += record[1]

        self.assertEqual(result, test_sum)

    def test_episodes_per_year(self):
        data = [10, 5, 5, 10, 10, 8, 3]
        result = sum(data)

        testing_data = pl.read_csv('../results/csv_files/episodes_per_year.csv').rows()
        test_sum = 0

        for record in testing_data:
            test_sum += record[1]

        self.assertEqual(result, test_sum)


if __name__ == '__main__':
    unittest.main()
