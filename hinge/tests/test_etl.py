from unittest import TestCase
import pandas as pd

from hinge.main.etl import Etl


class TestETL(TestCase):
    def test_etl_integration(self):
        expected_df = pd.read_csv('/Users/karthikeyanchandrasekar/Desktop/hinge/hinge/tests/resources/master_set.csv')

        etl = Etl()

        # Extractions
        us_golf_df = etl.extract('/Users/karthikeyanchandrasekar/Desktop/hinge/resources/unity_golf_club.csv',
                                 delimiter=',')
        us_soft_df = etl.extract('/Users/karthikeyanchandrasekar/Desktop/hinge/resources/us_softball_league.tsv',
                                 delimiter='\t')

        # column renaming
        column_rename_mapping = {"joined_league": "member_since", "date_of_birth": "dob", "us_state": "state"}
        us_soft_df = etl.column_rename(us_soft_df, columns=column_rename_mapping)

        # Data Tranformation
        us_soft_df = etl.date_transform(us_soft_df, column='dob', from_format='%m/%d/%Y', to_format='%Y/%m/%d')

        # name tranformation
        us_soft_df = etl.name_transformation(us_soft_df, column='name')

        # Merge transactions
        players_transactions = pd.concat([us_soft_df, us_golf_df], ignore_index=True)

        players_transactions['col_year'] = pd.to_datetime(players_transactions['dob']).dt.year
        actual_df = players_transactions[players_transactions['member_since'] > players_transactions['col_year']]
        actual_df = actual_df.drop("col_year", axis=1)

        assert actual_df["company_id"].count() == expected_df["company_id"].count()

        # TODO Need to check the complete dataframe, currently validating only count as part of integration testing.
        # pd.testing.assert_frame_equal(expected_df,actual_df)
