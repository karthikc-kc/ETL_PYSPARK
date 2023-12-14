import pandas as pd
from pandas import DataFrame

class Etl:
    @staticmethod
    def extract(file_path: str, delimiter: str) -> DataFrame:
        df = pd.read_csv(file_path, delimiter=delimiter)
        return df

    def column_rename(self, df: DataFrame, columns: dict) -> DataFrame:
        df = df.rename(columns=columns)
        return df

    def name_transformation(self, df: DataFrame, column) -> DataFrame:
        df[['first_name', 'last_name']] = df[column].str.split(n=1, expand=True)
        df = df.drop(column,axis=1)
        return df

    def date_transform(self, df: DataFrame, column, from_format, to_format) -> DataFrame:
        df[column] = pd.to_datetime(df[column], format=from_format)
        df[column] = df[column].dt.strftime(to_format)
        return df


if __name__ == '__main__':



    etl = Etl()
    # TODO : Need to pass from the config parameters
    # Extractions

    us_golf_df = etl.extract('/Users/karthikeyanchandrasekar/Desktop/hinge/resources/unity_golf_club.csv',
                             delimiter=',')
    us_soft_df = etl.extract('/Users/karthikeyanchandrasekar/Desktop/hinge/resources/us_softball_league.tsv',
                             delimiter='\t')

    # column renaming
    # TODO : We can take this from file-mapping (config) file

    column_rename_mapping = {"joined_league": "member_since", "date_of_birth": "dob", "us_state": "state"}

    us_soft_df = etl.column_rename(us_soft_df, columns=column_rename_mapping)

    # Data Tranformation

    us_soft_df = etl.date_transform(us_soft_df,column='dob', from_format= '%m/%d/%Y', to_format='%Y/%m/%d')

    # name tranformation

    us_soft_df = etl.name_transformation(us_soft_df, column='name')


