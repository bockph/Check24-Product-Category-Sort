import math

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
import sqlalchemy as sa
import pymysql
import json
import os
import difflib
import swifter
from pandarallel import pandarallel

pd.set_option('display.max_columns', 20)
from tqdm import tqdm

tqdm.pandas()
from statistics import median, mean

from haversine import haversine, Unit

hotel_to_pv_country_mapping = {'Bonaire, St. Eustatius und Saba': 'Bonaire, Sint Eustatius & Saba',
                               'Bosnien und Herzegovina': 'Bosnien-Herzegowina',
                               'Jungferninseln (GB)': 'Britische Jungferninseln',
                               'Jungferninseln (US)': 'Amerikanische Jungferninseln',
                               'Curaçao': 'Curacao',
                               'Großbritannien': 'Großbritannien & Nordirland',
                               'Nordmazedonien': 'Mazedonien',
                               'Honduras': 'Republik Honduras',
                               'Südkorea': 'Republik Korea (Südkorea)',
                               'St. Kitts und Nevis': 'Saint Kitts & Nevis',
                               'St. Lucia': 'Saint Lucia',
                               'St. Vincent & Grenadinen': 'Saint Vincent & die Grenadinen',
                               'St. Martin': 'Saint-Martin',
                               'St. Maarten': 'Sint Maarten',
                               'Elfenbeinküste': "Elfenbeinküste (Côte d'Ivoire)",
                               'Réunion': 'La Réunion',
                               'Weißrussland': 'Weißrussland (Belarus)',
                               'Republik Kongo': 'Kongo',
                               'Brunei Darussalam': 'Brunei',
                               'São Tomé und Principe': 'São Tomé & Príncipe',
                               'Trinidad und Tobago': 'Trinidad & Tobago',
                               'Tschechische Republik': 'Tschechien',
                               'Turks- & Caicosinseln': 'Turks & Caicosinseln',
                               'Kongo': 'Republik Kongo'
                               }


# def sanity_check_countries(group_countries,location_countries):

class FuzzyMerge:
    """
    Works like pandas merge except merges on approximate matches.
    """

    def __init__(self, **kwargs):
        self.left = kwargs.get("left")
        self.right = kwargs.get("right")
        self.left_on = kwargs.get("left_on")
        self.right_on = kwargs.get("right_on")
        self.how = kwargs.get("how", "inner")
        self.cutoff = kwargs.get("cutoff", 0.8)

    def merge(self) -> pd.DataFrame:
        temp = self.right.copy()
        temp[self.left_on] = [
            self.get_closest_match(x, self.left[self.left_on]) for x in temp[self.right_on]
        ]

        df = self.left.merge(temp, on=self.left_on, how=self.how)
        df["similarity_percent"] = df.apply(lambda x: self.similarity_score(x[self.left_on], x[self.right_on]), axis=1)

        return df

    def get_closest_match(self, left: pd.Series, right: pd.Series) -> str or None:
        matches = difflib.get_close_matches(left, right, cutoff=self.cutoff)

        return matches[0] if matches else None

    @staticmethod
    def similarity_score(left: pd.Series, right: pd.Series) -> int:
        return int(round(difflib.SequenceMatcher(a=str(left).lower(), b=str(right).lower()).ratio(), 2) * 100)


def create_connection():
    mysql_url = sa.engine.URL.create("mysql+pymysql", username="root", password="2553Freemander10!", host="localhost",
                                     port="3306", database="pv_data_import")
    sqlEngine = create_engine(mysql_url, pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    return dbConnection


def import_pv_data(from_db=False):
    if from_db:
        # 1. clean data

        print("Start pulling PV DATA from DB")
        dbConnection = create_connection()
        ##
        # Should not be needed anymore
        ##

        # location = pd.read_sql(sa.text("select * from location"), con=dbConnection)
        # location[['id', 'object_id', 'parent_object_id', 'parent_id', 'geo_lat', 'geo_long', 'group_id']] = location[
        #     ['id', 'object_id', 'parent_object_id', 'parent_id', 'geo_lat', 'geo_long', 'group_id']].apply(
        #     pd.to_numeric, errors='coerce')
        #
        # location.to_pickle("db_location.p")
        # location = pd.read_pickle("db_location.p")

        group = pd.read_sql(
            sa.text("select * from `group` where group.type in (\"COUNTRY\",\"REGION\",\"CITY\",\"REGION_GROUP\")"),
            con=dbConnection)

        group['id'] = group['id'].astype(int)

        group.to_pickle("db_group.p")
        # group = pd.read_pickle("db_group.p")

        pv_locations = pd.read_sql(
            sa.text("select distinct country_id, country_name, country_code, country_autocomplete, country_updated,"
                    "region_id,region_name,region_airport_code,region_alternative_name,region_autocomplete,region_updated,"
                    "city_id,city_name,city_airport_code,city_autocomplete,city_latitude,city_longitude,city_updated"
                    " from `import_pv_data`"), con=dbConnection)

        pv_locations[['country_id', 'region_id', 'city_id']] = pv_locations[
            ['country_id', 'region_id', 'city_id']].astype(int)  # apply(pd.to_numeric, errors='coerce')
        pv_locations[['city_latitude', 'city_longitude']] = pv_locations[
            ['city_latitude', 'city_longitude']].apply(pd.to_numeric, errors='coerce').astype(float)
        pv_locations.to_pickle("db_pv_locations.p")
        # pv_data = pd.read_pickle("db_pv_data.p")

        pv_hotels = pd.read_sql(
            sa.text(
                "select hotel_id as id,hotel_name as name,latitude,longitude,accommodation_type,city_id,region_id,country_id"
                " from `import_pv_data`"), con=dbConnection)
        pv_hotels[['id', 'city_id']] = pv_hotels[['id', 'city_id']].astype(int)  # apply(pd.to_numeric, errors='coerce')
        pv_hotels.to_pickle("db_pv_hotel.p")
        # hotel = pd.read_pickle("db_pv_hotel.p")

        # locations_merged = pd.read_sql(sa.text("select * from locations_merged"), con=dbConnection)
        #
        # locations_merged.to_pickle("db_locations_merged.p")
        # locations_merged = pd.read_pickle("db_locations_merged.p")

        dbConnection.close()
    else:
        # locations_merged = pd.read_pickle("db_locations_merged.p")
        group = pd.read_pickle("db_group.p")
        # location = pd.read_pickle("db_location.p")
        pv_locations = pd.read_pickle("db_pv_locations.p")
        pv_hotels = pd.read_pickle("db_pv_hotel.p")

    return group, pv_locations, pv_hotels  # locations_merged, group, location, pv_data, pv_hotels


def import_pv_data_new(from_db=False):
    if from_db:
        # 1. clean data

        print("Start pulling PV DATA from DB")
        dbConnection = create_connection()

        group = pd.read_sql(sa.text("select * from `group` where type in ('CITY','REGION','COUNTRY')"),
                            con=dbConnection)
        location_data = pd.read_sql(sa.text("select * from `location_data`"), con=dbConnection)

        group.to_pickle("db_group_new.p")
        location_data.to_pickle("db_location_data.p")

        dbConnection.close()
    else:
        group = pd.read_pickle("db_group_new.p")
        location_data = pd.read_pickle("db_location_data.p")

    return group, location_data,


def compare_new_pv_data():
    group, location_data = import_pv_data_new(True)
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    merged = pd.merge(group, location_data, left_on=["aggregate_post_count", 'type'], right_on=["pv_data_id", 'type'],
                      how="left")

    ###
    # 1. Combine Regions
    ###
    hotel_regions = hotel_hotels[['region_name', 'country_name']].drop_duplicates(keep="first").rename(
        columns={"country_name": "hotel_country_name"})
    hotel_regions['hotel_combined'] = (hotel_regions['region_name'].astype(str) + hotel_regions['hotel_country_name'])

    pv_regions = location_data[location_data.type == "REGION"][['name', 'country_id']]
    pv_countries = location_data[location_data.type == "COUNTRY"][['name', 'id']].rename(
        columns={"name": "pv_country_name"})
    pv_regions = pd.merge(pv_regions, pv_countries, left_on="country_id", right_on="id", how="left")
    pv_regions['pv_combined'] = (pv_regions['name'].astype(str) + pv_regions['pv_country_name']).drop(
        columns=["id", "country_id"])

    country_merged_regions = pd.merge(pv_regions, hotel_regions, how="inner", left_on="pv_country_name",
                                      right_on="hotel_country_name")
    country_merged_regions['similarity'] = country_merged_regions.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['name']).lower(), str(x['region_name']).lower()).ratio(), axis=1)

    # hotel_countries = hotel_hotels[['country_name','region_name']].drop_duplicates(keep="first")

    # combined = pd.merge(pv_regions,hotel_regions, left_on="name", right_on="region_name", how="outer")

    # combined['similarity'] = combined.apply(lambda x: difflib.SequenceMatcher(None, str(x['name']).lower(), str(x['region_name']).lower()).ratio(), axis=1)

    # pv_regions.name=pv_regions.name.astype(str)
    # hotel_regions.region_name=hotel_regions.region_name.astype(str)

    # combined_2 =FuzzyMerge(left=pv_regions, right=hotel_regions, left_on=['name','pv_country_name'], right_on=['region_name','hotel_country_name'], how="left", cutoff=0.3).merge()

    # merged = pd.merge(group[group.type=="REGION"], location_data[location_data.type=="REGION"], left_on="aggregate_post_count", right_on="pv_data_id", how="left")
    print("stop")


def import_hotel_data(from_db=True):
    if from_db:
        print("Start pulling Hotel DATA from DB")

        dbConnection = create_connection()

        hotel_location = pd.read_sql(sa.text("select "
                                             "id, pauschalreise_location_type, pauschalreise_location_id, source_created_at,"
                                             " source_deleted_at,source_updated_at,hotels_count,homes_count, translation_de_name,translation_en_name, type,"
                                             "use_in_autocompleter, ST_AsText(point) as point, preposition,"
                                             "aliases from hotel_data_locations"), con=dbConnection)

        hotel_location = hotel_location[(hotel_location.translation_de_name != "NOT_ASSIGNED") & (
                hotel_location.translation_en_name != "NOT_ASSIGNED")]
        hotel_location = hotel_location[(hotel_location.translation_de_name != "NULL_COUNTRY") & (
                hotel_location.translation_en_name != "NULL_COUNTRY")]
        hotel_location = hotel_location[(hotel_location.translation_de_name != "NULL_CITY") & (
                hotel_location.translation_en_name != "NULL_CITY")]

        hotel_location.loc[~hotel_location.point.str.contains("POINT", na=False, regex=False), 'point'] = "POINT(0 0)"

        hotel_location[['longitude', 'latitude']] = hotel_location['point'].str.replace("POINT\(", "").str.replace("\)",
                                                                                                                   "").str.split(
            " ",
            expand=True)
        hotel_location[
            ['id', 'pauschalreise_location_id', 'type', 'longitude', 'latitude', 'hotels_count', 'homes_count']] = \
            hotel_location[
                ['id', 'pauschalreise_location_id', 'type', 'longitude', 'latitude', 'hotels_count',
                 'homes_count']].apply(
                pd.to_numeric, errors='coerce')
        hotel_location.to_pickle("db_hotel_location.p")

        hotel = pd.read_sql(
            sa.text(
                "select id, name as hotel_name, pauschalreise_hotel_id,pauschalreise_city_id,pauschalreise_area_id,city_name,country_name,city_id,coordinates"
                " from hotel_data_hotels  "), con=dbConnection)
        hotel[['id', 'pauschalreise_hotel_id', "pauschalreise_city_id", "pauschalreise_area_id", 'city_id']] = hotel[
            ['id', 'pauschalreise_hotel_id', "pauschalreise_city_id", "pauschalreise_area_id", 'city_id']].apply(
            pd.to_numeric, errors='coerce').astype(int, errors='ignore')
        hotel.loc[~hotel.coordinates.str.contains("POINT", na=False, regex=False), 'coordinates'] = "POINT(0 0)"

        hotel[['longitude', 'latitude']] = hotel['coordinates'].str.replace("POINT\(", "").str.replace("\)",
                                                                                                       "").str.split(
            " ",
            expand=True)
        hotel.to_pickle("db_hotel_hotel.p")

        dbConnection.close()
    else:
        hotel_location = pd.read_pickle("db_hotel_location.p")
        hotel = pd.read_pickle("db_hotel_hotel.p")

    return hotel_location, hotel


def insert_group_data(group, locations_merged, print_info=False):
    locations_merged[['group_id', 'name', 'type', 'is_active', 'latitude', 'longitude']] = group[
        ['id', 'name', 'type', 'is_active', 'geo_lat', 'geo_long']]
    if print_info:
        print("\n______________________\nGroup Inserted Results\n______________________\n")
        print(locations_merged.info())
    return locations_merged


def insert_location_data(location, locations_merged, print_info=False):
    location_and_group = pd.merge(locations_merged,
                                  location[location.type.isin(['COUNTRY', 'REGION_GROUP', 'CITY', 'REGION'])],
                                  left_on=["group_id", "type", "name"], right_on=["group_id", "type", "name"],
                                  how="outer")

    # print("Group Entries: {} Location Entries:{} Merged Entries:{}".format(
    #     len(locations_merged),
    #     len(location[location.type.isin(
    #         ['COUNTRY', 'REGION_GROUP', 'CITY',
    #          'REGION'])]),
    #     len(location_and_group)))
    # # TODO what shall we do with duplicate groups that have no entry in location? --> check depencencies and delete?
    # print("Not Same Latitude: {}  Longitude: {}".format(
    #     (location_and_group['latitude'] != location_and_group['geo_lat']).sum(),
    #     (location_and_group['longitude'] != location_and_group['geo_long']).sum()))

    # TODO this is only filling up fields but not taking care about name etc. of new columns
    location_and_group['pv_data_id'] = location_and_group['pv_data_id'].combine_first(
        location_and_group['object_id'])  # This is needed to get the pv_data_id
    # store country pv_data_id to pv_country_id
    location_and_group.loc[location_and_group.type == "COUNTRY", "pv_country_id"] = location_and_group.loc[
        location_and_group.type == "COUNTRY", "pv_data_id"]  # .combine_first(location_and_group.loc[location_and_group.type == "COUNTRY", "parent_object_id"])
    # store region pv_data_id to pv_region_id
    location_and_group.loc[location_and_group.type == "REGION", "pv_region_id"] = location_and_group.loc[
        location_and_group.type == "REGION", "pv_data_id"]  # .combine_first(location_and_group.loc[location_and_group.type == "COUNTRY", "parent_object_id"])
    # retrieve and store country_id for regions using parent_object_id
    location_and_group.loc[location_and_group.type == "REGION", "pv_country_id"] = location_and_group.loc[
        location_and_group.type == "REGION", "pv_country_id"].combine_first(
        location_and_group.loc[location_and_group.type == "REGION", "parent_object_id"])
    # retrieve and store region_id for cities using parent_object_id
    location_and_group.loc[location_and_group.type == "CITY", "pv_region_id"] = location_and_group.loc[
        location_and_group.type == "CITY", "pv_region_id"].combine_first(
        location_and_group.loc[location_and_group.type == "CITY", "parent_object_id"])

    # retrieve and store country_id for cities using merge on regions
    region_country_data = location_and_group[location_and_group.type == "REGION"][['pv_data_id', 'pv_country_id']]
    merge_result = \
        pd.merge(location_and_group[['pv_region_id']], region_country_data, left_on="pv_region_id",
                 right_on="pv_data_id",
                 how="left")['pv_country_id']
    location_and_group['pv_country_id'] = location_and_group['pv_country_id'].combine_first(
        merge_result)

    # TODO generally created / updated at has to be adapted

    location_and_group.rename(
        columns={'is_active_x': 'is_active', 'created_at_x': 'created_at', 'updated_at_x': 'updated_at'},
        inplace=True)
    print(location_and_group.info())
    location_and_group.drop(
        columns=['created_at_y', 'updated_at_y', 'is_active_y', 'geo_lat', 'geo_long', 'parent_id', 'object_id',
                 'parent_object_id', 'deleted_at', 'cover_image', 'hierarchy_level'], inplace=True)

    # print("Check if merged table has same columns as target table")
    # print(location_and_group.columns == locations_merged.columns)

    # print("Number of Countries: {} Regions: {} Cities: {}".format(
    #     len(location_and_group[location_and_group.type == "COUNTRY"]),
    #     len(location_and_group[location_and_group.type == "REGION"]),
    #     len(location_and_group[location_and_group.type == "CITY"])))
    if print_info:
        print("\n______________________\nLocation inserted results\n______________________\n")
        print(location_and_group.info())
    return location_and_group
    # "region_id,region_name,region_airport_code,region_alternative_name,region_autocomplete,region_updated,"
    #                               "city_id,city_name,city_airport_code,city_autocomplete,city_latitude,city_longitude,city_updated"


def insert_new_pv_data(pv_data, locations_merged, print_info=False):
    print(pv_data.info())
    # pv_data=pv_data[pv_data.active=="Y"]
    # 28771 --> delta 1077
    # 28763
    countries = pv_data[
        ['country_id', 'country_name', 'country_code', 'country_autocomplete',
         'country_updated']].drop_duplicates().reset_index(
        drop=True)
    regions = pv_data[['country_id', 'region_id', 'region_name', 'region_airport_code', 'region_alternative_name',
                       'region_autocomplete', 'region_updated']].drop_duplicates().reset_index(
        drop=True)  # TODO add region_id
    print("HELLO")

    cities = pv_data[
        ['country_id', 'region_id', 'region_name', 'city_id', 'city_name', 'city_airport_code', 'city_autocomplete',
         'city_latitude',
         'city_longitude', 'city_updated']].drop_duplicates().reset_index(
        drop=True)

    # Pauschalreise delivers data with wrong assignment of city_id to region_id
    # Hence we do two steps.

    # 1. load correct regions
    region_id_counts = regions.region_id.value_counts()
    duplicates = regions[regions.region_id.isin(region_id_counts.index[region_id_counts.gt(1)])].sort_values(
        by=['region_id'])
    # load wrong regions

    city_id_counts = cities.city_id.value_counts()
    duplicates = cities[cities.city_id.isin(city_id_counts.index[city_id_counts.gt(1)])].sort_values(by=['city_id'])
    duplicates_short = duplicates[['country_id', 'region_id', 'city_id', 'city_name']]
    location_short = locations_merged[['pv_country_id', 'pv_region_id', 'pv_data_id', 'name', 'type']]
    comparison_data = pd.merge(duplicates_short, location_short[location_short.type == "CITY"], left_on="city_id",
                               right_on="pv_data_id", how="left")
    comparison_data = comparison_data[
        (comparison_data.region_id != comparison_data.pv_region_id) | (comparison_data.name.isnull())]
    comparison_data.to_csv("tmp_comparison.csv")
    comparison_data['drop_value'] = comparison_data['city_name']

    # merge deleted rows into cities
    cities = pd.merge(cities, comparison_data[['country_id', 'region_id', 'city_id', 'drop_value']],
                      left_on=['country_id', 'region_id', 'city_id'], right_on=['country_id', 'region_id', 'city_id'],
                      how="left")
    # delete where values exist
    cities = cities[cities.drop_value.isnull()]
    # TODO CHECK If CITY Groups are missing now

    print("Number of Countries: {} Regions: {} Cities: {}".format(len(countries), len(regions), len(cities)))

    countries[['pv_region_id', 'airport_code', 'latitude', 'longitude']] = None
    countries.rename(columns={'country_id': 'pv_data_id', 'country_name': 'name', 'country_code': 'nationality_code',
                              'country_autocomplete': 'autocomplete', 'country_updated': 'source_updated_at'},
                     inplace=True)
    countries['pv_country_id'] = countries['pv_data_id']
    countries['type'] = "COUNTRY"

    regions.rename(columns={'country_id': 'pv_country_id', 'region_id': 'pv_data_id', 'region_name': 'name',
                            'region_airport_code': 'airport_code', 'region_autocomplete': 'autocomplete',
                            'region_updated': 'source_updated_at'}, inplace=True)
    regions.drop(columns=['region_alternative_name'], inplace=True)
    regions[['latitude', 'longitude', 'nationality_code']] = None
    regions['pv_region_id'] = regions['pv_data_id']
    regions['type'] = "REGION"

    cities.rename(columns={'country_id': 'pv_country_id', 'region_id': 'pv_region_id', 'city_id': 'pv_data_id',
                           'city_name': 'name', 'city_airport_code': 'airport_code',
                           'city_autocomplete': 'autocomplete', 'city_latitude': 'latitude',
                           'city_longitude': 'longitude', 'city_updated': 'source_updated_at'}, inplace=True)
    cities[['nationality_code']] = None
    cities['type'] = "CITY"
    location_from_csv = pd.concat([countries, regions, cities], ignore_index=True)

    location_merged_with_csv = pd.merge(locations_merged, location_from_csv, left_on=['pv_data_id', 'name', 'type'],
                                        right_on=['pv_data_id', 'name', 'type'], how="outer").reset_index(drop=True)

    location_merged_with_csv.rename(
        columns={'pv_region_id_x': 'pv_region_id', 'pv_country_id_x': 'pv_country_id', 'latitude_x': 'latitude',
                 'longitude_x': 'longitude', 'nationality_code_x': 'nationality_code',
                 'airport_code_x': 'airport_code', 'autocomplete_x': 'autocomplete',
                 'source_updated_at_x': 'source_updated_at'}, inplace=True)

    y_data_tmp = location_merged_with_csv[
        ['pv_region_id_y', 'pv_country_id_y', 'latitude_y', 'longitude_y', 'nationality_code_y',
         'airport_code_y', 'autocomplete_y', 'source_updated_at_y']]

    y_data_tmp.rename(
        columns={'pv_region_id_y': 'pv_region_id', 'pv_country_id_y': 'pv_country_id', 'latitude_y': 'latitude',
                 'longitude_y': 'longitude', 'nationality_code_y': 'nationality_code',
                 'airport_code_y': 'airport_code', 'autocomplete_y': 'autocomplete',
                 'source_updated_at_y': 'source_updated_at'}, inplace=True)

    location_merged_with_csv = location_merged_with_csv.combine_first(y_data_tmp)
    # print("\n______________________\ngoing to compare\n______________________\n")
    # test = location_merged_with_csv['airport_code'].compare(location_merged_with_csv['airport_code_y'])
    # print("Airport Code Success") if len(test) == 0 else print("Airport Code Fail:\n{}".format(test.info()))
    #
    # test = location_merged_with_csv['autocomplete'].compare(location_merged_with_csv['autocomplete_y'])
    # print("Autocomplete Success") if len(test) == 0 else print("Autocomplete Fail:\n{}".format(test.info()))
    #
    # test = location_merged_with_csv['latitude'].compare(location_merged_with_csv['latitude_y'])
    # print("Latitude Success") if len(test) == 0 else print("Latitude Fail:\n{}".format(test.info()))
    # print(test[~test.other.isnull()])
    # test = location_merged_with_csv['longitude'].compare(location_merged_with_csv['longitude_y'])
    # print(test[~test.other.isnull()])
    #
    # print("Longitude Success") if len(test) == 0 else print("Longitude Fail:\n{}".format(test.info()))
    #
    # test = location_merged_with_csv['nationality_code'].compare(location_merged_with_csv['nationality_code_y'])
    # print("Nationality Code Success") if len(test) == 0 else print("Nationality Code Fail:\n{}".format(test.info()))
    #
    # test = location_merged_with_csv['pv_country_id'].compare(location_merged_with_csv['pv_country_id_y'])
    # print("Country ID Success") if len(test) == 0 else print("Country ID Fail:\n{}".format(test.info()))
    #
    # test = location_merged_with_csv['pv_region_id'].compare(location_merged_with_csv['pv_region_id_y'])
    # print("Region ID Success") if len(test) == 0 else print("Region ID Fail:\n{}".format(test.info()))
    # test = location_merged_with_csv['source_updated_at'].compare(location_merged_with_csv['source_updated_at_y'])
    # print("Source Updated At Success") if len(test) == 0 else print("Source Updated At Fail:\n{}".format(test.info()))

    location_merged_with_csv.drop(
        columns=['airport_code_y', 'latitude_y', 'longitude_y', 'nationality_code_y', 'pv_country_id_y',
                 'pv_region_id_y', 'source_updated_at_y', 'autocomplete_y'], inplace=True)
    if print_info:
        print("\n______________________\nPV Data Insert Results\n______________________\n")

        print(location_merged_with_csv.info())
    return location_merged_with_csv


def clean_up_pv_data_locations():
    locations_merged, group, location, pv_data, _ = import_pv_data(from_db=False)

    #   1. Import Location from group table
    locations_merged = insert_group_data(group, locations_merged, print_info=False)

    print("--------------")

    #   2. Update Countries from location table
    locations_merged = insert_location_data(location, locations_merged, print_info=False)
    print("--------------")
    #   3. Update Countries from new pv-community-export (2) table
    locations_merged = insert_new_pv_data(pv_data, locations_merged, print_info=False)

    regions_groups = locations_merged[locations_merged.type == "REGION_GROUP"]  # OK
    # print(regions_groups.info())
    countries = locations_merged[locations_merged.type == "COUNTRY"]
    print(countries.info())
    regions = locations_merged[locations_merged.type == "REGION"]
    print(regions.info())
    cities = locations_merged[locations_merged.type == "CITY"]
    print(cities[cities.latitude.isnull()])

    locations_merged.to_pickle("db_locations_merged_final.p")
    # print(cities.info())

    # TODO 1. What are those 18 rows without pv_data_id
    # 3. Compare stats country-country, region-region, city-city
    # 4. Compare if geocoordinates would change
    # 5. make back comparison if the rows with groupid changed elemental data
    # 7. why is image empt<
    # 8. why is airport code with so many empties?
    # 9. source_udated_at: Überhaupt interessant? Zwei Ebenen
    #       inhaltlich <-- kann man eigentlich zurückverfolgen wenn der need wirklich besteht
    #       performance für sql iterationen <-- mit Kollegen besprechen


# def cleanup_hotel_data():

def migrate_hotel_locations():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, _, _, _, pv_hotels = import_pv_data(from_db=False)
    pv_data = pd.read_pickle("db_locations_merged_final.p")
    new_external_data_needed = False

    def calculate_unique_identifier(cities_w_externaldata, new_data_source):
        if 'unique_identifier' not in cities_w_externaldata.columns:
            cities_w_externaldata['unique_identifier'] = None
            cities_w_externaldata['not_unique'] = None
        else:
            cities_w_externaldata['not_unique'] = cities_w_externaldata.duplicated(subset=['unique_identifier'],
                                                                                   keep=False)
            cities_w_externaldata.loc[cities_w_externaldata['unique_identifier'].isnull(), 'not_unique'] = True
        cities_w_externaldata.loc[cities_w_externaldata['not_unique'] == True, 'unique_identifier'] = \
            cities_w_externaldata.loc[cities_w_externaldata['not_unique'] == True, new_data_source]

        return cities_w_externaldata

    #### Here startes the cool stuff
    hotel_locations_cities = hotel_locations[hotel_locations.type == 3].reset_index(drop=True)
    if new_external_data_needed:
        hotel_external_geodata = parse_json("hotel_location_map_tiler")
        hotel_external_geodata.to_pickle("hotel_external_geodata.p")

    hotel_external_geodata = pd.read_pickle("hotel_external_geodata.p")

    hotel_locations_cities = pd.merge(hotel_locations_cities, hotel_external_geodata, left_on="id", right_on="id",
                                      how="left")

    ###Get the ids of the highest location data
    # check for duplicates and marke in extra collumn
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'municipality_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'municipal_district_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'neighbourhood_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'place_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'postal_code_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'address_id_external')
    hotel_locations_cities = calculate_unique_identifier(hotel_locations_cities, 'poi_id_external')

    # get external pv_data
    pv_data_cities = pv_data[pv_data.type == "CITY"].reset_index(drop=True)
    if new_external_data_needed:
        pv_external_geodata = parse_json("pv_location_map_tiler")
        pv_external_geodata.to_pickle("pv_external_geodata.p")

    pv_external_geodata = pd.read_pickle("pv_external_geodata.p")
    # add external geo data to pv_data
    pv_data_cities = pd.merge(pv_data_cities, pv_external_geodata, left_on="pv_data_id", right_on="id", how="left")

    ###Get the ids of the highest location data
    # check for duplicates and marke in extra collumn
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'municipality_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'municipal_district_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'neighbourhood_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'place_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'postal_code_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'address_id_external')
    pv_data_cities = calculate_unique_identifier(pv_data_cities, 'poi_id_external')

    merged_on_unique_external = pd.merge(
        hotel_locations_cities[['unique_identifier', 'id', 'translation_de_name']].dropna(),
        pv_data_cities[['unique_identifier', 'pv_data_id', 'name']].dropna(),
        left_on="unique_identifier", right_on="unique_identifier", how="inner")
    merged_on_unique_external.to_pickle("pv_merged_hotel_maptiler.p")


def create_location_mapping():
    geo_mapping = pd.read_pickle("pv_merged_hotel_maptiler.p")
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, _, _, _, pv_hotels = import_pv_data(from_db=False)
    pv_locations = pd.read_pickle("db_locations_merged_final.p")
    pv_countries = pv_locations[pv_locations.type == "COUNTRY"]
    pv_locations = pv_locations[pv_locations.type == "CITY"]
    hotel_locations = hotel_locations[hotel_locations.type == 3]
    # TODO only Cities
    # get hotel: pauschalreise_city_id, name, country_name, country_id based on merge with id and city_id
    hotel_data = pd.merge(
        hotel_hotels[['country_name', 'city_name', 'pauschalreise_hotel_id', 'city_id', 'hotel_name']],
        hotel_locations[['id', 'translation_de_name', 'latitude', 'longitude', 'pauschalreise_location_id']],
        left_on=['city_id'], right_on=['id'], how="left")
    hotel_data.rename(columns={'translation_de_name': 'hotel_city_name1', 'city_name': 'hotel_city_name2',
                               'country': 'hotel_country_name', 'id': 'hotel_location_id'}, inplace=True)
    hotel_data.drop(columns=['city_id'], inplace=True)

    # get pv: pv_data_id, name, pv_country_name, based on merge with object pv_data_id
    pv_data = pd.merge(pv_hotels[['id', 'city_id', 'name']],
                       pv_locations[['pv_data_id', 'name', 'pv_country_id', 'latitude', 'longitude']],
                       left_on=['city_id'], right_on=['pv_data_id'], how="left")
    pv_data.rename(columns={"pv_data_id": "pv_location_id", "name_y": "pv_city_name", "pv_country_id": "pv_country_id"},
                   inplace=True)

    # merge on pauschalreise city_id and pvdata_id to get the mappings
    mappings = pd.merge(hotel_data, pv_data, left_on="pauschalreise_hotel_id", right_on="id", how="inner")
    # mappings['similarity'] = mappings.apply(
    #     lambda x: difflib.SequenceMatcher(None, str(x['hotel_name']).lower(),
    #                                       str(x['name_x']).lower()).ratio() * 100, axis=1)
    # mappings=mappings[mappings.similarity>60]
    # tmp
    unique = pd.read_csv("sorted_unique_mappings.csv", decimal=",")
    mapping_filtered = pd.read_csv("location_mapping_with_hotelpv.csv")
    no_mapping_filtered = pd.read_csv("location_mapping_rest.csv")
    all = pd.concat([unique, mapping_filtered, no_mapping_filtered])

    # drop unique ones
    counts = mappings.groupby(
        ['hotel_location_id', 'pauschalreise_location_id', 'pv_location_id', 'pv_city_name', 'hotel_city_name1',
         'latitude_x', 'longitude_x', 'latitude_y', 'longitude_y']).size().reset_index(name='count').sort_values(
        by=['count'], ascending=False)

    counts['distance'] = counts.apply(
        lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y'])), axis=1)
    counts['hotel_mapping'] = counts.pauschalreise_location_id.astype(int) == counts.pv_location_id.astype(int)

    # counts_clean = counts.drop_duplicates(subset=['hotel_location_id'], keep=False)
    # counts_clean = counts_clean.drop_duplicates(subset=['pv_location_id'], keep=False)
    # TODO this is the actual correct query to drop duplicates
    counts_clean = counts[~counts.duplicated("pv_location_id", keep=False)][
        ~counts.duplicated("hotel_location_id", keep=False)].sort_values("hotel_location_id")

    counts_clean['similarity'] = counts_clean.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name1']).lower(),
                                          str(x['pv_city_name']).lower()).ratio() * 100, axis=1)
    counts_clean['distance'] = counts_clean.apply(
        lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y']),
                            unit=Unit.KILOMETERS), axis=1)

    counts_clean.drop(columns=['latitude_x', 'longitude_x', 'latitude_y', 'longitude_y'], inplace=True)
    counts_clean['weighted_count'] = counts_clean['count']

    counts_clean['weighted_distance'] = (counts_clean['distance']).pow(2) / 9 * 10 * (
        -1)  # (counts_w_dupplicates['distance']/max_distance).pow(2)/9*(-1)#
    # counts_w_dupplicates['distance']=counts_w_dupplicates['weighted_distance']
    counts_clean['weighted_similarity'] = counts_clean['similarity'] / 10  # *0#0*0.36
    counts_clean['rank'] = counts_clean['weighted_count'] + counts_clean['weighted_distance'] + counts_clean[
        'weighted_similarity']
    counts_clean.sort_values(by=['rank'], ascending=False, inplace=True)

    hotel_cities_extra = pd.read_csv("hotel_cities.csv", sep=";")
    hotel_countries_extra = pd.read_csv("hotel_countries.csv", sep=";")
    hotel_city_country = pd.merge(hotel_cities_extra[['id', 'country_id']], hotel_countries_extra[['id', 'name']],
                                  left_on="country_id", right_on="id", how="left")
    hotel_city_country.rename(columns={'id_x': 'city_id', 'name': 'hotel_country_name'}, inplace=True)

    pv_city_country = pd.merge(pv_locations[['pv_data_id', 'pv_country_id']], pv_countries[['pv_data_id', 'name']],
                               left_on="pv_country_id", right_on="pv_data_id", how="left")
    pv_city_country.rename(columns={'name': 'pv_country_name'}, inplace=True)
    sorted_tmp = pd.merge(counts_clean, pv_city_country[['pv_data_id_x', 'pv_country_name']], left_on="pv_location_id",
                          right_on="pv_data_id_x", how="left")
    sorted_tmp = pd.merge(sorted_tmp, hotel_city_country[['city_id', 'hotel_country_name']],
                          left_on="hotel_location_id", right_on="city_id", how="left")
    sorted = sorted_tmp.drop(columns=['pv_data_id_x', 'city_id'])
    sorted['country_similarity'] = sorted.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_country_name']).lower(),
                                          str(x['pv_country_name']).lower()).ratio() * 100, axis=1)
    sorted_unique_mappings = sorted
    # sorted_unique_mappings=sorted[((sorted['count'] > 5).astype(int) + (sorted['similarity'] > 80).astype(int) + (                 sorted['distance'] < 10).astype(int) )== 3]
    sorted_unique_mappings['Fullfilled Criterias'] = (sorted_unique_mappings['count'] > 5).astype(int) + (
            sorted_unique_mappings['similarity'] > 80).astype(int) + (
                                                             sorted_unique_mappings['distance'] < 10).astype(int)
    sorted_unique_mappings.drop(columns=['weighted_count', 'weighted_distance', 'weighted_similarity'], inplace=True)

    # sorted_unique_mappings.sort_values("similarity",ascending=False).sort_values("hotel_mapping",ascending=False)\
    #     .sort_values("Fullfilled Criterias",ascending=False).to_csv("sorted_unique_mappings.csv",decimal=",")

    counts_w_dupplicates = counts[~counts.index.isin(counts_clean.index)]
    counts_w_dupplicates = pd.merge(counts_w_dupplicates, geo_mapping[['id', 'pv_data_id', 'unique_identifier']],
                                    left_on=['hotel_location_id', 'pv_location_id'], right_on=['id', 'pv_data_id'],
                                    how="left").drop(columns=['pv_data_id'])
    counts_w_dupplicates['similarity'] = counts_w_dupplicates.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name1']).lower(),
                                          str(x['pv_city_name']).lower()).ratio() * 100, axis=1)
    counts_w_dupplicates['distance'] = counts_w_dupplicates.apply(
        lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y']),
                            unit=Unit.KILOMETERS), axis=1)
    counts_w_dupplicates.drop(columns=['latitude_x', 'longitude_x', 'latitude_y', 'longitude_y'], inplace=True)
    max_count = median(counts_w_dupplicates['count'])
    max_distance = median(counts_w_dupplicates['distance'])

    # Problem:
    # nur gruppe hat ab ca 2-3k keine aussagekraft
    # nur entfernung schwierig, da teilweise falsche koordinaten, geo konstellationen in denen eine andere stadt näher ist
    # nur name insbesondere bei partial matches problematisch,häufig auch direkt nebeneinanderliegende stadtteile mit unterschiedlichem namen aber sonst kein match möglich

    # 1km /10 Hotels /75% -->0.2;5;0.75-->
    # 3km radius is as important as 10 Hotels or a 100% match
    ###below 3km impact is little, above exponentially stronger

    # if similarity is 100% and distance is below 10km -->it is a match (hypothesis, correct city is more important) or city must have a count of 30 or so
    # if similarity is 100% and distance is above 20km (big size city) -->distance is no criteria anymore (hypothesis hotel count is more important)
    # cutoff 95% percentile?

    #

    counts_w_dupplicates['weighted_count'] = counts_w_dupplicates['count']

    counts_w_dupplicates['weighted_distance'] = (counts_w_dupplicates['distance']).pow(2) / 9 * 10 * (
        -1)  # (counts_w_dupplicates['distance']/max_distance).pow(2)/9*(-1)#
    # counts_w_dupplicates['distance']=counts_w_dupplicates['weighted_distance']
    counts_w_dupplicates['weighted_similarity'] = counts_w_dupplicates['similarity'] / 10  # *0#0*0.36
    counts_w_dupplicates['rank'] = counts_w_dupplicates['weighted_count'] + counts_w_dupplicates['weighted_distance'] + \
                                   counts_w_dupplicates['weighted_similarity']

    index_sim_smalldist = (counts_w_dupplicates.similarity == 10) & (counts_w_dupplicates['distance'] < (10))
    counts_w_dupplicates.loc[index_sim_smalldist, 'rank'] = counts_w_dupplicates.loc[
                                                                index_sim_smalldist, 'weighted_count'] * 0.25 + \
                                                            counts_w_dupplicates.loc[
                                                                index_sim_smalldist, 'weighted_distance'] + \
                                                            counts_w_dupplicates.loc[
                                                                index_sim_smalldist, 'weighted_similarity']  # counts_w_dupplicates['weighted_count']+counts_w_dupplicates['weighted_distance']+counts_w_dupplicates['weighted_similarity']

    index_sim_bigdist = (counts_w_dupplicates.similarity == 10) & (counts_w_dupplicates['distance'] > (10))
    counts_w_dupplicates.loc[index_sim_bigdist, 'rank'] = counts_w_dupplicates.loc[
                                                              index_sim_bigdist, 'weighted_count'] + \
                                                          counts_w_dupplicates.loc[
                                                              index_sim_bigdist, 'weighted_similarity']
    # counts_w_dupplicates['rank']=counts_w_dupplicates.apply(lambda x: x['count']/max_count*0.2-x['distance']/max_distance*0.7+0.1*x['similarity'],axis=1)
    # sorted = counts_w_dupplicates.sort_values(by=['similarity'],ascending=False).sort_values(by=['count'],ascending=False).sort_values(by=['distance'],ascending=True).reset_index()#.sort_values(by=['unique_identifier'],ascending=False).reset_index(drop=False)

    counts_w_dupplicates.drop(
        columns=['weighted_count', 'weighted_distance', 'weighted_similarity', 'unique_identifier'], inplace=True)
    counts_w_dupplicates.rename(columns={'hotel_mapping': 'hotel_based_pvmapping', 'id': 'external_geo_match',
                                         'pauschalreise_location_id': 'hotel_pv_id_mapping',
                                         'hotel_city_name1': 'hotel_city_name', 'count': 'common_hotels',
                                         'similarity': 'textual_similarity'}, inplace=True)

    counts_w_dupplicates[['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id']] = counts_w_dupplicates[
        ['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id']].astype(int)
    # counts_w_dupplicates['id1']=counts_w_dupplicates['id'].astype('boolean')
    counts_w_dupplicates['external_geo_match'] = counts_w_dupplicates['external_geo_match'].fillna(0).astype('bool')

    sorted = counts_w_dupplicates.sort_values(by=['rank'], ascending=False)
    # correct = sorted[sorted.hotel_mapping == True]
    # correct_filtered = correct.drop_duplicates(subset=['pv_location_id'], keep='first').reset_index(drop=True)
    # correct_filtered = correct_filtered.drop_duplicates(subset=['hotel_location_id'], keep='first')
    # tbd = sorted[sorted.hotel_mapping == False]
    hotel_cities_extra = pd.read_csv("hotel_cities.csv", sep=";")
    hotel_countries_extra = pd.read_csv("hotel_countries.csv", sep=";")
    hotel_city_country = pd.merge(hotel_cities_extra[['id', 'country_id']], hotel_countries_extra[['id', 'name']],
                                  left_on="country_id", right_on="id", how="left")
    hotel_city_country.rename(columns={'id_x': 'city_id', 'name': 'hotel_country_name'}, inplace=True)

    pv_city_country = pd.merge(pv_locations[['pv_data_id', 'pv_country_id']], pv_countries[['pv_data_id', 'name']],
                               left_on="pv_country_id", right_on="pv_data_id", how="left")
    pv_city_country.rename(columns={'name': 'pv_country_name'}, inplace=True)
    sorted_tmp = pd.merge(sorted, pv_city_country[['pv_data_id_x', 'pv_country_name']], left_on="pv_location_id",
                          right_on="pv_data_id_x", how="left")
    sorted_tmp = pd.merge(sorted_tmp, hotel_city_country[['city_id', 'hotel_country_name']],
                          left_on="hotel_location_id", right_on="city_id", how="left")
    sorted = sorted_tmp.drop(columns=['pv_data_id_x', 'city_id'])
    sorted['country_similarity'] = sorted.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_country_name']).lower(),
                                          str(x['pv_country_name']).lower()).ratio() * 100, axis=1)
    sorted = sorted[(sorted['common_hotels'] > 5).astype(int) + (sorted['textual_similarity'] > 60).astype(int) + (
            sorted['distance'] < 10).astype(int) >= 2]

    sorted_filtered = sorted.drop_duplicates(subset=['hotel_location_id'], keep='first')

    sorted_filtered = sorted_filtered.drop_duplicates(subset=['pv_location_id'], keep='first').reset_index(drop=True)
    sorted_filtered = sorted_filtered.drop_duplicates(subset=['hotel_location_id'], keep='first')

    mapping_filtered = sorted_filtered[sorted_filtered.hotel_based_pvmapping == True]
    no_mapping_filtered = sorted_filtered[sorted_filtered.hotel_based_pvmapping == False]
    # mapping_filtered.to_csv("location_mapping_with_hotelpv.csv")
    # no_mapping_filtered.to_csv("location_mapping_rest.csv")

    not_mapped = pv_locations[(~pv_locations.pv_data_id.isin(counts_clean.pv_location_id)) & (
        ~pv_locations.pv_data_id.isin(sorted_filtered.pv_location_id))]
    not_mapped.drop(
        columns=['airport_code', 'autocomplete', 'created_at', 'drop_value', 'group_id', 'hotel_data_id', 'id_x',
                 'id_y', 'image', 'nationality_code', 'source_updated_at', 'type', 'updated_at'], inplace=True)

    missing_mappings = pv_locations[(~pv_locations.pv_data_id.isin(counts_clean.pv_location_id)) & (
        ~pv_locations.pv_data_id.isin(sorted_filtered.pv_location_id))]
    active_hotel_locations = hotel_locations[(hotel_locations.hotels_count > 0) | (hotel_locations.homes_count > 0)]
    active_hotel_locations = active_hotel_locations[
        ['id', 'translation_de_name', 'latitude', 'longitude', 'pauschalreise_location_type',
         'pauschalreise_location_id']]
    active_hotel_locations = active_hotel_locations[active_hotel_locations.pauschalreise_location_type == 'city']
    active_hotel_locations = active_hotel_locations[~active_hotel_locations.id.isin(sorted_filtered.hotel_location_id)]
    # round geocoordinates to 3 digits
    active_hotel_locations[['latitude', 'longitude']] = active_hotel_locations[['latitude', 'longitude']].round(1)
    missing_mappings[['latitude', 'longitude']] = missing_mappings[['latitude', 'longitude']].round(1)

    last_mapping = pd.merge(active_hotel_locations, missing_mappings, left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude'], how="inner")

    last_mapping['textual_similarity'] = last_mapping.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['translation_de_name']).lower(),
                                          str(x['name']).lower()).ratio() * 100, axis=1)

    # make a key column to merge based on close matches
    # missing_mappings['Fuzzy_Key'] = missing_mappings.name.map(lambda x: difflib.get_close_matches(x, active_hotel_locations[:500].translation_de_name, n=1, cutoff=0.6))

    # since the values in our Fuzzy_Key column are lists, we have to convert them to strings
    # missing_mappings['Fuzzy_Key'] = missing_mappings.Fuzzy_Key.apply(lambda x: ''.join(map(str, x)))

    # correct_filtered2 = correct_filtered[(sorted_filtered['common_hotels'] > 5) | (sorted_filtered['textual_similarity'] > 60)]
    # correct_filtered3 = correct_filtered[(correct_filtered['common_hotels'] > 5).astype(int) + (correct_filtered['textual_similarity'] > 60).astype(int)+(correct_filtered['distance']<10).astype(int)>=2]
    # sorted_filtered[(~sorted_filtered.pv_location_id.isin(correct_filtered.pv_location_id))&(~sorted_filtered.hotel_location_id.isin(correct_filtered.hotel_location_id))]

    # counts_w_duplicates.to_csv("location_mapping_via_hotelconnection_ambigious.csv")
    # counts_clean.to_csv("location_mapping_via_hotelconnection_unique.csv")
    # hotel_locations[(hotel_locations.hotels_count>0) |(hotel_locations.homes_count>0)].to_csv("active_hotel_locations.csv")
    # hotel_locations.to_csv("active_hotel_locations_full.csv")
    # pv_locations.to_csv("pv_locations.csv")

    # counts_w_duplicates.sort_values(by=['similarity'],ascending=False).sort_values(by=['count'],ascending=False).sort_values(by=['distance'],ascending=True).sort_values(by=['unique_identifier'],ascending=False).reset_index(drop=False)
    # #counts_w_duplicates[counts_w_duplicates.hotel_location_id==224503]
    print("stop")
    # Probleme
    # Touristenspot ist nicht der Hotelspot. Pauschalreise wählt ersteres für seine Hotels, Hotel die tatsächlichen



def location_mapping_v2():
    geo_mapping = pd.read_pickle("pv_merged_hotel_maptiler.p")
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, pv_cities, pv_hotels = import_pv_data(from_db=False)
    pv_cities.rename(
        columns={"city_id": "pv_location_id", "city_name": "pv_city_name", "country_id": "pv_country_id",
                 "country_name": "pv_country_name"},
        inplace=True)
    # pv_locations = pd.read_pickle("db_locations_merged_final.p")
    hotel_locations.loc[hotel_locations['translation_de_name'] == "", 'translation_de_name'] = hotel_locations.loc[
        hotel_locations['translation_de_name'] == "", 'translation_en_name']

    hotel_cities = hotel_locations[hotel_locations.type == 3]
    # TODO only Cities
    # get hotel: pauschalreise_city_id, name, country_name, country_id based on merge with id and city_id
    hotel_cities['hotel_has_hotels'] = ''
    hotel_cities.loc[((hotel_cities.homes_count + hotel_cities.hotels_count) > 0), 'hotel_has_hotels'] = 'x'
    hotel_data = pd.merge(
        hotel_hotels[['country_name', 'city_name', 'pauschalreise_hotel_id', 'city_id', 'hotel_name']],
        hotel_cities[['id', 'translation_de_name', 'latitude', 'longitude', 'pauschalreise_location_id',
                      'pauschalreise_location_type', 'hotel_has_hotels']],
        left_on=['city_id'], right_on=['id'], how="left")
    hotel_data.rename(columns={'translation_de_name': 'hotel_city_name1', 'city_name': 'hotel_city_name2',
                               'country': 'hotel_country_name', 'id': 'hotel_location_id'}, inplace=True)
    hotel_data.drop(columns=['city_id'], inplace=True)

    # get pv: pv_data_id, name, pv_country_name, based on merge with object pv_data_id
    pv_data = pd.merge(pv_hotels[['id', 'city_id', 'name']],
                       pv_cities[
                           ['pv_location_id', 'pv_city_name', 'pv_country_id', 'city_latitude', 'city_longitude']],
                       left_on="city_id", right_on=['pv_location_id'], how="left")

    # merge on pauschalreise hoteld_id and hotel hotel_id
    locations_connections_via_hotels = pd.merge(hotel_data, pv_data, left_on="pauschalreise_hotel_id", right_on="id",
                                                how="inner")

    # get mappings with counts
    mappings = locations_connections_via_hotels.groupby(
        ['hotel_location_id', 'pauschalreise_location_id', 'pauschalreise_location_type', 'pv_location_id',
         'pv_city_name', 'hotel_city_name1',
         'city_latitude', 'city_longitude', 'latitude', 'longitude', 'hotel_has_hotels']).size().reset_index(
        name='count').sort_values(
        by=['count'], ascending=False)
    mappings['pv_location_id'] = mappings['pv_location_id'].astype(int)
    mappings['pauschalreise_location_id'] = mappings['pauschalreise_location_id'].astype(int)
    mappings['hotel_mapping'] = mappings.pauschalreise_location_id == mappings.pv_location_id

    mappings['distance'] = mappings.apply(
        lambda x: haversine((x['city_latitude'], x['city_longitude']), (x['latitude'], x['longitude']),
                            unit=Unit.KILOMETERS), axis=1)

    mappings['similarity'] = mappings.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name1']).lower(),
                                          str(x['pv_city_name']).lower()).ratio() * 100, axis=1)

    mappings.drop(columns=['city_latitude', 'city_longitude', 'latitude', 'longitude', ], inplace=True)

    mappings['weighted_distance'] = (mappings['distance']).pow(2) / 9 * 10 * (
        -1)
    mappings['weighted_similarity'] = mappings['similarity'] / 10  # *0#0*0.36
    mappings['rank'] = mappings['count'] + mappings['weighted_distance'] + mappings[
        'weighted_similarity']

    # counts_w_dupplicates = counts[~counts.index.isin(counts_clean.index)]

    index_sim_smalldist = (mappings.similarity == 10) & (mappings['distance'] < (10))
    mappings.loc[index_sim_smalldist, 'rank'] = mappings.loc[index_sim_smalldist, 'count'] * 0.25 + \
                                                mappings.loc[
                                                    index_sim_smalldist, 'weighted_distance'] + \
                                                mappings.loc[
                                                    index_sim_smalldist, 'weighted_similarity']  # counts_w_dupplicates['weighted_count']+counts_w_dupplicates['weighted_distance']+counts_w_dupplicates['weighted_similarity']

    index_sim_bigdist = (mappings.similarity == 10) & (mappings['distance'] > (10))
    mappings.loc[index_sim_bigdist, 'rank'] = mappings.loc[index_sim_bigdist, 'count'] + \
                                              mappings.loc[
                                                  index_sim_bigdist, 'weighted_similarity']

    mappings.drop(
        columns=['weighted_distance', 'weighted_similarity'], inplace=True)
    mappings.rename(columns={'hotel_mapping': 'hotel_based_pvmapping',
                             'pauschalreise_location_id': 'hotel_pv_id_mapping',
                             'pauschalreise_location_type': 'hotel_pv_type_mapping',
                             'hotel_city_name1': 'hotel_city_name', 'count': 'common_hotels',
                             'similarity': 'textual_similarity'}, inplace=True)

    mappings[['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id']] = mappings[
        ['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id']].astype(int)

    # add country data
    hotel_cities_extra = pd.read_csv("hotel_cities.csv", sep=";")
    hotel_countries_extra = pd.read_csv("hotel_countries.csv", sep=";")
    hotel_city_country = pd.merge(hotel_cities_extra[['id', 'country_id']], hotel_countries_extra[['id', 'name']],
                                  left_on="country_id", right_on="id", how="left")
    hotel_city_country.rename(columns={'id_x': 'city_id', 'name': 'hotel_country_name'}, inplace=True)

    mappings = pd.merge(mappings, pv_cities[['pv_location_id', 'pv_country_name']], left_on="pv_location_id",
                        right_on="pv_location_id", how="left")
    mappings = pd.merge(mappings, hotel_city_country[['city_id', 'hotel_country_name']],
                        left_on="hotel_location_id", right_on="city_id", how="left")
    mappings.drop(columns=['city_id'], inplace=True)
    mappings['country_similarity'] = mappings.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_country_name']).lower(),
                                          str(x['pv_country_name']).lower()).ratio() * 100, axis=1)
    mappings['Fullfilled Criterias'] = \
        (mappings['common_hotels'] > 10).astype(int) + \
        (mappings['textual_similarity'] >= 40).astype(int) * .5 + \
        (mappings['distance'] < 10).astype(int) * .5 + \
        (mappings['textual_similarity'] >= 90).astype(int) * .5 + \
        (mappings['distance'] < 3).astype(int) * .5

    mappings['Fullfilled Criterias'] = mappings['Fullfilled Criterias'] + (mappings['common_hotels'] > 100).astype(
        int) * 3  # +(mappings['textual_similarity'] ==100).astype(int)*.5
    mappings[['textual_similarity', 'country_similarity']] = mappings[
        ['textual_similarity', 'country_similarity']].astype(int)

    mappings['Fullfilled Criterias'] = mappings['Fullfilled Criterias'].round(1)
    mappings_cut = mappings[mappings['Fullfilled Criterias'] >= 1.5]

    # mappings_cut_drop = mappings_cut_drop[
    #     ~(mappings_cut_drop.duplicated("pv_location_id", keep=False) | mappings_cut_drop.duplicated("hotel_location_id",
    #                                                                                                 keep=False))]

    orig_1 = pd.read_csv("original_mapping_1.csv", delimiter=";")
    orig_2 = pd.read_csv("original_mapping_2.csv", delimiter=";")
    orig_3 = pd.read_csv("original_mapping_31.csv")
    all = pd.concat([orig_1, orig_2, orig_3])
    all_cleaned = all[~(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id",
                                                                                      keep=False))]
    # TODO we are removing already checked duplicates. These should be rechecked later
    # all_double = all[(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id", keep=False))]

    mappings_cut_drop = mappings_cut[
        ~(mappings_cut.duplicated("pv_location_id", keep=False) | mappings_cut.duplicated("hotel_location_id",
                                                                                          keep=False)) |
        ((mappings_cut.pv_location_id.isin(all_cleaned.pv_location_id)) & (
            mappings_cut.hotel_location_id.isin(all_cleaned.hotel_location_id)))]

    # false_drop = mappings.drop_duplicates("hotel_location_id", keep=False).drop_duplicates("pv_location_id", keep=False)
    # correct_drop = mappings[
    #     ~(mappings.duplicated("pv_location_id", keep=False) | mappings.duplicated("hotel_location_id", keep=False))]
    # wrong_entries = false_drop[~false_drop.pv_location_id.isin(correct_drop.pv_location_id)]

    all_2 = pd.merge(all_cleaned, mappings_cut, on=["hotel_location_id", "pv_location_id"], how="left")
    all_22 = all_2[
        ~(all_2.duplicated("pv_location_id", keep=False) | all_2.duplicated("hotel_location_id",
                                                                            keep=False)) | (
                    all_2['checked manually'] == "x")]
    all_222 = all_22[
        ~(all_2.duplicated("pv_location_id", keep=False) | all_2.duplicated("hotel_location_id",
                                                                            keep=False))]

    to_check = evaluate_current_city_mapping()
    to_check2 = pd.merge(to_check, mappings, on=["hotel_location_id", "pv_location_id"], how="left")
    # all['needs rework'] = ""
    # all.loc[all['pv_location_id'].isin(wrong_entries.pv_location_id), 'needs rework'] = "x"

    all_cleaned = all[~(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id",
                                                                                      keep=False))]

    all_cleaned.to_csv("full_mapping.csv", sep=";", decimal=",", index=False)
    all_cleaned['automatic select'] = "x"
    mappings_w_curated = pd.merge(mappings, all_cleaned[
        ['hotel_location_id', 'pv_location_id', 'checked manually', 'needs rework', 'automatic select']],
                                  on=['hotel_location_id', 'pv_location_id'], how="left")

    # take all mappings, mark the good ones,
    pv_left_alone_cities = pv_cities[~pv_cities.pv_location_id.isin(mappings_w_curated.pv_location_id)]
    # .rename(
    # columns={'name': 'pv_city_name', 'pv_data_id': 'pv_location_id'})
    # this is for calculating other mappings based on distance LATER
    pv_left_alone_cities_cleaned = pv_cities[~pv_cities.pv_location_id.isin(all_cleaned.pv_location_id)]  # \
    # .rename(
    # columns={'name': 'pv_city_name', 'pv_data_id': 'pv_location_id'})

    # pv_city_country['pv_location_id'] = pv_city_country['pv_data_id_x'].astype(int)
    # list_for_distance_mapping_pv = pd.merge(
    #     pv_left_alone_cities_cleaned[['pv_location_id', 'latitude', 'longitude', 'pv_city_name']],
    #     pv_city_country[['pv_location_id', 'pv_country_name']], on="pv_location_id", how="left")
    pv_left_alone_cities_cleaned.to_csv("list_for_distance_mapping_pv.csv", sep=";", decimal=".", index=False)

    pv_left_alone_cities = pv_left_alone_cities[['pv_location_id', 'pv_city_name']]

    hotel_left_alone_cities = hotel_cities[~hotel_cities.id.isin(mappings_w_curated.hotel_location_id)].rename(
        columns={'id': 'hotel_location_id', 'translation_de_name': 'hotel_city_name',
                 'pauschalreise_location_id': 'hotel_pv_id_mapping', 'id': 'hotel_location_id'})
    hotel_left_alone_cities_cleaned = hotel_cities[~hotel_cities.id.isin(all_cleaned.hotel_location_id)].rename(
        columns={'id': 'hotel_location_id', 'translation_de_name': 'hotel_city_name',
                 'pauschalreise_location_id': 'hotel_pv_id_mapping', 'id': 'hotel_location_id'})
    # this is for calculating other mappings based on distance
    hotel_city_country.rename(columns={'city_id': 'hotel_location_id'}, inplace=True)
    list_for_distance_mapping_hotel = pd.merge(
        hotel_left_alone_cities_cleaned[['hotel_location_id', 'latitude', 'longitude', 'hotel_city_name']],
        hotel_city_country[['hotel_location_id', 'hotel_country_name']], on="hotel_location_id", how="left")
    list_for_distance_mapping_hotel.to_csv("list_for_distance_mapping_hotel.csv", sep=";", decimal=".", index=False)
    hotel_left_alone_cities = hotel_left_alone_cities[
        ['hotel_location_id', 'hotel_city_name', 'hotel_pv_id_mapping', 'hotel_has_hotels']]

    # all all cities from pv and hotel that have no mapping at all
    all_cities = pd.concat([mappings_w_curated, pv_left_alone_cities, hotel_left_alone_cities])
    # convert float to int
    all_cities[
        ['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id', 'textual_similarity', 'country_similarity',
         'Fullfilled Criterias']] = all_cities[
        ['hotel_location_id', 'hotel_pv_id_mapping', 'pv_location_id', 'textual_similarity', 'country_similarity',
         'Fullfilled Criterias']].fillna(-1).astype(int)

    # read csv with more manually checked
    full_mapping_curated = pd.read_csv("full_mapping_curated.csv", delimiter=";")

    # if manually checked not set, merge it from csv with more manually checked
    all_cities_new = pd.merge(all_cities.drop(columns="checked manually"),
                              full_mapping_curated[['hotel_location_id', 'pv_location_id', 'checked manually']],
                              on=['hotel_location_id', 'pv_location_id'], how="left")

    all_cities_new.to_csv("all_cities_w_mapping.csv", sep=";", decimal=",", index=False)
    print("stop")


# cleanup of location mapping 18.11 mallorca
def location_mapping_v3():
    # get already curated mappings
    already_curated = pd.read_csv("cities_curated1711.csv").drop_duplicates(keep='first')
    already_curated_2 = pd.read_csv("cities_curated2011.csv", delimiter=";")  # .drop_duplicates(keep='first')
    already_curated_3 = pd.read_csv("cities_curated2111.csv", delimiter=";")  # .drop_duplicates(keep='first')

    already_curated = pd.concat(
        [already_curated_3, already_curated_2, already_curated])  # .drop_duplicates(keep='first')

    check_duplicates = already_curated[(already_curated.duplicated("pv_location_id", keep=False) |
                                        already_curated.duplicated("hotel_location_id", keep=False))]
    # already_curated=already_curated[~(already_curated.duplicated("pv_location_id", keep=False) |
    #                     already_curated.duplicated("hotel_location_id", keep=False))]

    # countrywise cross join
    # --> textual similarity
    # --> distance
    # calculate common hotels and add

    # get pv data
    _, pv_cities, pv_hotels = import_pv_data(from_db=False)
    ##select city,region,country data and geocoordinates of cities

    pv_cities.rename(
        columns={"city_id": "pv_city_id", "city_name": "pv_city_name", "country_id": "pv_country_id",
                 "country_name": "pv_country_name", "region_id": "pv_region_id", "region_name": "pv_region_name",
                 "city_latitude": "pv_city_latitude", "city_longitude": "pv_city_longitude"},
        inplace=True)

    ##make all to int
    pv_cities[['pv_city_id', 'pv_country_id', 'pv_region_id']] = pv_cities[
        ['pv_city_id', 'pv_country_id', 'pv_region_id']].fillna(-1).astype(float).astype(int)
    # get hotel data
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    # add hotel has hotels field, for later filtering
    hotel_locations['hotel_has_hotels'] = ''
    hotel_locations.loc[((hotel_locations.homes_count + hotel_locations.hotels_count) > 0), 'hotel_has_hotels'] = 'x'

    # get hotel hierarchy mapping
    hotel_location_hierarchy = pd.read_csv("hotel_cities.csv", delimiter=";")
    hotel_location_hierarchy.rename(
        columns={"id": "hotel_city_id", "country_id": "hotel_country_id", "primary_region_id": "hotel_region_id"},
        inplace=True)
    hotel_location_hierarchy = hotel_location_hierarchy[['hotel_city_id', "hotel_region_id", "hotel_country_id"]]

    # fill up empty hotel location names with english ones
    hotel_locations.loc[hotel_locations['translation_de_name'] == "", 'translation_de_name'] = hotel_locations.loc[
        hotel_locations['translation_de_name'] == "", 'translation_en_name']

    # get hotel cities only, select important fields and rename
    hotel_cities = hotel_locations[hotel_locations.type == 3][
        ['id', 'translation_de_name', 'longitude', 'latitude', 'pauschalreise_location_id', 'hotel_has_hotels']]
    hotel_cities.rename(columns={"id": "hotel_city_id", "translation_de_name": "hotel_city_name",
                                 "longitude": "hotel_city_longitude", "latitude": "hotel_city_latitude",
                                 "pauschalreise_location_id": "origmapping_pv_city_id"},
                        inplace=True)
    # get hotel regions only, select important fiealds and rename
    hotel_regions = hotel_locations[hotel_locations.type == 2][
        ['id', 'translation_de_name']]
    hotel_regions.rename(columns={"id": "hotel_region_id", "translation_de_name": "hotel_region_name"},
                         inplace=True)
    # convert region names using pv-hote-region-mapping
    region_mapping = pd.read_csv("region_mapping.csv", delimiter=",")
    replace_dict = pd.Series(region_mapping.pv_region_name.values, index=region_mapping.hotel_region_name).to_dict()
    hotel_regions['hotel_region_name'] = hotel_regions['hotel_region_name'].replace(replace_dict)

    # get hotel COUNTRIES only, select important fields and rename
    hotel_countries = hotel_locations[hotel_locations.type == 1][
        ['id', 'translation_de_name']]
    hotel_countries.rename(columns={"id": "hotel_country_id", "translation_de_name": "hotel_country_name"},
                           inplace=True)
    # rename hotel country names and assert that it is not different from pv
    hotel_countries['hotel_country_name'] = hotel_countries['hotel_country_name'].replace(hotel_to_pv_country_mapping)

    # merge region,country ids into cities
    hotel_cities = hotel_cities.merge(hotel_location_hierarchy, on="hotel_city_id", how="left")
    hotel_cities = hotel_cities.merge(hotel_regions, on="hotel_region_id", how="left")
    hotel_cities = hotel_cities.merge(hotel_countries, on="hotel_country_id", how="left")
    def calc_textual_similarity(x):
        return difflib.SequenceMatcher(None, str(x['hotel_city_name']).lower(),
                                       str(x['pv_city_name']).lower()).ratio() * 100
####Some Analysis and Export
    check_duplicates = pd.merge(check_duplicates, pv_cities, left_on="pv_location_id", right_on='pv_city_id',
                                how="left")
    check_duplicates = pd.merge(check_duplicates, hotel_cities, left_on="hotel_location_id", right_on='hotel_city_id',
                                how="left")

    tmp = pd.read_csv("cities_curated22_11_merged.csv", delimiter=";")
    tmp.sort_values("manual_check", ascending=False).drop_duplicates(subset=["pv_city_id", "hotel_city_id"],
                                                                     keep="first")

    tmp2 = tmp.sort_values("manual_check", ascending=False).drop_duplicates(subset=["pv_city_id", "hotel_city_id"],
                                                                            keep="first")
    tmp2[~(tmp2.duplicated("pv_city_id", keep=False) | tmp2.duplicated("hotel_city_id", keep=False))]
    tmp4 = pd.merge(tmp2, pv_cities, on="pv_city_id", how="left")
    tmp4 = pd.merge(tmp4, hotel_cities, on="hotel_city_id", how="left")
    tmp4['textual_similarity'] = tmp4.progress_apply(calc_textual_similarity, axis=1)
    tmp4['manhattan_distance'] = (tmp4['pv_city_latitude'].abs() -
                                                      tmp4['hotel_city_latitude'].abs()).abs() + (
                                                             tmp4['pv_city_longitude'].abs() -
                                                             tmp4[
                                                                 'hotel_city_longitude'].abs()).abs()

    tmp5 = tmp4[["pv_city_id", "hotel_city_id", "origmapping_pv_city_id", "pv_city_name", "hotel_city_name",
                 "textual_similarity", "manhattan_distance", "manual_check", "pv_region_name", "hotel_region_name",
                 "hotel_region_id", "pv_country_name", "hotel_has_hotels"]]######
    # remove already curated mappings
    pv_cities = pv_cities[~pv_cities.pv_city_id.isin(already_curated.pv_location_id)].reset_index(drop=True)
    hotel_cities = hotel_cities[~hotel_cities.hotel_city_id.isin(already_curated.hotel_location_id)].reset_index(
        drop=True)

    # TODO to make the calculations feasible we first go through cities that have hotels
    # In the second step we should go through cities that have NO hotels
    # [hotel_cities['hotel_has_hotels']!="x"]
    city_mappings_by_country = pd.merge(pv_cities, hotel_cities, left_on="pv_country_name",
                                        right_on="hotel_country_name", how="inner")

    city_mappings_by_country['manhattan_distance'] = (city_mappings_by_country['pv_city_latitude'].abs() -
                                                      city_mappings_by_country['hotel_city_latitude'].abs()).abs() + (
                                                             city_mappings_by_country['pv_city_longitude'].abs() -
                                                             city_mappings_by_country[
                                                                 'hotel_city_longitude'].abs()).abs()
    city_mappings_by_country_tmp = city_mappings_by_country
    city_mappings_by_country = city_mappings_by_country[city_mappings_by_country['manhattan_distance'] < 0.5]

    # pandarallel.initialize()

    def calc_textual_similarity(x):
        return difflib.SequenceMatcher(None, str(x['hotel_city_name']).lower(),
                                       str(x['pv_city_name']).lower()).ratio() * 100

    # city_mappings_by_country['similarity'] = city_mappings_by_country.progress_apply(
    #     lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name']).lower(),
    #                                       str(x['pv_city_name']).lower()).ratio() * 100, axis=1)
    city_mappings_by_country['similarity'] = city_mappings_by_country.progress_apply(calc_textual_similarity, axis=1)
    city_mappings_by_country_final = city_mappings_by_country[
        ['pv_city_id', 'hotel_city_id', 'pv_city_name', 'hotel_city_name', 'similarity', 'manhattan_distance',
         'pv_region_name', 'hotel_region_name', 'pv_country_name', 'hotel_country_name']]

    # TODO we are removing already checked duplicates. These should be rechecked later

    # orig_1 = pd.read_csv("original_mapping_1.csv", delimiter=";")
    # orig_2 = pd.read_csv("original_mapping_2.csv", delimiter=";")
    # orig_3 = pd.read_csv("original_mapping_31.csv")
    # all = pd.concat([orig_1, orig_2, orig_3])
    # all_cleaned = all[~(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id",
    #                                                      keep=False))]
    # #all_double = all[(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id", keep=False))]
    filtered = city_mappings_by_country_final[
        (city_mappings_by_country_final.manhattan_distance < 0.01) | (city_mappings_by_country_final.similarity > 80)]
    filtered = filtered[~(filtered.duplicated("pv_city_id", keep=False) | filtered.duplicated("hotel_city_id",
                                                                                              keep=False))]
    # removing duplicate mappings
    all_cleaned = all[~(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id",
                                                                                      keep=False))]



    print("stop")


def evaluate_current_city_mapping():
    full_mapping_curated = pd.read_csv("full_mapping_curated.csv", delimiter=";")
    orig_1 = pd.read_csv("original_mapping_1.csv", delimiter=";")
    orig_2 = pd.read_csv("original_mapping_2.csv", delimiter=";")
    orig_3 = pd.read_csv("original_mapping_31.csv")
    all = pd.concat([orig_1, orig_2, orig_3])
    all_cleaned_orig = all[~(all.duplicated("pv_location_id", keep=False) | all.duplicated("hotel_location_id",
                                                                                           keep=False))]

    full_mapping_curated = full_mapping_curated[full_mapping_curated['checked manually'] == "x"]
    all_cleaned = all_cleaned_orig[all_cleaned_orig['checked manually'] == "x"]
    left = pd.merge(full_mapping_curated, all_cleaned, on=['pv_location_id', 'hotel_location_id'], how="left")
    inner = pd.merge(full_mapping_curated, all_cleaned, on=['pv_location_id', 'hotel_location_id'], how="inner")
    right = pd.merge(full_mapping_curated, all_cleaned, on=['pv_location_id', 'hotel_location_id'], how="right")
    outer = pd.merge(full_mapping_curated, all_cleaned, on=['pv_location_id', 'hotel_location_id'], how="outer")

    new_all = pd.concat([all_cleaned[['hotel_location_id', 'pv_location_id']],
                         full_mapping_curated[['hotel_location_id', 'pv_location_id']]])
    new_all.drop_duplicates(keep="first", inplace=True)

    distance_mapping = pd.read_csv("city_distance_mapping_curated.csv", delimiter=";", decimal=",")
    distance_mapping = distance_mapping[distance_mapping['manual check'] == "x"]
    new_all2 = pd.concat([new_all, distance_mapping[['hotel_location_id', 'pv_location_id']]])
    new_all22 = new_all2.drop_duplicates(keep="first")

    cities_curated = new_all22[
        ~(new_all22.duplicated("pv_location_id", keep=False) | new_all22.duplicated("hotel_location_id",
                                                                                    keep=False))]
    cities_curated.to_csv("cities_curated1711.csv", decimal=",")

    # run mapping again

    # take regional mapping and assign to cities

    return distance_mapping
    print("stop")


def distance_mapping(pv_locations, hotel_locations):
    # input pv_locations: pandas Dataframe with pv_location_id,  latitude, longitude
    # input hotel_locations: pandas Dataframe with hotel_location_id,  latitude, longitude
    # create all possible combinations
    mix = pv_locations.merge(hotel_locations, how="cross")
    # mix = mix[:100000]
    mix[['latitude_x', 'longitude_x', 'latitude_y', 'longitude_y']] = mix[
        ['latitude_x', 'longitude_x', 'latitude_y', 'longitude_y']].fillna(-1).astype(float)
    mix['estimated_distance'] = (mix['latitude_x'].abs() - mix['latitude_y'].abs()).abs() + (
            mix['longitude_x'].abs() - mix['longitude_y'].abs()).abs()
    mix = mix[mix['estimated_distance'] < 5]

    # calculate the distance between all pv and hotel locations combinations
    # mix['distance'] = mix.progress_apply(lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y']),unit=Unit.KILOMETERS), axis=1)
    # sort by distance
    mix.sort_values(by=['estimated_distance'], ascending=True, inplace=True)
    # mix = mix[mix['distance'] < 30]
    # iterate through all possible mappings, select the ones with the smallest distance and remove all other combinations with the same pv or hotel location
    # this should keep the mapping with the smallest distance evenso there are other possible mappings with these ids. the other possible mappings should be deleted
    # this is done to avoid that the same pv location is mapped to multiple hotel locations

    final_mapping_list = []
    used_pv_locations = []
    used_hotel_locations = []
    for index, row in mix.iterrows():
        pv_location_id = row['pv_location_id']
        hotel_location_id = row['hotel_location_id']
        if (pv_location_id not in used_pv_locations) & (hotel_location_id not in used_hotel_locations):
            final_mapping_list.append({'pv_location_id': pv_location_id, 'hotel_location_id': hotel_location_id,
                                       'pv_name': row['pv_name'],
                                       'hotel_name': row['hotel_name'],
                                       'manhatten_distance': row['estimated_distance']})
            used_pv_locations.append(pv_location_id)
            used_hotel_locations.append(hotel_location_id)
            # pd.DataFrame(row[['pv_location_id','hotel_location_id','pv_city_name','hotel_city_name','distance']]))
            # final_mapping.append(row[['pv_location_id','hotel_location_id','distance']],inplace=True)
    final_mapping = pd.DataFrame(final_mapping_list)
    return final_mapping


def location_distance_mapping(country=True):
    pv_locations = pd.read_csv("list_for_distance_mapping_pv.csv", delimiter=";")
    hotel_locations = pd.read_csv("list_for_distance_mapping_hotel.csv", delimiter=";")
    tqdm.pandas()

    all_mappings_list = []

    if country:
        list_countries = pd.concat([pv_locations['pv_country_name'], hotel_locations['hotel_country_name']]).unique()
        hotel_to_pv = {'Bonaire, St. Eustatius und Saba': 'Bonaire, Sint Eustatius & Saba',
                       'Bosnien und Herzegovina': 'Bosnien-Herzegowina',
                       'Jungferninseln (GB)': 'Britische Jungferninseln',
                       'Jungferninseln (US)': 'Amerikanische Jungferninseln',
                       'Curaçao': 'Curacao',
                       'Großbritannien': 'Großbritannien & Nordirland',
                       # '':'La Réunion',
                       'Nordmazedonien': 'Mazedonien',
                       'Honduras': 'Republik Honduras',
                       'Südkorea': 'Republik Korea (Südkorea)',
                       'St. Kitts und Nevis': 'Saint Kitts & Nevis',
                       'St. Lucia': 'Saint Lucia',
                       'St. Vincent & Grenadinen': 'Saint Vincent & die Grenadinen',
                       'St. Martin': 'Saint-Martin',
                       'St. Maarten': 'Sint Maarten',
                       # '': 'Skandinavien',
                       # '': 'Swasiland',
                       'São Tomé und Principe': 'São Tomé & Príncipe',
                       'Trinidad und Tobago': 'Trinidad & Tobago',
                       'Tschechische Republik': 'Tschechien',
                       'Turks- & Caicosinseln': 'Turks & Caicosinseln'

                       }
        hotel_locations['hotel_country_name'] = hotel_locations['hotel_country_name'].replace(hotel_to_pv)

        list_countries = pd.concat([pv_locations['pv_country_name'], hotel_locations['hotel_country_name']]).unique()

        for country in tqdm(list_countries):
            all_mappings_list.append(distance_mapping(pv_locations[pv_locations['pv_country_name'] == country],
                                                      hotel_locations[
                                                          hotel_locations['hotel_country_name'] == country]))

    all_mappings_df = pd.concat(all_mappings_list)
    all_mappings_df['similarity'] = all_mappings_df.progress_apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name']).lower(),
                                          str(x['pv_city_name']).lower()).ratio() * 100, axis=1)
    all_mappings_df.to_csv("all_mappings.csv", sep=";", decimal=",", index=False)
    return all_mappings_df


def region_mapping_via_coordinates():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    groups, pv_locations, pv_hotels = import_pv_data(from_db=False)

    # pv_locations = pd.read_pickle("db_locations_merged_final.p")
    ## For doing mapping via city connections this has to be updated with another mapping list of Niko (contains ~5k more)
    city_mapping = pd.read_csv("full_mapping.csv", delimiter=";")[['pv_location_id', 'hotel_location_id']].rename(
        columns={"pv_location_id": "pv_city_id", "hotel_location_id": "hotel_city_id"})

    pv_regions = pv_locations[
        ['region_id', 'region_name', 'country_id', 'region_airport_code', 'region_alternative_name',
         'region_autocomplete', 'region_updated']].drop_duplicates()
    pv_regions = pv_regions[['region_id', 'region_name', 'country_id']].rename(
        columns={"country_id": "pv_country_id", "region_id": "pv_region_id", "region_name": "pv_region_name"})
    assert len(pv_regions) == len(pv_regions.pv_region_id.unique())

    pv_cities = pv_locations[
        ['city_id', 'city_name', 'region_id', 'country_id', 'city_airport_code', 'city_autocomplete', 'city_updated',
         'city_latitude', 'city_longitude']].drop_duplicates()
    pv_cities = pv_cities[
        ['city_id', 'city_name', 'region_id', 'country_id', 'city_latitude', 'city_longitude']].rename(
        columns={"country_id": "pv_country_id", "region_id": "pv_region_id", "city_id": "pv_city_id",
                 "city_name": "pv_city_name", 'city_latitude': 'pv_city_latitude',
                 'city_longitude': 'pv_city_longitude'})
    assert len(pv_cities) == len(pv_cities.pv_city_id.unique())

    # For doing mapping via city connections this has to be updated with another mapping list of Niko (contains ~5k more)
    city_mapping = pd.merge(city_mapping, pv_cities, left_on="pv_city_id", right_on="pv_city_id", how="inner")

    # we need a special file for hotel_cities that contains corresponding region_ids
    hotel_cities = pd.read_csv("hotel_cities.csv", sep=";")[['id', 'primary_region_id']].rename(
        columns={"id": "hotel_city_id", "primary_region_id": "hotel_region_id"})
    hotel_cities['hotel_region_id'] = hotel_cities['hotel_region_id'].fillna(-1).astype(int)

    city_mapping = pd.merge(city_mapping, hotel_cities, left_on="hotel_city_id", right_on="hotel_city_id", how="inner")

    hotel_regions = hotel_locations[hotel_locations.type == 2][
        ['id', 'translation_de_name', 'translation_en_name', 'pauschalreise_location_id']].rename(
        columns={"id": "hotel_region_id", "translation_de_name": "hotel_region_name",
                 "pauschalreise_location_id": "hotel_region_pauschalreise_id"})
    hotel_regions.loc[hotel_regions['hotel_region_name'] == "", 'hotel_region_name'] = hotel_regions.loc[
        hotel_regions['hotel_region_name'] == "", 'translation_en_name']

    already_mapped = pd.read_csv("region_curated_1.csv", delimiter=";")
    already_mapped_curated = already_mapped[already_mapped['checked manually'] == "x"][
        ['pv_region_id', 'hotel_region_id']]

    ##calculate pv region coordinates
    # calculate center for regions by city
    pv_region_center_cities = pv_cities.groupby(['pv_region_id']).agg(
        {'pv_city_latitude': 'mean', 'pv_city_longitude': 'mean'}).reset_index()
    pv_region_center_cities['pv_location_id'] = pv_region_center_cities['pv_region_id']
    pv_region_center_cities = pd.merge(pv_region_center_cities, pv_regions[['pv_region_id', 'pv_region_name']],
                                       left_on="pv_location_id", right_on="pv_region_id", how="left")
    pv_region_center_cities['pv_name'] = pv_region_center_cities['pv_region_name']
    pv_region_center_cities[['latitude', 'longitude']] = pv_region_center_cities[
        ['pv_city_latitude', 'pv_city_longitude']]
    # calculate center for regions by hotel
    pv_region_center_hotels = pv_hotels.groupby(['region_id']).agg(
        {'latitude': 'mean', 'longitude': 'mean'}).reset_index()
    pv_region_center_hotels['pv_location_id'] = pv_region_center_hotels['region_id']
    pv_region_center_hotels = pd.merge(pv_region_center_hotels, pv_regions[['pv_region_id', 'pv_region_name']],
                                       left_on="pv_location_id", right_on="pv_region_id", how="left")
    pv_region_center_hotels['pv_name'] = pv_region_center_hotels['pv_region_name']

    # calculate hotel region coordinates
    hotel_hotels['hotel_city_id'] = hotel_hotels['city_id'].fillna(-1).astype(int)
    hotels_w_regions = pd.merge(hotel_hotels, hotel_cities, on="hotel_city_id", how="inner")
    hotels_w_regions[['latitude', 'longitude']] = hotels_w_regions[['latitude', 'longitude']].fillna(-1).astype(float)
    hotel_region_center_hotels = hotels_w_regions.groupby(['hotel_region_id']).agg(
        {'latitude': 'mean', 'longitude': 'mean'}).reset_index()
    hotel_region_center_hotels['hotel_location_id'] = hotel_region_center_hotels['hotel_region_id'].fillna(-1).astype(
        int)
    hotel_region_center_hotels = pd.merge(hotel_region_center_hotels,
                                          hotel_regions[['hotel_region_id', 'hotel_region_name']],
                                          left_on="hotel_location_id", right_on="hotel_region_id", how="left")
    hotel_region_center_hotels['hotel_name'] = hotel_region_center_hotels['hotel_region_name']
    # calculate center for regions by
    hotel_cities = pd.merge(hotel_cities, hotel_locations[['id', 'longitude', 'latitude']], left_on="hotel_city_id",
                            right_on="id", how="inner")
    hotel_region_center_cities = hotel_cities.groupby(['hotel_region_id']).agg(
        {'latitude': 'mean', 'longitude': 'mean'}).reset_index()
    hotel_region_center_cities['hotel_location_id'] = hotel_region_center_cities['hotel_region_id'].fillna(-1).astype(
        int)
    hotel_region_center_cities = pd.merge(hotel_region_center_cities,
                                          hotel_regions[['hotel_region_id', 'hotel_region_name']],
                                          left_on="hotel_location_id", right_on="hotel_region_id", how="left")
    hotel_region_center_cities['hotel_name'] = hotel_region_center_cities['hotel_region_name']

    # drop already curated regions
    pv_region_center_cities1 = pv_region_center_cities[
        ~pv_region_center_cities['pv_location_id'].isin(already_mapped_curated['pv_region_id'])]
    pv_region_center_hotels1 = pv_region_center_hotels[
        ~pv_region_center_hotels['pv_location_id'].isin(already_mapped_curated['pv_region_id'])]
    hotel_region_center_hotels1 = hotel_region_center_hotels[
        ~hotel_region_center_hotels['hotel_location_id'].isin(already_mapped_curated['hotel_region_id'])]
    hotel_region_center_cities1 = hotel_region_center_cities[
        ~hotel_region_center_cities['hotel_location_id'].isin(already_mapped_curated['hotel_region_id'])]

    best_mapping_via_hotels = distance_mapping(pv_region_center_hotels1, hotel_region_center_hotels1)
    best_mapping_via_cities = distance_mapping(pv_region_center_cities1, hotel_region_center_cities1)

    ###So lets do the best mapping again with curated mappings
    # drop already curated regions
    pv_region_center_cities2 = pv_region_center_cities[
        pv_region_center_cities['pv_location_id'].isin(already_mapped_curated['pv_region_id'])]
    pv_region_center_hotels2 = pv_region_center_hotels[
        pv_region_center_hotels['pv_location_id'].isin(already_mapped_curated['pv_region_id'])]
    hotel_region_center_hotels2 = hotel_region_center_hotels[
        hotel_region_center_hotels['hotel_location_id'].isin(already_mapped_curated['hotel_region_id'])]
    hotel_region_center_cities2 = hotel_region_center_cities[
        hotel_region_center_cities['hotel_location_id'].isin(already_mapped_curated['hotel_region_id'])]

    best_mapping_via_hotels2 = distance_mapping(pv_region_center_hotels2, hotel_region_center_hotels2)
    best_mapping_via_cities2 = distance_mapping(pv_region_center_cities2, hotel_region_center_cities2)

    best_mapping_via_hotels2['checked_manually_hotelcoordinates'] = "x"

    best_mapping_via_hotels = pd.concat([best_mapping_via_hotels, best_mapping_via_hotels2])
    best_mapping_via_cities = pd.concat([best_mapping_via_cities, best_mapping_via_cities2])

    combined_mapping = pd.merge(best_mapping_via_hotels, best_mapping_via_cities, on=["pv_location_id"], how="outer")
    combined_mapping['textual_similarity_hotelcoordinates'] = combined_mapping.progress_apply(
        lambda x: 0 if str(x['hotel_name_x']) == "nan" else difflib.SequenceMatcher(None, str(x['pv_name_x']).lower(),
                                                                                    str(x[
                                                                                            'hotel_name_x']).lower()).ratio() * 100,
        axis=1).fillna(-1).astype(int)
    combined_mapping['textual_similarity_citycoordinates'] = combined_mapping.progress_apply(
        lambda x: 0 if str(x['hotel_name_y']) == "nan" else difflib.SequenceMatcher(None, str(x['pv_name_y']).lower(),
                                                                                    str(x[
                                                                                            'hotel_name_y']).lower()).ratio() * 100,
        axis=1).fillna(-1).astype(int)
    combined_mapping['textual_similarity_hotelcoordinates'] = combined_mapping[
        'textual_similarity_hotelcoordinates'].astype(int)
    combined_mapping.sort_values(by=['textual_similarity_hotelcoordinates', 'textual_similarity_citycoordinates'],
                                 ascending=False, inplace=True)

    combined_mapping['equal_mapping'] = combined_mapping.hotel_location_id_x == combined_mapping.hotel_location_id_y
    combined_mapping['hotel_location_id_hotelcoordinates'] = combined_mapping.hotel_location_id_x.fillna(-1).astype(
        int)  # .to_numeric(downcast="integer", errors='coerce')
    combined_mapping['hotel_location_id_citycoordinates'] = combined_mapping.hotel_location_id_y.fillna(-1).astype(
        int)  # .to_numeric(downcast="integer", errors='coerce')
    combined_mapping['pv_name'] = combined_mapping['pv_name_x'].combine_first(
        combined_mapping['pv_name_y'])
    combined_mapping['distance_hotelcoordinates'] = combined_mapping['manhatten_distance_x'].round(2)
    combined_mapping['distance_citycoordinates'] = combined_mapping['manhatten_distance_y'].round(2)
    combined_mapping['hotel_name_hotelcoordinates'] = combined_mapping['hotel_name_x']
    combined_mapping['hotel_name_citycoordinates'] = combined_mapping['hotel_name_y']
    combined_mapping.rename(
        columns={'hotel_name_x': 'hotel_name_hotelcoordinates', 'hotel_name_y': 'hotel_name_citycoordinates'},
        inplace=True)
    combined_mapping['checked_manually_citycoordinates'] = ""

    combined_mapping = combined_mapping[
        ['pv_location_id', 'hotel_location_id_hotelcoordinates', 'hotel_location_id_citycoordinates', 'equal_mapping',
         'checked_manually_hotelcoordinates', 'checked_manually_citycoordinates', 'pv_name',
         'hotel_name_hotelcoordinates', 'hotel_name_citycoordinates',
         'textual_similarity_hotelcoordinates', 'textual_similarity_citycoordinates', 'distance_hotelcoordinates',
         'distance_citycoordinates']]
    combined_mapping['checked_manually_hotelcoordinates'].fillna("", inplace=True)

    combined_mapping.reset_index(drop=True, inplace=True)

    print("stop")


def region_mapping_via_name():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    groups, pv_locations, pv_hotels = import_pv_data(from_db=False)

    pv_regions = pv_locations[
        ['region_id', 'region_name', 'country_id', 'country_name', 'region_airport_code', 'region_alternative_name',
         'region_autocomplete', 'region_updated']].drop_duplicates()
    pv_regions = pv_regions[['region_id', 'region_name', 'country_id', 'country_name']].rename(
        columns={"country_id": "pv_country_id", "region_id": "pv_region_id", "region_name": "pv_region_name",
                 "country_name": "pv_country_name"})
    assert len(pv_regions) == len(pv_regions.pv_region_id.unique())

    # combine with country
    # we need a special file for hotel_cities that contains corresponding region_ids
    hotel_cities = pd.read_csv("hotel_cities.csv", sep=";")[['id', 'primary_region_id', 'country_id']].rename(
        columns={"id": "hotel_city_id", "primary_region_id": "hotel_region_id", 'country_id': "hotel_country_id"})
    hotel_cities['hotel_region_id'] = hotel_cities['hotel_region_id'].fillna(-1).astype(int)

    hotel_locations.loc[hotel_locations['translation_de_name'] == "", 'translation_de_name'] = hotel_locations.loc[
        hotel_locations['translation_de_name'] == "", 'translation_en_name']

    hotel_countries = hotel_locations[hotel_locations.type == 1][['id', 'translation_de_name']].rename(
        columns={"id": "hotel_country_id", "translation_de_name": "hotel_country_name"})
    hotel_cities = pd.merge(hotel_cities, hotel_countries, on="hotel_country_id", how="left")

    hotel_regions = hotel_locations[hotel_locations.type == 2][
        ['id', 'translation_de_name', 'pauschalreise_location_id']].rename(
        columns={"id": "hotel_region_id", "translation_de_name": "hotel_region_name",
                 "pauschalreise_location_id": "hotel_region_pauschalreise_id"})
    hotel_regions = pd.merge(hotel_regions, hotel_cities[['hotel_region_id', 'hotel_country_name']].drop_duplicates(),
                             on="hotel_region_id", how="left")
    hotel_to_pv = {'Bonaire, St. Eustatius und Saba': 'Bonaire, Sint Eustatius & Saba',
                   'Bosnien und Herzegovina': 'Bosnien-Herzegowina',
                   'Jungferninseln (GB)': 'Britische Jungferninseln',
                   'Jungferninseln (US)': 'Amerikanische Jungferninseln',
                   'Curaçao': 'Curacao',
                   'Großbritannien': 'Großbritannien & Nordirland',
                   # '':'La Réunion',
                   'Nordmazedonien': 'Mazedonien',
                   'Honduras': 'Republik Honduras',
                   'Südkorea': 'Republik Korea (Südkorea)',
                   'St. Kitts und Nevis': 'Saint Kitts & Nevis',
                   'St. Lucia': 'Saint Lucia',
                   'St. Vincent & Grenadinen': 'Saint Vincent & die Grenadinen',
                   'St. Martin': 'Saint-Martin',
                   'St. Maarten': 'Sint Maarten',
                   # '': 'Skandinavien',
                   # '': 'Swasiland',
                   'São Tomé und Principe': 'São Tomé & Príncipe',
                   'Trinidad und Tobago': 'Trinidad & Tobago',
                   'Tschechische Republik': 'Tschechien',
                   'Turks- & Caicosinseln': 'Turks & Caicosinseln'

                   }
    hotel_regions['hotel_country_name'] = hotel_regions['hotel_country_name'].replace(hotel_to_pv)

    curated = pd.read_csv("region_mapping_w_distance_curated.csv", delimiter=";", decimal=",")
    curated_pv = curated.loc[(curated.checked_manually_hotelcoordinates.isin(["x", "?"]) | (
        curated.checked_manually_citycoordinates.isin(["x", "?"]))), 'pv_location_id'].drop_duplicates()
    curated_hotel1 = curated.loc[
        curated.checked_manually_hotelcoordinates.isin(["x", "?"]), 'hotel_location_id_hotelcoordinates']
    curated_hotel2 = curated.loc[
        curated.checked_manually_citycoordinates.isin(["x", "?"]), 'hotel_location_id_citycoordinates']
    curated_hotel = pd.concat([curated_hotel1, curated_hotel2]).drop_duplicates()

    curated_new = pd.read_csv("region_semifinal_curated_idlist.csv", delimiter=";", decimal=",")
    pv_regions_tomatch = pv_regions[~pv_regions.pv_region_id.isin(curated_new['pv_region_id'])]
    hotel_regions_tomatch = hotel_regions[~hotel_regions.hotel_region_id.isin(curated_new['hotel_region_id'])]

    # pv_regions_tomatch=pv_regions
    # hotel_regions_tomatch=hotel_regions

    # all_combinations =pd.merge(pv_regions_tomatch,hotel_regions_tomatch,how="cross")
    # all_combinations['similarity'] = all_combinations.progress_apply(
    #     lambda x: difflib.SequenceMatcher(None, str(x['hotel_region_name']).lower(),
    #                                       str(x['pv_region_name']).lower()).ratio() * 100, axis=1)

    list_countries = pd.concat([pv_regions['pv_country_name'], hotel_regions['hotel_country_name']]).unique()
    #
    all_mappings_list = []
    for country in tqdm(list_countries):
        all_combinations = pd.merge(pv_regions_tomatch[pv_regions_tomatch['pv_country_name'] == country],
                                    hotel_regions_tomatch[hotel_regions_tomatch['hotel_country_name'] == country],
                                    how="cross")
        all_combinations['similarity'] = all_combinations.apply(
            lambda x: difflib.SequenceMatcher(None, str(x['hotel_region_name']).lower(),
                                              str(x['pv_region_name']).lower()).ratio() * 100, axis=1)
        all_mappings_list.append(all_combinations)

    all_mappings_df = pd.concat(all_mappings_list)

    # #combine with country
    # # we need a special file for hotel_cities that contains corresponding region_ids
    # hotel_cities = pd.read_csv("hotel_cities.csv", sep=";")[['id', 'primary_region_id']].rename(
    #     columns={"id": "hotel_city_id", "primary_region_id": "hotel_region_id"})
    # hotel_cities['hotel_region_id'] = hotel_cities['hotel_region_id'].fillna(-1).astype(int)

    all_mappings_df2 = all_mappings_df[
        (all_mappings_df.similarity > 75) & (all_mappings_df.pv_country_name == all_mappings_df.hotel_country_name)]

    all_mappings_df2 = all_mappings_df2[~(
            all_mappings_df2.duplicated("pv_region_id", keep=False) | all_mappings_df2.duplicated("hotel_region_id",
                                                                                                  keep=False))]

    curated_mix = pd.merge(pv_regions, all_mappings_df2[
        ['pv_region_id', 'hotel_region_id', 'hotel_region_name', 'hotel_country_name', 'similarity']],
                           on="pv_region_id", how="left")
    curated_mix = pd.merge(curated_mix, curated, left_on="pv_region_id", right_on="pv_location_id", how="left")
    curated_mix.sort_values(['similarity', 'checked_manually_hotelcoordinates', 'checked_manually_citycoordinates'],
                            ascending=False, inplace=True)
    curated_mix.reset_index(inplace=True, drop=True)

    region_curated_1 = pd.read_csv("region_curated_1.csv", delimiter=";")

    curated_mix = pd.merge(curated_mix, region_curated_1[
        ['pv_region_id', 'hotel_region_id', 'hotel_region_name', 'similarity', 'checked manually']],
                           on=["pv_region_id"], how="left")

    curated_mix_ids = pd.read_csv("region_semifinal_curated_idlist.csv", delimiter=";", decimal=",")
    curated_w_name = pd.merge(curated_mix_ids, pv_regions[['pv_region_id', 'pv_region_name', 'pv_country_name']],
                              left_on="pv_region_id", right_on="pv_region_id", how="left")
    curated_w_name = pd.merge(curated_w_name,
                              hotel_regions[['hotel_region_id', 'hotel_region_name', 'hotel_country_name']],
                              left_on="hotel_region_id", right_on="hotel_region_id", how="left")
    print("stop")


def region_mapping():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    groups, pv_locations, pv_hotels = import_pv_data(from_db=False)

    # pv_locations = pd.read_pickle("db_locations_merged_final.p")
    ## For doing mapping via city connections this has to be updated with another mapping list of Niko (contains ~5k more)
    city_mapping = pd.read_csv("full_mapping.csv", delimiter=";")[['pv_location_id', 'hotel_location_id']].rename(
        columns={"pv_location_id": "pv_city_id", "hotel_location_id": "hotel_city_id"})

    pv_regions = pv_locations[
        ['region_id', 'region_name', 'country_id', 'region_airport_code', 'region_alternative_name',
         'region_autocomplete', 'region_updated']].drop_duplicates()
    pv_regions = pv_regions[['region_id', 'region_name', 'country_id']].rename(
        columns={"country_id": "pv_country_id", "region_id": "pv_region_id", "region_name": "pv_region_name"})
    assert len(pv_regions) == len(pv_regions.pv_region_id.unique())
    # pv_countries = pv_locations[
    #     ['country_id', 'country_name', 'country_code', 'country_autocomplete', 'country_updated']].drop_duplicates()
    # pv_countries = pv_countries[['country_id', 'country_name', ]].rename(
    #     columns={"country_id": "pv_country_id", "country_name": "pv_country_name"})
    # assert len(pv_countries) == len(pv_countries.pv_country_id.unique())
    pv_cities = pv_locations[
        ['city_id', 'city_name', 'region_id', 'country_id', 'city_airport_code', 'city_autocomplete', 'city_updated',
         'city_latitude', 'city_longitude']].drop_duplicates()
    pv_cities = pv_cities[
        ['city_id', 'city_name', 'region_id', 'country_id', 'city_latitude', 'city_longitude']].rename(
        columns={"country_id": "pv_country_id", "region_id": "pv_region_id", "city_id": "pv_city_id",
                 "city_name": "pv_city_name", 'city_latitude': 'pv_city_latitude',
                 'city_longitude': 'pv_city_longitude'})
    assert len(pv_cities) == len(pv_cities.pv_city_id.unique())

    # For doing mapping via city connections this has to be updated with another mapping list of Niko (contains ~5k more)
    city_mapping = pd.merge(city_mapping, pv_cities, left_on="pv_city_id", right_on="pv_city_id", how="inner")

    # we need a special file for hotel_cities that contains corresponding region_ids
    hotel_cities = pd.read_csv("hotel_cities.csv", sep=";")[['id', 'primary_region_id']].rename(
        columns={"id": "hotel_city_id", "primary_region_id": "hotel_region_id"})

    city_mapping = pd.merge(city_mapping, hotel_cities, left_on="hotel_city_id", right_on="hotel_city_id", how="inner")

    hotel_regions = hotel_locations[hotel_locations.type == 2][
        ['id', 'translation_de_name', 'translation_en_name', 'pauschalreise_location_id']].rename(
        columns={"id": "hotel_region_id", "translation_de_name": "hotel_region_name",
                 "pauschalreise_location_id": "hotel_region_pauschalreise_id"})
    hotel_regions.loc[hotel_regions['hotel_region_name'] == "", 'hotel_region_name'] = hotel_regions.loc[
        hotel_regions['hotel_region_name'] == "", 'translation_en_name']

    region_mapping = pd.merge(city_mapping, pv_regions, on=["pv_region_id", 'pv_country_id'], how="inner")
    region_mapping = pd.merge(region_mapping, hotel_regions, left_on="hotel_region_id", right_on="hotel_region_id",
                              how="inner")

    region_mapping[['pv_region_id', 'hotel_region_id', 'pv_country_id', 'hotel_city_id']] = region_mapping[
        ['pv_region_id', 'hotel_region_id', 'pv_country_id', 'hotel_city_id']].astype(int)

    # get mappings with counts
    mappings = region_mapping.groupby(
        ['pv_region_id', 'hotel_region_id', 'pv_region_name', 'hotel_region_name', 'translation_en_name',
         'hotel_region_pauschalreise_id']).size().reset_index(name='count').sort_values(
        by=['count'], ascending=False)
    mappings['hotel_mapping'] = mappings.pv_region_id == mappings.hotel_region_pauschalreise_id

    # mappings['distance'] = mappings.apply(
    #     lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y']),
    #                         unit=Unit.KILOMETERS), axis=1)

    mappings['similarity'] = mappings.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['pv_region_name']).lower(),
                                          str(x['hotel_region_name']).lower()).ratio() * 100, axis=1)

    mappings['weighted_similarity'] = mappings['similarity'] / 10  # *0#0*0.36
    mappings['rank'] = mappings['count'] + mappings[
        'weighted_similarity']

    mappings.sort_values(by=['count'], ascending=False, inplace=True)
    mappings2 = mappings[:250]
    mappings_cut_drop2 = mappings2[
        ~(mappings2.duplicated("pv_region_id", keep=False) | mappings2.duplicated("hotel_region_id",
                                                                                  keep=False))]

    # mappings['Fullfilled Criterias'] = \
    #     (mappings['common_hotels'] > 10).astype(int) + \
    #     (mappings['textual_similarity'] >= 40).astype(int) * .5 + \
    #     (mappings['distance'] < 10).astype(int) * .5 + \
    #     (mappings['textual_similarity'] >= 90).astype(int) * .5 + \
    #     (mappings['distance'] < 3).astype(int) * .5
    #
    # mappings['Fullfilled Criterias'] = mappings['Fullfilled Criterias'] + (mappings['common_hotels'] > 100).astype(
    #     int) * 3  # +(mappings['textual_similarity'] ==100).astype(int)*.5
    #
    # mappings_cut = mappings[mappings['Fullfilled Criterias'] >= 1.5]

    mappings_cut_drop = mappings[
        ~(mappings.duplicated("pv_region_id", keep=False) | mappings.duplicated("hotel_region_id",
                                                                                keep=False))]
    city_mix = pd.merge(city_mapping, mappings_cut_drop2, on="pv_region_id", how="inner")
    pv_hotels_from_region = pd.merge(pv_hotels, city_mix, left_on="city_id", right_on="pv_city_id",
                                     how="inner").dropna()
    # hotels.groupby(['region_id']).agg(
    #     latitude=pd.NamedAgg(column="latitude", aggfunc="mean"),
    #     longitude=pd.NamedAgg(column="longitude", aggfunc="mean")
    # )
    print("stop")


def clean_up_pv_data_hotels():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, _, _, _, pv_hotels = import_pv_data(from_db=False)

    # merge on pauschalreise city_id and pvdata_id to get the mappings
    mappings = pd.merge(hotel_hotels, pv_hotels, left_on="pauschalreise_hotel_id", right_on="id", how="inner")

    mappings['similarity'] = mappings.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_name']).lower(),
                                          str(x['name']).lower()).ratio() * 100, axis=1)

    print("stop")


def request_write_map_tiler(folder, id, longitude, latitude):
    result = None
    # url = "https://api.maptiler.com/geocoding/reverse?lat={}&lon={}&key=9Z4Z4ZQZ4Z4Z4Z4Z4Z4Z".format(latitude,longitude)
    url = "https://api.maptiler.com/geocoding/{},{}.json?key=Hlzt6D2yqvnwlixiZ60x&language=en".format(longitude,
                                                                                                      latitude)
    try:
        response = requests.get(url).json()
        with open(os.path.join(folder, str(id) + ".json"), 'w') as outfile:
            json.dump(response, outfile)
    except Exception as e:
        with open(os.path.join(folder, str(id) + ".json"), 'w') as outfile:
            json.dump("failed", outfile)
        print("ID {} failed".format(id))
        print(e)


def parse_json(folder):
    data = []
    counter = 0
    for file in tqdm(os.listdir(folder)):
        if file.endswith(".json"):
            with open(os.path.join(folder, file), 'r') as outfile:
                json_response = json.load(outfile)
            result = {'id': int(file.replace(".json", "")),
                      # 'country_external': None, 'country_id_external': None,
                      # 'region_external': None, 'region_id_external': None,
                      # 'subregion_external': None, 'subregion_id_external': None,
                      'county_external': None, 'county_id_external': None,
                      'joint_municipality_external': None, 'joint_municipality_id_external': None,
                      'joint_submunicipality_external': None, 'joint_submunicipality_id_external': None,
                      'municipality_external': None, 'municipality_id_external': None,
                      'municipal_district_external': None, 'municipal_district_id': None,
                      'locality_external': None, 'locality_id_external': None,
                      'neighbourhood_external': None, 'neighbourhood_id_external': None,
                      'place_external': None, 'place_id_external': None,
                      'postal_code': None, 'postal_code_id_external': None,
                      'address_external': None, 'address_id_external': None,
                      'poi_external': None, 'poi_id_external': None
                      }
            try:
                for feat in json_response['features']:
                    case = feat['place_type'][0]
                    # if case == 'country':
                    #     result['country_external'] = feat['text']
                    #     result['country_id_external'] = feat['id']
                    # if case == 'region':
                    #     result['region_external'] = feat['text']
                    #     result['region_id_external'] = feat['id']
                    # if case == 'subregion':
                    #     result['subregion_external'] = feat['text']
                    #     result['subregion_id_external'] = feat['id']
                    if case == 'county':
                        result['county_external'] = feat['text']
                        result['county_id_external'] = feat['id']
                    if case == 'joint_municipality':
                        result['joint_municipality_external'] = feat['text']
                        result['joint_municipality_id_external'] = feat['id']
                    if case == 'joint_submunicipality':
                        result['joint_submunicipality_external'] = feat['text']
                        result['joint_submunicipality_id_external'] = feat['id']
                    if case == 'municipality':
                        result['municipality_external'] = feat['text']
                        result['municipality_id_external'] = feat['id']
                    if case == 'municipal_district':
                        result['municipal_district_external'] = feat['text']
                        result['municipal_district_id_external'] = feat['id']
                    if case == 'locality':
                        result['locality_external'] = feat['text']
                        result['locality_id_external'] = feat['id']
                    if case == 'neighbourhood':
                        result['neighbourhood_external'] = feat['text']
                        result['neighbourhood_id_external'] = feat['id']
                    if case == 'place':
                        result['place_external'] = feat['text']
                        result['place_id_external'] = feat['id']
                    # if case == 'postal_code':
                    #     result['postal_code_external'] = feat['text']
                    #     result['postal_code_id_external'] = feat['id']
                    # if case == 'address':
                    #     result['address_external'] = feat['text']
                    #     result['address_id_external'] = feat['id']
                    # if case == 'poi':
                    #     result['poi_external'] = feat['text']
                    #     result['poi_id_external'] = feat['id']

                data.append(result)

            except Exception as e:
                counter += 1
    print("Number of failed requests: {}".format(counter))
    data = pd.DataFrame(data)
    # data[['region_external', 'region_id_external']] = data[['region_external', 'region_id_external']].combine_first(
    #     data[['subregion_external', 'subregion_id_external']].rename({'subregion_external': 'region_external','subregion_id_external': 'region_id_external'}, axis='columns'))
    #
    # data[['region_external', 'region_id_external', 'municipality_external', 'municipality_id_external']] = \
    #     data[['region_external', 'region_id_external', 'municipality_external',
    #           'municipality_id_external']].combine_first(
    #         data[['subregion_external', 'subregion_id_external', 'joint_municipality_external',
    #               'joint_municipality_id_external']].rename({'subregion_external': 'region_external','subregion_id_external': 'region_id_external','joint_municipality_external': 'municipality_external','joint_municipality_id_external': 'municipality_id_external'}, axis='columns'))

    #  data[['county_external', 'county_id_external', 'county_external', 'county_id_external']]
    # )
    return pd.DataFrame(data)


if __name__ == "__main__":
    # clean_up_pv_data_locations()
    # migrate_hotel_locations()
    # create_location_mapping()
    # clean_up_pv_data_hotels()

    # compare_new_pv_data()
    #  mapping=pd.read_csv("location_mapping.csv",delimiter=";")
    #  print(mapping.info())
    #  print("stop")
    # region_mapping_via_name()
    # region_mapping_via_coordinates()
    # evaluate_current_city_mapping()
    location_mapping_v3()
    # location_distance_mapping()
    # mapping_filtered.to_csv("location_mapping_with_hotelpv.csv")
    # no_mapping_filtered.to_csv("location_mapping_rest.csv")
    # unique = pd.read_csv("sorted_unique_mappings.csv", decimal=",")
    # mapping_filtered = pd.read_csv("location_mapping_with_hotelpv.csv")
    # no_mapping_filtered = pd.read_csv("location_mapping_rest.csv")
    # all = pd.concat([unique, mapping_filtered, no_mapping_filtered])

    distance_curated = pd.read_csv("city_distance_mapping_curated.csv", delimiter=";", decimal=",")
    distance_checked = distance_curated[distance_curated['manual check'] == "x"]

    all_curated = pd.read_csv("all_cities_w_mapping.csv", delimiter=";", decimal=",")
    all_curated2 = all_curated[(~all_curated.pv_location_id.isin(distance_checked.pv_location_id.unique())) & (
        ~all_curated.hotel_location_id.isin(distance_checked.hotel_location_id.unique()))]
    # all_curated2 = pd.merge(all_curated, distance_curated, on=["pv_location_id", "hotel_location_id"], how="inner")
    all = pd.concat([all_curated2, distance_checked.rename(columns={"manual check": "checked manually"})])

    print("stop")  # TODO add location aliases from pv data
