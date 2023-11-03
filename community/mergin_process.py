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
pd.set_option('display.max_columns', 20)
from tqdm import tqdm
from statistics import median, mean

from haversine import haversine, Unit

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

        # location = pd.read_sql(sa.text("select * from location"), con=dbConnection)
        # location[['id', 'object_id', 'parent_object_id', 'parent_id', 'geo_lat', 'geo_long', 'group_id']] = location[
        #     ['id', 'object_id', 'parent_object_id', 'parent_id', 'geo_lat', 'geo_long', 'group_id']].apply(
        #     pd.to_numeric, errors='coerce')
        #
        # location.to_pickle("db_location.p")
        location = pd.read_pickle("db_location.p")

        # group = pd.read_sql(
        #     sa.text("select * from `group` where group.type in (\"COUNTRY\",\"REGION\",\"CITY\",\"REGION_GROUP\")"),
        #     con=dbConnection)
        #
        # group['id'] = group['id'].astype(int)

        # group.to_pickle("db_group.p")
        group = pd.read_pickle("db_group.p")

        # pv_data = pd.read_sql(
        #     sa.text("select country_id, country_name, country_code, country_autocomplete, country_updated,"
        #             "region_id,region_name,region_airport_code,region_alternative_name,region_autocomplete,region_updated,"
        #             "city_id,city_name,city_airport_code,city_autocomplete,city_latitude,city_longitude,city_updated,active"
        #             " from `community_export_final`"), con=dbConnection)
        #
        # pv_data[['city_latitude', 'city_longitude', 'country_id', 'region_id', 'city_id']] = pv_data[
        #     ['city_latitude', 'city_longitude', 'country_id', 'region_id', 'city_id']].apply(pd.to_numeric, errors='coerce')
        #
        # pv_data.to_pickle("db_pv_data.p")
        pv_data = pd.read_pickle("db_pv_data.p")

        hotel = pd.read_sql(
            sa.text("select hotel_id as id,hotel_name as name,latitude,longitude,accommodation_type,city_id"
                    " from `community_export_final`"), con=dbConnection)
        hotel[['id', 'city_id']] = hotel[['id', 'city_id']].apply(pd.to_numeric, errors='coerce')
        print(hotel.info())
        hotel.to_pickle("db_pv_hotel.p")
        hotel = pd.read_pickle("db_pv_hotel.p")

        # locations_merged = pd.read_sql(sa.text("select * from locations_merged"), con=dbConnection)
        #
        # locations_merged.to_pickle("db_locations_merged.p")
        locations_merged = pd.read_pickle("db_locations_merged.p")

        dbConnection.close()
    else:
        locations_merged = pd.read_pickle("db_locations_merged.p")
        group = pd.read_pickle("db_group.p")
        location = pd.read_pickle("db_location.p")
        pv_data = pd.read_pickle("db_pv_data.p")
        hotel = pd.read_pickle("db_pv_hotel.p")

    return locations_merged, group, location, pv_data, hotel

def import_pv_data_new(from_db=False):
    if from_db:
        # 1. clean data

        print("Start pulling PV DATA from DB")
        dbConnection = create_connection()

        group = pd.read_sql(sa.text("select * from `group` where type in ('CITY','REGION','COUNTRY')"), con=dbConnection)
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
    merged = pd.merge(group, location_data, left_on=["aggregate_post_count",'type'], right_on=["pv_data_id",'type'], how="left")


    ###
    # 1. Combine Regions
    ###
    hotel_regions = hotel_hotels[['region_name','country_name']].drop_duplicates(keep="first").rename(columns={"country_name":"hotel_country_name"})
    hotel_regions['hotel_combined'] = (hotel_regions['region_name'].astype(str)+hotel_regions['hotel_country_name'])

    pv_regions = location_data[location_data.type=="REGION"][['name','country_id']]
    pv_countries = location_data[location_data.type=="COUNTRY"][['name','id']].rename(columns={"name":"pv_country_name"})
    pv_regions = pd.merge(pv_regions,pv_countries, left_on="country_id", right_on="id", how="left")
    pv_regions['pv_combined'] =(pv_regions['name'].astype(str)+pv_regions['pv_country_name']).drop(columns=["id","country_id"])

    country_merged_regions=pd.merge(pv_regions, hotel_regions, how="inner", left_on="pv_country_name", right_on="hotel_country_name")
    country_merged_regions['similarity'] = country_merged_regions.apply(lambda x: difflib.SequenceMatcher(None, str(x['name']).lower(), str(x['region_name']).lower()).ratio(), axis=1)

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

        hotel_location.loc[~hotel_location.point.str.contains("POINT",na=False,regex=False), 'point'] = "POINT(0 0)"

        hotel_location[['longitude', 'latitude']] = hotel_location['point'].str.replace("POINT\(","").str.replace( "\)","").str.split(" ",
                                                                                                           expand=True)
        hotel_location[
            ['id', 'pauschalreise_location_id', 'type', 'longitude', 'latitude', 'hotels_count', 'homes_count']] = \
        hotel_location[
            ['id', 'pauschalreise_location_id', 'type', 'longitude', 'latitude', 'hotels_count', 'homes_count']].apply(
            pd.to_numeric, errors='coerce')
        hotel_location.to_pickle("db_hotel_location.p")

        hotel = pd.read_sql(
            sa.text("select id,destination_name as hotel_name, pauschalreise_hotel_id,city_id,city as city_name,country as country_name,region as region_name"
                    " from hotel_data_hotels"), con=dbConnection)
        hotel[['id', 'pauschalreise_hotel_id', 'city_id']] = hotel[['id', 'pauschalreise_hotel_id', 'city_id']].apply(
            pd.to_numeric, errors='coerce')

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
        ['country_id', 'region_id','region_name', 'city_id', 'city_name', 'city_airport_code', 'city_autocomplete', 'city_latitude',
         'city_longitude', 'city_updated']].drop_duplicates().reset_index(
        drop=True)

    # Pauschalreise delivers data with wrong assignment of city_id to region_id
    # Hence we do two steps.

    # 1. load correct regions
    region_id_counts = regions.region_id.value_counts()
    duplicates = regions[regions.region_id.isin(region_id_counts.index[region_id_counts.gt(1)])].sort_values(by=['region_id'])
    #load wrong regions

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
    locations_merged, group, location, pv_data,_ = import_pv_data(from_db=False)


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

    merged_on_unique_external = pd.merge(hotel_locations_cities[['unique_identifier', 'id','translation_de_name']].dropna(),
                                         pv_data_cities[['unique_identifier', 'pv_data_id','name']].dropna(),
                                         left_on="unique_identifier", right_on="unique_identifier", how="inner")
    merged_on_unique_external.to_pickle("pv_merged_hotel_maptiler.p")



def create_location_mapping():
    geo_mapping =pd.read_pickle("pv_merged_hotel_maptiler.p")
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, _, _, _, pv_hotels = import_pv_data(from_db=False)
    pv_locations =      pd.read_pickle("db_locations_merged_final.p")
    pv_countries = pv_locations[pv_locations.type=="COUNTRY"]
    pv_locations = pv_locations[pv_locations.type=="CITY"]
    hotel_locations = hotel_locations[hotel_locations.type==3]
    #TODO only Cities
    #get hotel: pauschalreise_city_id, name, country_name, country_id based on merge with id and city_id
    hotel_data = pd.merge(hotel_hotels[['country_name','city_name', 'pauschalreise_hotel_id', 'city_id','hotel_name']],hotel_locations[['id','translation_de_name','latitude','longitude','pauschalreise_location_id' ]],left_on=['city_id'],right_on=['id'],how="left")
    hotel_data.rename(columns={'translation_de_name':'hotel_city_name1','city_name':'hotel_city_name2','country':'hotel_country_name','id':'hotel_location_id'},inplace=True)
    hotel_data.drop(columns=['city_id'],inplace=True)

    #get pv: pv_data_id, name, pv_country_name, based on merge with object pv_data_id
    pv_data=pd.merge(pv_hotels[['id','city_id','name']],pv_locations[['pv_data_id','name','pv_country_id','latitude','longitude']],left_on=['city_id'],right_on=['pv_data_id'],how="left")
    pv_data.rename(columns={"pv_data_id":"pv_location_id","name_y":"pv_city_name","pv_country_id":"pv_country_id"},inplace=True)

    #merge on pauschalreise city_id and pvdata_id to get the mappings
    mappings = pd.merge(hotel_data,pv_data,left_on="pauschalreise_hotel_id",right_on="id",how="inner")
    # mappings['similarity'] = mappings.apply(
    #     lambda x: difflib.SequenceMatcher(None, str(x['hotel_name']).lower(),
    #                                       str(x['name_x']).lower()).ratio() * 100, axis=1)
    # mappings=mappings[mappings.similarity>60]
    #drop unique ones
    counts = mappings.groupby(['hotel_location_id','pauschalreise_location_id', 'pv_location_id','pv_city_name','hotel_city_name1','latitude_x','longitude_x','latitude_y','longitude_y']).size().reset_index(name='count').sort_values(by=['count'],ascending=False)
    counts['distance']=counts.apply(lambda x: haversine((x['latitude_x'],x['longitude_x']),(x['latitude_y'],x['longitude_y'])),axis=1)
    counts['hotel_mapping']=counts.pauschalreise_location_id.astype(int)==counts.pv_location_id.astype(int)

    counts_clean = counts.drop_duplicates(subset=['hotel_location_id'], keep=False)
    counts_clean = counts_clean.drop_duplicates(subset=['pv_location_id'], keep=False)
    counts_clean['similarity'] = counts_clean.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_city_name1']).lower(),
                                          str(x['pv_city_name']).lower()).ratio() * 100, axis=1)
    counts_clean['distance'] = counts_clean.apply(
        lambda x: haversine((x['latitude_x'], x['longitude_x']), (x['latitude_y'], x['longitude_y']),
                            unit=Unit.KILOMETERS), axis=1)

    counts_clean.drop(columns=['latitude_x','longitude_x','latitude_y','longitude_y'],inplace=True)
    counts_clean['weighted_count'] =counts_clean['count']

    counts_clean['weighted_distance'] =(counts_clean['distance']).pow(2)/9*10*(-1)#(counts_w_dupplicates['distance']/max_distance).pow(2)/9*(-1)#
    # counts_w_dupplicates['distance']=counts_w_dupplicates['weighted_distance']
    counts_clean['weighted_similarity'] =counts_clean['similarity']/10#*0#0*0.36
    counts_clean['rank'] = counts_clean['weighted_count']+counts_clean['weighted_distance']+counts_clean['weighted_similarity']
    counts_clean.sort_values(by=['rank'],ascending=False,inplace=True)

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
    sorted_unique_mappings=sorted[((sorted['count'] > 5).astype(int) + (sorted['similarity'] > 60).astype(int) + (                 sorted['distance'] < 10).astype(int) >= 2)|(sorted['similarity']==100)]
    sorted_unique_mappings.sort_values("similarity",ascending=False).to_csv("sorted_unique_mappings.csv")

    counts_w_dupplicates = counts[~counts.index.isin(counts_clean.index)]
    counts_w_dupplicates = pd.merge(counts_w_dupplicates,geo_mapping[['id','pv_data_id','unique_identifier']],left_on=['hotel_location_id','pv_location_id'],right_on=['id','pv_data_id'],how="left").drop(columns=['pv_data_id'])
    counts_w_dupplicates['similarity']=counts_w_dupplicates.apply(lambda x: difflib.SequenceMatcher(None,str(x['hotel_city_name1']).lower(), str(x['pv_city_name']).lower()).ratio()*100, axis=1)
    counts_w_dupplicates['distance']=counts_w_dupplicates.apply(lambda x: haversine((x['latitude_x'],x['longitude_x']),(x['latitude_y'],x['longitude_y']),unit=Unit.KILOMETERS),axis=1)
    counts_w_dupplicates.drop(columns=['latitude_x','longitude_x','latitude_y','longitude_y'],inplace=True)
    max_count = median(counts_w_dupplicates['count'])
    max_distance = median(counts_w_dupplicates['distance'])


    #Problem:
    #nur gruppe hat ab ca 2-3k keine aussagekraft
    #nur entfernung schwierig, da teilweise falsche koordinaten, geo konstellationen in denen eine andere stadt näher ist
    #nur name insbesondere bei partial matches problematisch,häufig auch direkt nebeneinanderliegende stadtteile mit unterschiedlichem namen aber sonst kein match möglich


    #1km /10 Hotels /75% -->0.2;5;0.75-->
    #3km radius is as important as 10 Hotels or a 100% match
    ###below 3km impact is little, above exponentially stronger

    #if similarity is 100% and distance is below 10km -->it is a match (hypothesis, correct city is more important) or city must have a count of 30 or so
    #if similarity is 100% and distance is above 20km (big size city) -->distance is no criteria anymore (hypothesis hotel count is more important)
    #cutoff 95% percentile?

    #

    counts_w_dupplicates['weighted_count'] =counts_w_dupplicates['count']

    counts_w_dupplicates['weighted_distance'] =(counts_w_dupplicates['distance']).pow(2)/9*10*(-1)#(counts_w_dupplicates['distance']/max_distance).pow(2)/9*(-1)#
    # counts_w_dupplicates['distance']=counts_w_dupplicates['weighted_distance']
    counts_w_dupplicates['weighted_similarity'] =counts_w_dupplicates['similarity']/10#*0#0*0.36
    counts_w_dupplicates['rank'] = counts_w_dupplicates['weighted_count']+counts_w_dupplicates['weighted_distance']+counts_w_dupplicates['weighted_similarity']



    index_sim_smalldist=(counts_w_dupplicates.similarity==10)&(counts_w_dupplicates['distance']<(10))
    counts_w_dupplicates.loc[index_sim_smalldist,'rank'] =counts_w_dupplicates.loc[index_sim_smalldist,'weighted_count']*0.25+ \
                                                          counts_w_dupplicates.loc[index_sim_smalldist, 'weighted_distance'] + \
                                                          counts_w_dupplicates.loc[index_sim_smalldist,'weighted_similarity'] # counts_w_dupplicates['weighted_count']+counts_w_dupplicates['weighted_distance']+counts_w_dupplicates['weighted_similarity']


    index_sim_bigdist=(counts_w_dupplicates.similarity == 10) & (counts_w_dupplicates['distance'] > (10))
    counts_w_dupplicates.loc[index_sim_bigdist, 'rank'] = counts_w_dupplicates.loc[index_sim_bigdist,'weighted_count']+counts_w_dupplicates.loc[index_sim_bigdist,'weighted_similarity']
    # counts_w_dupplicates['rank']=counts_w_dupplicates.apply(lambda x: x['count']/max_count*0.2-x['distance']/max_distance*0.7+0.1*x['similarity'],axis=1)
    # sorted = counts_w_dupplicates.sort_values(by=['similarity'],ascending=False).sort_values(by=['count'],ascending=False).sort_values(by=['distance'],ascending=True).reset_index()#.sort_values(by=['unique_identifier'],ascending=False).reset_index(drop=False)


    counts_w_dupplicates.drop(columns=['weighted_count','weighted_distance','weighted_similarity','unique_identifier'],inplace=True)
    counts_w_dupplicates.rename(columns={'hotel_mapping':'hotel_based_pvmapping','id':'external_geo_match','pauschalreise_location_id':'hotel_pv_id_mapping','hotel_city_name1':'hotel_city_name','count':'common_hotels','similarity':'textual_similarity'},inplace=True)

    counts_w_dupplicates[['hotel_location_id','hotel_pv_id_mapping','pv_location_id']]=counts_w_dupplicates[['hotel_location_id','hotel_pv_id_mapping','pv_location_id']].astype(int)
    # counts_w_dupplicates['id1']=counts_w_dupplicates['id'].astype('boolean')
    counts_w_dupplicates['external_geo_match']=counts_w_dupplicates['external_geo_match'].fillna(0).astype('bool')

    sorted = counts_w_dupplicates.sort_values(by=['rank'], ascending=False)
    # correct = sorted[sorted.hotel_mapping == True]
    # correct_filtered = correct.drop_duplicates(subset=['pv_location_id'], keep='first').reset_index(drop=True)
    # correct_filtered = correct_filtered.drop_duplicates(subset=['hotel_location_id'], keep='first')
    # tbd = sorted[sorted.hotel_mapping == False]
    hotel_cities_extra = pd.read_csv("hotel_cities.csv",sep=";")
    hotel_countries_extra = pd.read_csv("hotel_countries.csv",sep=";")
    hotel_city_country = pd.merge(hotel_cities_extra[['id','country_id']],hotel_countries_extra[['id','name']],left_on="country_id",right_on="id",how="left")
    hotel_city_country.rename(columns={'id_x':'city_id','name':'hotel_country_name'},inplace=True)

    pv_city_country = pd.merge(pv_locations[['pv_data_id','pv_country_id']],pv_countries[['pv_data_id','name']],left_on="pv_country_id",right_on="pv_data_id",how="left")
    pv_city_country.rename(columns={'name':'pv_country_name'},inplace=True)
    sorted_tmp = pd.merge(sorted,pv_city_country[['pv_data_id_x','pv_country_name']],left_on="pv_location_id",right_on="pv_data_id_x",how="left")
    sorted_tmp = pd.merge(sorted_tmp,hotel_city_country[['city_id','hotel_country_name']],left_on="hotel_location_id",right_on="city_id",how="left")
    sorted= sorted_tmp.drop(columns=['pv_data_id_x','city_id'])
    sorted['country_similarity']=sorted.apply(lambda x: difflib.SequenceMatcher(None,str(x['hotel_country_name']).lower(), str(x['pv_country_name']).lower()).ratio()*100, axis=1)
    sorted = sorted[(sorted['common_hotels'] > 5).astype(int) + (sorted['textual_similarity'] > 60).astype(int)+(sorted['distance']<10).astype(int)>=2]

    sorted_filtered = sorted.drop_duplicates(subset=['hotel_location_id'], keep='first')

    sorted_filtered = sorted_filtered.drop_duplicates(subset=['pv_location_id'], keep='first').reset_index(drop=True)
    sorted_filtered = sorted_filtered.drop_duplicates(subset=['hotel_location_id'], keep='first')
    mapping_filtered=sorted_filtered[sorted_filtered.hotel_based_pvmapping==True]
    no_mapping_filtered = sorted_filtered[sorted_filtered.hotel_based_pvmapping == False]
    mapping_filtered.to_csv("location_mapping_with_hotelpv.csv")
    no_mapping_filtered.to_csv("location_mapping_rest.csv")

    not_mapped = pv_locations[(~pv_locations.pv_data_id.isin(counts_clean.pv_location_id)) & (~pv_locations.pv_data_id.isin(sorted_filtered.pv_location_id))]
    not_mapped.drop(columns=['airport_code','autocomplete','created_at','drop_value','group_id','hotel_data_id','id_x','id_y','image','nationality_code','source_updated_at','type','updated_at'],inplace=True)


    missing_mappings =pv_locations[(~pv_locations.pv_data_id.isin(counts_clean.pv_location_id)) & (~pv_locations.pv_data_id.isin(sorted_filtered.pv_location_id))]
    active_hotel_locations = hotel_locations[(hotel_locations.hotels_count > 0) | (hotel_locations.homes_count > 0)]
    active_hotel_locations = active_hotel_locations[['id', 'translation_de_name', 'latitude', 'longitude', 'pauschalreise_location_type', 'pauschalreise_location_id']]
    active_hotel_locations = active_hotel_locations[active_hotel_locations.pauschalreise_location_type == 'city']
    active_hotel_locations = active_hotel_locations[~active_hotel_locations.id.isin(sorted_filtered.hotel_location_id)]
    # round geocoordinates to 3 digits
    active_hotel_locations[['latitude','longitude']] = active_hotel_locations[['latitude','longitude']].round(1)
    missing_mappings[['latitude','longitude']] = missing_mappings[['latitude','longitude']].round(1)

    last_mapping = pd.merge(active_hotel_locations,missing_mappings,left_on=['latitude','longitude'],right_on=['latitude','longitude'],how="inner")

    last_mapping['textual_similarity'] = last_mapping.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['translation_de_name']).lower(),
                                          str(x['name']).lower()).ratio() * 100, axis=1)

    # make a key column to merge based on close matches
    # missing_mappings['Fuzzy_Key'] = missing_mappings.name.map(lambda x: difflib.get_close_matches(x, active_hotel_locations[:500].translation_de_name, n=1, cutoff=0.6))

    # since the values in our Fuzzy_Key column are lists, we have to convert them to strings
    # missing_mappings['Fuzzy_Key'] = missing_mappings.Fuzzy_Key.apply(lambda x: ''.join(map(str, x)))



    # correct_filtered2 = correct_filtered[(sorted_filtered['common_hotels'] > 5) | (sorted_filtered['textual_similarity'] > 60)]
    # correct_filtered3 = correct_filtered[(correct_filtered['common_hotels'] > 5).astype(int) + (correct_filtered['textual_similarity'] > 60).astype(int)+(correct_filtered['distance']<10).astype(int)>=2]
    #sorted_filtered[(~sorted_filtered.pv_location_id.isin(correct_filtered.pv_location_id))&(~sorted_filtered.hotel_location_id.isin(correct_filtered.hotel_location_id))]

    # counts_w_duplicates.to_csv("location_mapping_via_hotelconnection_ambigious.csv")
    # counts_clean.to_csv("location_mapping_via_hotelconnection_unique.csv")
    # hotel_locations[(hotel_locations.hotels_count>0) |(hotel_locations.homes_count>0)].to_csv("active_hotel_locations.csv")
    # hotel_locations.to_csv("active_hotel_locations_full.csv")
    # pv_locations.to_csv("pv_locations.csv")

    #counts_w_duplicates.sort_values(by=['similarity'],ascending=False).sort_values(by=['count'],ascending=False).sort_values(by=['distance'],ascending=True).sort_values(by=['unique_identifier'],ascending=False).reset_index(drop=False)
# #counts_w_duplicates[counts_w_duplicates.hotel_location_id==224503]
    print("stop")
    #Probleme
    # Touristenspot ist nicht der Hotelspot. Pauschalreise wählt ersteres für seine Hotels, Hotel die tatsächlichen
#sorted[sorted.pv_location_id.isin(hotel_locations['pauschalreise_location_id'].dropna().drop_duplicates(keep=False))][sorted.pauschalreise_location_id==27028]
    #Hotel ist detaillierter als pv z.B. 19552, 26070, (2402 (Fort.. hollywod)
    # analyze_file = pd.merge(pv_data_cities[['name', 'pv_data_id', 'municipality_id_external']], counts_w_dupplicates,
    #                         left_on=['pv_data_id'],
    #                         right_on=['parent_object_id'], how="right")



#correct_filtered[(correct_filtered['count']>5) | (correct_filtered.similarity>60)]

def clean_up_pv_data_hotels():
    hotel_locations, hotel_hotels = import_hotel_data(from_db=False)
    _, _, _, _, pv_hotels = import_pv_data(from_db=False)


    # merge on pauschalreise city_id and pvdata_id to get the mappings
    mappings = pd.merge(hotel_hotels, pv_hotels, left_on="pauschalreise_hotel_id", right_on="id", how="inner")

    mappings['similarity'] = mappings.apply(
        lambda x: difflib.SequenceMatcher(None, str(x['hotel_name']).lower(),
                                          str(x['name']).lower()).ratio() *100, axis=1)

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
    create_location_mapping()
    # clean_up_pv_data_hotels()

   # compare_new_pv_data()
    mapping=pd.read_csv("location_mapping.csv",delimiter=";")
    print(mapping.info())
    print("stop")
    # TODO add location aliases from pv data

