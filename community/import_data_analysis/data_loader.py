
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
from numpy import nanmedian
from numpy import nanmean

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


def create_connection(database_name):
    mysql_url = sa.engine.URL.create("mysql+pymysql", username="root", password="2553Freemander10!", host="localhost",
                                     port="3306", database=database_name)
    sqlEngine = create_engine(mysql_url, pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    return dbConnection

def import_group_data(from_db=False, database_name="pv_data_import"):
    if from_db:
        print("Start pulling PV DATA from DB")
        dbConnection = create_connection(database_name=database_name)

        group = pd.read_sql(sa.text("select * from `group`"), con=dbConnection)

        group.to_pickle("db_group.p")



        dbConnection.close()
    else:
        group = pd.read_pickle("db_group.p")
        group['id'] = group['id'].astype(int)
    return group

def import_comparison_group_data(from_db=False, database_name="c24_test_community"):
    if from_db:
        print("Start pulling Group Comparison DATA from DB")
        dbConnection = create_connection(database_name=database_name)

        # group_old = pd.read_sql(sa.text("select * from `group`"), con=dbConnection)
        # group_old.to_pickle("db_group_old_comparison.p")

        group_new = pd.read_sql(sa.text("select * from `group_new2`"), con=dbConnection)
        group_new.to_pickle("db_group_new2_comparison.p")
        group_old = pd.read_pickle("db_group_new_comparison.p")



        dbConnection.close()
    else:
        group_old = pd.read_pickle("db_group_old_comparison.p")
        group_new = pd.read_pickle("db_group_new_comparison.p")

    return group_old, group_new



def import_pv_data(from_db=False, database_name="pv_data_import"):
    if from_db:
        print("Start pulling PV DATA from DB")
        dbConnection = create_connection(database_name=database_name)
        pv_locations = pd.read_sql(
            sa.text("select distinct country_id as pv_country_id, country_name as pv_country_name,   country_updated,"
                    "region_id as pv_region_id,region_name as pv_region_name,region_updated,"
                    "city_id as pv_city_id,city_name as pv_city_name,city_latitude as pv_city_latitude,city_longitude as pv_city_longitude,city_updated"
                    " from `import_pv_data`"), con=dbConnection)
        pv_locations.to_pickle("db_pv_locations.p")

        pv_hotels = pd.read_sql(
            sa.text(
                "select hotel_id as id,hotel_name as name,latitude,longitude,accommodation_type,city_id,region_id,country_id"
                " from `import_pv_data`"), con=dbConnection)

        pv_hotels.to_pickle("db_pv_hotel.p")


        dbConnection.close()
    else:
        pv_locations = pd.read_pickle("db_pv_locations.p")
        pv_hotels = pd.read_pickle("db_pv_hotel.p")

    #cleanup locations
    pv_locations[['pv_country_id', 'pv_region_id', 'pv_city_id']] = pv_locations[
        ['pv_country_id', 'pv_region_id', 'pv_city_id']].fillna(-1).astype(float).astype(int)
    pv_locations[['pv_city_latitude', 'pv_city_longitude']] = pv_locations[
        ['pv_city_latitude', 'pv_city_longitude']].fillna(0).astype(float)

    pv_countries = pv_locations[['pv_country_id', 'pv_country_name']].drop_duplicates()

    pv_regions= pv_locations[['pv_region_id', 'pv_region_name','pv_country_id','pv_country_name']].drop_duplicates()
    pv_cities=pv_locations[['pv_city_id', 'pv_city_name', 'pv_city_latitude', 'pv_city_longitude', 'pv_region_name',
                            'pv_country_name','pv_country_id', 'pv_region_id']].drop_duplicates()

    #cleanup Hotels
    pv_hotels[['id', 'city_id']] = pv_hotels[['id', 'city_id']].astype(int)

    #add region to hotels
    pv_hotels = pv_hotels.merge(pv_cities[['pv_city_id', 'pv_region_name', 'pv_country_name','pv_region_id','pv_country_id']],
                                left_on="city_id", right_on="pv_city_id", how="left")

    try:
        pv_region_center=pd.read_csv("region_coordinates.csv",delimiter=";")
    except:
        ##calculate pv region coordinates
        # calculate center for regions by city
        pv_region_center_cities = pv_cities[(pv_cities.pv_city_latitude!=0)&(pv_cities.pv_city_longitude!=0)].groupby(['pv_region_id']).agg(
            {'pv_city_latitude': nanmedian, 'pv_city_longitude': nanmedian}).reset_index()
        pv_region_center_cities.fillna(0, inplace=True)
        # calculate center for regions by hotel

        pv_region_center_hotels = pv_hotels[(pv_hotels.latitude!=0)&(pv_hotels.longitude!=0)].groupby(['pv_region_id']).agg(
            {'latitude': nanmedian, 'longitude': nanmedian}).reset_index()
        pv_region_center_hotels.fillna(0, inplace=True)

        pv_region_center = pv_region_center_cities.merge(pv_region_center_hotels, on="pv_region_id", how="outer")
        pv_region_center['pv_region_latitude'] = pv_region_center[['pv_city_latitude', 'latitude']].mean(axis=1)
        pv_region_center['pv_region_longitude'] = pv_region_center[['pv_city_longitude', 'longitude']].mean(axis=1)
        pv_region_center = pv_region_center[['pv_region_id', 'pv_region_latitude', 'pv_region_longitude']]
        pv_region_center=pv_region_center[(pv_region_center.pv_region_latitude!=0) & (pv_region_center.pv_region_longitude!=0)]
        pv_region_center.drop_duplicates(subset="pv_region_id",inplace=True)
        pv_region_center.to_csv("region_coordinates.csv", sep=";", index=False)

    pv_regions=pv_regions.merge(pv_region_center, on="pv_region_id", how="left")
    pv_regions[['pv_region_latitude','pv_region_longitude']]=pv_regions[['pv_region_latitude','pv_region_longitude']].astype(float)


    #TODO calculate geocoordinates for countries and regions
    return pv_countries,pv_regions,pv_cities, pv_hotels

def import_hotel_data(from_db=True, database_name="pv_data_import"):
    if from_db:
        print("Start pulling Hotel DATA from DB")

        dbConnection = create_connection(database_name)

        hotel_location = pd.read_sql(sa.text("select "
                                             "id, pauschalreise_location_type, pauschalreise_location_id, source_created_at,"
                                             " source_deleted_at,source_updated_at,hotels_count,homes_count, translation_de_name,translation_en_name, type,"
                                             "use_in_autocompleter, ST_AsText(point) as point, preposition,"
                                             "aliases from hotel_data_locations"), con=dbConnection)
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

    # Remove invalid locations
    hotel_location = hotel_location[(hotel_location.translation_de_name != "NOT_ASSIGNED") & (
            hotel_location.translation_en_name != "NOT_ASSIGNED")]
    hotel_location = hotel_location[(hotel_location.translation_de_name != "NULL_COUNTRY") & (
            hotel_location.translation_en_name != "NULL_COUNTRY")]
    hotel_location = hotel_location[(hotel_location.translation_de_name != "NULL_CITY") & (
            hotel_location.translation_en_name != "NULL_CITY")]
    hotel_location = hotel_location[(hotel_location.translation_de_name != "NULL_REGION") & (
            hotel_location.translation_en_name != "NULL_REGION")]

    # create geocoordinates from point
    hotel_location.loc[~hotel_location.point.str.contains("POINT", na=False, regex=False), 'point'] = "POINT(0 0)"

    hotel_location[['longitude', 'latitude']] = (hotel_location['point'].str.replace("POINT\(", "")
                                                 .str.replace("\)", "").str.split(" ", expand=True))
    hotel_location.drop(columns=['point'], inplace=True)

    # add hotel has hotels field, for later filtering
    hotel_location['hotel_has_hotels'] = ''
    hotel_location.loc[((hotel_location.homes_count + hotel_location.hotels_count) > 0), 'hotel_has_hotels'] = 'x'

    # fill up empty hotel location names with english ones
    hotel_location.loc[hotel_location['translation_de_name'] == "", 'translation_de_name'] = hotel_location.loc[
        hotel_location['translation_de_name'] == "", 'translation_en_name']

    # get hotel hierarchy mapping
    hotel_location_hierarchy = pd.read_csv("hotel_city_region_country_mapping.csv", delimiter=";")


    hotel_location_hierarchy = hotel_location_hierarchy[['hotel_city_id','hotel_region_id',  "hotel_country_id"]]






    # get hotel COUNTRIES only, select important fields and rename
    hotel_countries = hotel_location[hotel_location.type == 1][
        ['id', 'translation_de_name', 'longitude', 'latitude', 'pauschalreise_location_id', 'hotel_has_hotels']]
    hotel_countries.rename(columns={"id": "hotel_country_id", "translation_de_name": "hotel_country_name",
                                 "longitude": "hotel_country_longitude", "latitude": "hotel_country_latitude",
                                 "pauschalreise_location_id": "origmapping_pv_country_id"},
                        inplace=True)
    #TODO create country mapping file
    # rename hotel country names and assert that it is not different from pv
    hotel_countries['hotel_country_name'] = hotel_countries['hotel_country_name'].replace(
        hotel_to_pv_country_mapping)

    # get hotel regions only, select important fiealds and rename
    hotel_regions = hotel_location[hotel_location.type == 2][
        ['id', 'translation_de_name', 'longitude', 'latitude', 'pauschalreise_location_id', 'hotel_has_hotels']]
    hotel_regions.rename(columns={"id": "hotel_region_id", "translation_de_name": "hotel_region_name",
                                  "longitude": "hotel_region_longitude", "latitude": "hotel_region_latitude",
                                  "pauschalreise_location_id": "origmapping_pv_region_id"},
                         inplace=True)
    hotel_regions[['hotel_region_latitude','hotel_region_longitude']]=hotel_regions[['hotel_region_latitude','hotel_region_longitude']].astype(float)

    hotel_regions['hotel_region_id'] = hotel_regions['hotel_region_id'].astype(int)

    # convert region names using pv-hote-region-mapping
    region_mapping = pd.read_csv("region_mapping.csv", delimiter=";",decimal=",")
    replace_dict = pd.Series(region_mapping.pv_region_name.values, index=region_mapping.hotel_region_name).to_dict()
    hotel_regions['hotel_region_name'] = hotel_regions['hotel_region_name'].replace(replace_dict)

    region_country_mapping = pd.read_csv("hotel_region_country_mapping.csv", delimiter=";")
    hotel_regions = (hotel_regions.merge(region_country_mapping[['hotel_region_id','hotel_country_id']].drop_duplicates(), on="hotel_region_id", how="left")
    .merge(hotel_countries[['hotel_country_id',"hotel_country_name"]], on="hotel_country_id", how="left"))
    # get hotel cities only, select important fields and rename
    hotel_cities = hotel_location[hotel_location.type == 3][
        ['id', 'translation_de_name', 'longitude', 'latitude', 'pauschalreise_location_id', 'hotel_has_hotels']]
    hotel_cities.rename(columns={"id": "hotel_city_id", "translation_de_name": "hotel_city_name",
                                 "longitude": "hotel_city_longitude", "latitude": "hotel_city_latitude",
                                 "pauschalreise_location_id": "origmapping_pv_city_id"},
                        inplace=True)
    hotel_cities = (hotel_cities.merge(
        hotel_location_hierarchy[['hotel_region_id', 'hotel_city_id','hotel_country_id']].drop_duplicates(), on="hotel_city_id",
        how="left").merge(hotel_regions[['hotel_region_id','hotel_region_name']], on="hotel_region_id", how="left")
                    .merge(hotel_countries[['hotel_country_id','hotel_country_name']], on="hotel_country_id", how="left"))
    hotel_cities['hotel_region_id'] = hotel_cities['hotel_region_id'].fillna(-1).astype(int)



    return hotel_countries,hotel_regions,hotel_cities, hotel

if __name__=="__main__":
    # hotel_countries,hotel_regions,hotel_cities, hotel = import_hotel_data(from_db=True)
    # pv_countries,pv_regions,pv_cities, pv_hotels = import_pv_data(from_db=True)
    print("done")