import pandas as pd
import data_loader as dl
import difflib
import os
from tqdm import tqdm
import json
from ast import literal_eval
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
def transform_polygon_city_json():
    # this stage gets the original mapping, filters for city match and then checks for textual similarity and distance

    #pv_countries, pv_regions, pv_cities, pv_hotels = dl.import_pv_data(from_db=False)
    #load cities.geojson
    json_file = os.path.join(os.path.dirname(__file__), 'cities.geojson')
    with open(json_file) as f:
        cities_json = json.load(f)

    #make df from geojson
    cities =[]
    cities_for_pd = []
    for idx,feature in enumerate(cities_json['features']):
        tmp_city = dict()
        tmp_city['polygon_id'] = idx
        tmp_city['polygon_name'] = feature['properties']['NAME']
        polygon_center = [sum(x) / len(x) for x in zip(*feature['geometry']['coordinates'][0])]
        tmp_city['polygon_center_latitude'] = polygon_center[1]
        tmp_city['polygon_center_longitude'] = polygon_center[0]
        cities_for_pd.append(tmp_city)
        tmp_city['polygon'] = feature['geometry']['coordinates'][0]
        #calculate center of polygon
        tmp_city['polygon_center'] = polygon_center
        cities.append(tmp_city)

    pd_polygon_cities = pd.DataFrame(cities_for_pd)
    pd_polygon_cities.to_csv("polygon_cities.csv", index=False)
    with open('polygon_data.json', 'w') as outfile:
        json.dump(cities, outfile)
    #make list of all possible combinations with first coordinates from geojson only

    print("stop")

def determine_if_in_polygon():
    # polygon_data = pd.read_csv("polygon_cities.csv")[['polygon_id','polygon']]
    possible_mappings =pd.read_csv("preliminary_mapping.csv")
    # possible_mappings=pd.merge(possible_mappings, polygon_data, how="right", on="polygon_id")



    polygon_mappings = []
    for idx, row in tqdm(possible_mappings.iterrows()):
        polygon = literal_eval(row['polygon'])
        point = Point(row['pv_city_longitude'], row['pv_city_latitude'])
        polygon = Polygon(polygon)
        contains = polygon.contains(point)
        row['contains']=contains
        polygon_mappings.append(row)
    polygon_mappings = pd.DataFrame(polygon_mappings)
    polygon_mappings.to_csv("polygon_mappings.csv", index=False)



def preliminary_mapping():
    polygon_cities=pd.read_csv("polygon_cities.csv")
    pv_countries, pv_regions, pv_cities, pv_hotels = dl.import_pv_data(from_db=False)
    last =0
    filtered_pds= []
    for idx in [2500,5000,7500,10000,12500,15000,17500,20000,22500,25000,27500,30000,32500,34472]:
        joined = pv_cities.merge(polygon_cities[last:idx], how="cross")

        # for idx, row in tqdm(joined.iterrows()):
        #     dist = ((row['pv_city_latitude'].abs() - row['polygon_center_latitude'].abs()).abs()
        #      + (row['pv_city_longitude'].abs() - row['polygon_center_longitude'].abs()).abs())

        joined['manhattan_distance'] = ((joined['pv_city_latitude'].abs() - joined['polygon_center_latitude'].abs()).abs()
             + (joined['pv_city_longitude'].abs() - joined['polygon_center_longitude'].abs()).abs())
        joined_filtered = joined[joined.manhattan_distance < 0.5]
        filtered_pds.append(joined_filtered)
        print(idx)
        print(len(joined_filtered))
        last=idx
    combined = pd.concat(filtered_pds)
    combined.to_csv("preliminary_mapping.csv", index=False)
    print("stop")


if __name__ == "__main__":
    # transform_polygon_city_json()
    preliminary_mapping()
    determine_if_in_polygon()