import requests
import json
import data_loader as dl
import pickle
from tqdm import tqdm
def get_json_response(query):
    url = 'https://nominatim.openstreetmap.org/search.php?q={}&email=philipp.bock+opensearch@check24.de&polygon_geojson=1&format=geocodejson&addressdetails=1'.format(query)
    r = requests.get(url)
    return json.loads(r.text)


if __name__ == '__main__':
    pv_countries,pv_regions,pv_cities, pv_hotels=dl.import_pv_data(from_db=False)

    countries =[]
    queue_completed=[]
    current_queue=[]
    for idx,row in tqdm(pv_countries.iterrows()):
        r = get_json_response(row['pv_country_name'])
        if type(r) is dict:
            r['query'] = row['pv_country_name']
            r['query_id'] = row['pv_country_id']
            r['query_type'] = 'country'
            r=[r]
        elif type(r) is list:
            for elem in r:
                elem['query']=row['pv_country_name']
                elem['query_id']=row['pv_country_id']
                elem['query_type']='country'
        countries=countries+r
    #store countries list to openstreetmap_countries.pkl
    with open('openstreetmap_countries.pkl', 'wb') as f:
        pickle.dump(countries, f)

    #load countries list from openstreetmap_countries.pkl
with open('openstreetmap_countries.pkl', 'rb') as f:
        countries = pickle.load(f)

    #show countries list
    print(countries)

    #display hallo welt
    print('hallo welt')

    #send request to google to get json response
    r = get_json_response('germany')

    #create data frame with city sizes for 60 largest German cities (by population)

    #load data from plygon_map_data.pkl


    #load hotel data using hotel data loader


    #load countries list from openstreetmap_countries.pkl
    with open('openstreetmap_countries.pkl', 'rb') as f:
        countries = pickle.load(f)

    r2 = get_json_response('paris')

    r3 = get_json_response('berlin')
    multiple = r2+r3

    print(r.json())