

import pandas as pd
import json
import requests
def parse_maptiler_answer(json_response):
    result=None
    # country
    # region
    # subregion
    # county
    # joint_municipality
    # joint_submunicipality
    # municipality
    # municipal_district
    # locality
    # neighbourhood
    # place
    # postal_code
    # address
    # poi
    result ={
        'country':'','country_id':'','region':'','region_id':'','subregion':'','subregion_id':'','county':'','county_id':'',
        'joint_municipality':'','joint_municipality_id':'','joint_submunicipality':'','joint_submunicipality_id':'',
        'municipality':'','municipality_id':'','municipal_district':'','municipal_district_id':'','locality':'','locality_id':'',
        'neighbourhood':'','neighbourhood_id':'','place':'','place_id':'','postal_code':'','postal_code_id':'','address':'','address_id':'',
        'poi':'','poi_id':''
    }

    for feat in json_response['features']:
        case=feat['place_type'][0]
        if case=='country':
            result['country']=feat['text']
            result['country_id']=feat['id']
        if case=='region':
            result['region']=feat['text']
            result['region_id']=feat['id']
        if case=='subregion':
            result['subregion']=feat['text']
            result['subregion_id']=feat['id']
        if case=='county':
            result['county']=feat['text']
            result['county_id']=feat['id']
        if case=='joint_municipality':
            result['joint_municipality']=feat['text']
            result['joint_municipality_id']=feat['id']
        if case=='joint_submunicipality':
            result['joint_submunicipality']=feat['text']
            result['joint_submunicipality_id']=feat['id']
        if case=='municipality':
            result['municipality']=feat['text']
            result['municipality_id']=feat['id']
        if case=='municipal_district':
            result['municipal_district']=feat['text']
            result['municipal_district_id']=feat['id']
        if case=='locality':
            result['locality']=feat['text']
            result['locality_id']=feat['id']
        if case=='neighbourhood':
            result['neighbourhood']=feat['text']
            result['neighbourhood_id']=feat['id']
        if case=='place':
            result['place']=feat['text']
            result['place_id']=feat['id']
        if case=='postal_code':
            result['postal_code']=feat['text']
            result['postal_code_id']=feat['id']
        if case=='address':
            result['address']=feat['text']
            result['address_id']=feat['id']
        if case=='poi':
            result['poi']=feat['text']
            result['poi_id']=feat['id']


    #     if feat['place_type'][0]=='city': region=feat['text'] else region =""
    #         result=feat['text']
    #         break
    # country= json_response['features'][5]['text']
    # region= json_response['features'][4]['text']
    # city= json_response['features'][3]['text']

    return (city,region,country)
def send_request(longitude,latitude):
    result=None
    # url = "https://api.maptiler.com/geocoding/reverse?lat={}&lon={}&key=9Z4Z4ZQZ4Z4Z4Z4Z4Z4Z".format(latitude,longitude)
    url="https://api.maptiler.com/geocoding/{},{}.json?key=Hlzt6D2yqvnwlixiZ60x".format(longitude,latitude)
    response = requests.get(url).json()
    with open('response.json', 'w') as outfile:
        json.dump(response, outfile)

    try:
        response_parsed = parse_maptiler_answer(response)
    except Exception as e:
        print("bad answer from maptiler")
        # response_parsed = parse_maptiler_answer(response)
    return response_parsed


if __name__=="__main__":

import pandas as pd
import json
import requests
def parse_maptiler_answer(json_response):
    result=None
    # country
    # region
    # subregion
    # county
    # joint_municipality
    # joint_submunicipality
    # municipality
    # municipal_district
    # locality
    # neighbourhood
    # place
    # postal_code
    # address
    # poi
    result ={
        'country':'','country_id':'','region':'','region_id':'','subregion':'','subregion_id':'','county':'','county_id':'',
        'joint_municipality':'','joint_municipality_id':'','joint_submunicipality':'','joint_submunicipality_id':'',
        'municipality':'','municipality_id':'','municipal_district':'','municipal_district_id':'','locality':'','locality_id':'',
        'neighbourhood':'','neighbourhood_id':'','place':'','place_id':'','postal_code':'','postal_code_id':'','address':'','address_id':'',
        'poi':'','poi_id':''
    }

    for feat in json_response['features']:
        case=feat['place_type'][0]
        if case=='country':
            result['country']=feat['text']
            result['country_id']=feat['id']
        if case=='region':
            result['region']=feat['text']
            result['region_id']=feat['id']
        if case=='subregion':
            result['subregion']=feat['text']
            result['subregion_id']=feat['id']
        if case=='county':
            result['county']=feat['text']
            result['county_id']=feat['id']
        if case=='joint_municipality':
            result['joint_municipality']=feat['text']
            result['joint_municipality_id']=feat['id']
        if case=='joint_submunicipality':
            result['joint_submunicipality']=feat['text']
            result['joint_submunicipality_id']=feat['id']
        if case=='municipality':
            result['municipality']=feat['text']
            result['municipality_id']=feat['id']
        if case=='municipal_district':
            result['municipal_district']=feat['text']
            result['municipal_district_id']=feat['id']
        if case=='locality':
            result['locality']=feat['text']
            result['locality_id']=feat['id']
        if case=='neighbourhood':
            result['neighbourhood']=feat['text']
            result['neighbourhood_id']=feat['id']
        if case=='place':
            result['place']=feat['text']
            result['place_id']=feat['id']
        if case=='postal_code':
            result['postal_code']=feat['text']
            result['postal_code_id']=feat['id']
        if case=='address':
            result['address']=feat['text']
            result['address_id']=feat['id']
        if case=='poi':
            result['poi']=feat['text']
            result['poi_id']=feat['id']


    #     if feat['place_type'][0]=='city': region=feat['text'] else region =""
    #         result=feat['text']
    #         break
    # country= json_response['features'][5]['text']
    # region= json_response['features'][4]['text']
    # city= json_response['features'][3]['text']

    return (city,region,country)
def send_request(longitude,latitude):
    result=None
    # url = "https://api.maptiler.com/geocoding/reverse?lat={}&lon={}&key=9Z4Z4ZQZ4Z4Z4Z4Z4Z4Z".format(latitude,longitude)
    url="https://api.maptiler.com/geocoding/{},{}.json?key=Hlzt6D2yqvnwlixiZ60x".format(longitude,latitude)
    response = requests.get(url).json()
    with open('response.json', 'w') as outfile:
        json.dump(response, outfile)

    try:
        response_parsed = parse_maptiler_answer(response)
    except Exception as e:
        print("bad answer from maptiler")
        # response_parsed = parse_maptiler_answer(response)
    return response_parsed


if __name__=="__main__":
    with open('pv_location_map_tiler/2332.json') as json_file:
        data = json.load(json_file)

    print(parse_maptiler_answer(data))
    # print(send_request(14.207669,40.973174))


    with open('pv_location_map_tiler/2332.json') as json_file:
        data = json.load(json_file)

    print(parse_maptiler_answer(data))
    # print(send_request(14.207669,40.973174))

