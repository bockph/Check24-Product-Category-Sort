import pandas as pd
import data_loader as dl
import difflib
import os
from tqdm import tqdm


def mapping_stage_one():
    # this stage gets the original mapping, filters for city match and then checks for textual similarity and distance

    pv_countries, pv_regions, pv_cities, pv_hotels = dl.import_pv_data(from_db=False)
    hotel_countries, hotel_regions, hotel_cities, hotel_hotels = dl.import_hotel_data(from_db=False)

    country_mapping = pd.read_csv("country_mapping.csv")
    region_mapping = pd.read_csv("region_mapping.csv")
    city_mapping = pd.read_csv("city_mapping.csv")[["pv_city_id", "hotel_city_id"]]

    city_mapping = city_mapping.merge(pv_cities[["pv_city_name", "pv_city_id"]], how="left", on="pv_city_id")
    city_mapping = city_mapping.merge(hotel_cities[["hotel_city_name", "hotel_city_id"]], how="left",
                                      on="hotel_city_id")
    orig_mapping = pv_hotels.merge(hotel_hotels, how="inner", left_on="pv_hotel_id", right_on="origmapping_pv_hotel_id")

    orig_mapping = orig_mapping.merge(country_mapping, how="left", left_on="pv_country_id", right_on="pv_country_id")
    orig_mapping = orig_mapping.merge(region_mapping, how="left", left_on="pv_region_id", right_on="pv_region_id")
    orig_mapping = orig_mapping.merge(city_mapping, how="left", left_on="pv_city_id", right_on="pv_city_id")

    orig_mapping['textual_similarity']=(orig_mapping.progress_apply(lambda x: 0 if str(x['pv_hotel_name']) == "nan"
        else difflib.SequenceMatcher(None, str(x['pv_hotel_name']).lower(),
                                     str(x['hotel_hotel_name']).lower()).ratio() * 100, axis=1).fillna(0).astype(int))

    orig_mapping['manhattan_distance'] = ((orig_mapping['pv_hotel_latitude'].abs() - orig_mapping['hotel_hotel_latitude'].abs()).abs()
         + (orig_mapping['pv_hotel_longitude'].abs() - orig_mapping['hotel_hotel_longitude'].abs()).abs())


    filtered_mapping = orig_mapping[orig_mapping["hotel_city_id_x"] == orig_mapping["hotel_city_id_y"]]
    # filtered_mapping['textual_similarity'] = (
    #     filtered_mapping.progress_apply(lambda x: 0 if str(x['pv_hotel_name']) == "nan"
    #     else difflib.SequenceMatcher(None, str(x['pv_hotel_name']).lower(),
    #                                  str(x['hotel_hotel_name']).lower()).ratio() * 100, axis=1).fillna(0).astype(int))
    #
    # filtered_mapping['manhattan_distance'] = \
    #     ((filtered_mapping['pv_hotel_latitude'].abs() - filtered_mapping['hotel_hotel_latitude'].abs()).abs()
    #      + (filtered_mapping['pv_hotel_longitude'].abs() - filtered_mapping['hotel_hotel_longitude'].abs()).abs())
    extended_mapping = filtered[~(
            filtered.duplicated("pv_hotel_id", keep=False) | filtered.duplicated(
        "hotel_hotel_id", keep=False))]
    filtered_mapping_reduced = filtered_mapping[
        ['pv_hotel_id', 'hotel_hotel_id', 'pv_hotel_name', 'hotel_hotel_name', 'textual_similarity',
         'manhattan_distance']]

def evaluate_mapping():
    pv_countries, pv_regions, pv_cities, pv_hotels = dl.import_pv_data(from_db=False)
    hotel_countries, hotel_regions, hotel_cities, hotel_hotels = dl.import_hotel_data(from_db=False)

    current_hotel_mapping = pd.read_csv("hotel_mapping.csv")
    pv_hotels = pv_hotels[~pv_hotels.pv_hotel_id.isin(current_hotel_mapping.pv_hotel_id)]
    hotel_hotels = hotel_hotels[~hotel_hotels.hotel_hotel_id.isin(current_hotel_mapping.hotel_hotel_id)]
    extended_mapping = pd.read_csv("hotel_mapping_extended3.csv")
    extended_mapping = extended_mapping[~(
                extended_mapping.duplicated("pv_hotel_id", keep=False) | extended_mapping.duplicated(
            "hotel_hotel_id", keep=False))]
    pv_hotels = pv_hotels[~pv_hotels.pv_hotel_id.isin(extended_mapping.pv_hotel_id)]
    hotel_hotels = hotel_hotels[~hotel_hotels.hotel_hotel_id.isin(extended_mapping.hotel_hotel_id)]

    country_mapping = pd.read_csv("country_mapping.csv")
    region_mapping = pd.read_csv("region_mapping.csv")
    city_mapping = pd.read_csv("city_mapping.csv")[["pv_city_id", "hotel_city_id"]]

    city_mapping = city_mapping.merge(pv_cities[["pv_city_name", "pv_city_id"]], how="left", on="pv_city_id")
    city_mapping = city_mapping.merge(hotel_cities[["hotel_city_name", "hotel_city_id"]], how="left",
                                      on="hotel_city_id")
    pv_hotels = pv_hotels.merge(city_mapping, how="left", left_on="pv_city_id", right_on="pv_city_id")
    # cross join on cities
    for idx, row in tqdm(country_mapping.iterrows()):
        current_pv_hotels = pv_hotels.loc[pv_hotels.pv_country_id == row.pv_country_id]
        current_hotel_hotels = hotel_hotels.loc[hotel_hotels.hotel_country_id == row.hotel_country_id]

        mapping = current_pv_hotels.merge(current_hotel_hotels, how="inner", on="hotel_city_id")
        mapping['manhattan_distance'] = \
            ((mapping['pv_hotel_latitude'].abs() - mapping['hotel_hotel_latitude'].abs()).abs()
             + (mapping['pv_hotel_longitude'].abs() - mapping['hotel_hotel_longitude'].abs()).abs())
        mapping = mapping[(mapping.manhattan_distance < 0.005) &(mapping.manhattan_distance >= 0.000)]
        mapping['textual_similarity'] = (
            mapping.progress_apply(lambda x: 0 if str(x['pv_hotel_name']) == "nan"
            else difflib.SequenceMatcher(None, str(x['pv_hotel_name']).lower(),
                                         str(x['hotel_hotel_name']).lower()).ratio() * 100, axis=1).fillna(0).astype(
                int))
        mapping = mapping[mapping.textual_similarity == 90][
            ['pv_hotel_id', 'hotel_hotel_id', 'pv_country_name', 'hotel_country_name']]
        mapping = mapping[~(
                mapping.duplicated("pv_hotel_id", keep=False) | mapping.duplicated(
            "hotel_hotel_id", keep=False))]
        output_path = 'hotel_mapping_extended4.csv'
        mapping.to_csv(output_path, mode='a', header=not os.path.exists(output_path), index=False)

    # similarity fail should we remove the hotel from the mapping? ~44k hotels
    # distance check can be done as it is ~3k hotels
    # TODO remove curated mappings, cross merge on cities run all metrics
    print("stop")


if __name__ == "__main__":
    mapping_stage_one()