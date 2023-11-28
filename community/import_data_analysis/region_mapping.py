import pandas as pd
import data_loader as dl
import difflib

if __name__ == "__main__":
    hotel_countries, hotel_regions, hotel_cities, _ = dl.import_hotel_data(from_db=False)
    pv_countries, pv_regions, pv_cities, _ = dl.import_pv_data(from_db=False)
    already_mapped = pd.read_csv("region_mapping_curated.csv", delimiter=";")
    already_mapped=already_mapped[~(
                already_mapped.duplicated("pv_region_id", keep=False) | already_mapped.duplicated("hotel_region_id",
                                                                                                  keep=False))]

    region_mappings = (pv_regions[~pv_regions.pv_region_id.isin(already_mapped.pv_region_id)]
                       .merge(hotel_regions[~hotel_regions.hotel_region_id.isin(already_mapped.hotel_region_id)], how="inner", left_on="pv_country_name",
                                       right_on="hotel_country_name"))

    region_mappings['textual_similarity'] = (
        region_mappings.progress_apply(lambda x: 0 if str(x['pv_region_name']) == "nan"
        else difflib.SequenceMatcher(None, str(x['pv_region_name']).lower(),
                                     str(x['hotel_region_name']).lower()).ratio() * 100, axis=1).fillna(-1).astype(int))

    region_mappings['manhattan_distance'] = \
        ((region_mappings['pv_region_latitude'].abs() - region_mappings['hotel_region_latitude'].abs()).abs()
         + (region_mappings['pv_region_longitude'].abs() - region_mappings['hotel_region_longitude'].abs()).abs())

    region_mappings_for_curation = region_mappings[['pv_region_id','hotel_region_id', 'pv_region_name',
                                                    'hotel_region_name','pv_country_name','hotel_country_name',
                                                    'textual_similarity', 'manhattan_distance']]
    # already_curated2 = already_mapped[['pv_region_id','hotel_region_id']]
    # already_curated2=already_curated2.merge(pv_regions[['pv_region_id','pv_region_name','pv_region_latitude','pv_region_longitude','pv_country_name']],how="inner",on="pv_region_id")
    # already_curated2=already_curated2.merge(hotel_regions[['hotel_region_id','hotel_region_name','hotel_region_latitude','hotel_region_longitude','hotel_country_name']],how="inner",on="hotel_region_id")
    # already_curated2['textual_similarity'] = (
    #     already_curated2.progress_apply(lambda x: 0 if str(x['pv_region_name']) == "nan"
    #     else difflib.SequenceMatcher(None, str(x['pv_region_name']).lower(),
    #                                  str(x['hotel_region_name']).lower()).ratio() * 100, axis=1).fillna(-1).astype(int))
    #
    # already_curated2['manhattan_distance'] = \
    #     ((already_curated2['pv_region_latitude'].abs() - already_curated2['hotel_region_latitude'].abs()).abs()
    #      + (already_curated2['pv_region_longitude'].abs() - already_curated2['hotel_region_longitude'].abs()).abs())
    #
    # already_curated2.drop(columns=['pv_region_latitude','pv_region_longitude','hotel_region_latitude','hotel_region_longitude','pv_country_name','hotel_country_name'],inplace=True)
    # already_curated2.to_csv("region_mapping_curated.csv",sep=";",index=False)


    # TODO common hotels

    # TODO calculate geocoordinates for cities without coordinates

    print("stop")
