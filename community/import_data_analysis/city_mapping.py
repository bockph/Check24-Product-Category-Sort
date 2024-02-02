import pandas as pd
import data_loader as dl
import difflib

if __name__=="__main__":
    # get already curated mappings
    already_curated_1 = pd.read_csv("../cities_curated1711.csv")[['pv_city_id','hotel_city_id']].drop_duplicates(keep='first')
    already_curated_2 = pd.read_csv("../cities_curated2011.csv", delimiter=";")
    already_curated_2=already_curated_2[already_curated_2.manual_check=="x"][['pv_city_id','hotel_city_id']].drop_duplicates(keep='first')
    already_curated_3 = pd.read_csv("../cities_curated2111.csv", delimiter=";")[['pv_city_id','hotel_city_id']].drop_duplicates(keep='first')
    already_curated_4 = pd.read_csv("../cities_curated2211.csv", delimiter=",")
    already_curated_4=already_curated_4[already_curated_4.manual_check=="x"][['pv_city_id','hotel_city_id']].drop_duplicates(keep='first')
    already_curated = pd.concat(
        [already_curated_3, already_curated_2, already_curated_1]).reset_index(drop=True).drop_duplicates(keep='first')

    check_duplicates = already_curated[~(already_curated.duplicated("pv_city_id", keep=False) |
                                        already_curated_2.duplicated("hotel_city_id", keep=False))]

    check_duplicates.to_csv("city_mapping.csv", index=False)



    print("stop")