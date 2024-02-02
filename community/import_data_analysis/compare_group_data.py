import data_loader as dl


def compare_group_data():
    group_old, group_new = dl.import_comparison_group_data(old_name="group_prod_before1312", new_name="group_prod_40k",
                                                           from_db=False)
    old_groups_in_new = group_old[group_old["id"].isin(group_new["id"])].sort_values("id").reset_index(drop=True)
    new_groups_in_old = group_new[group_new["id"].isin(group_old["id"])].sort_values("id").reset_index(drop=True)
    old_groups_in_new = old_groups_in_new.drop(
        columns=[ "updated_at","category","category_type","linkout_url",])
                #  "geo_lat","geo_long"])
    new_groups_in_old = new_groups_in_old.drop(
            columns=[
                      "updated_at","category","category_type","linkout_url",])
                                                      # "geo_lat", "geo_long"])
    # old_groups_in_new = old_groups_in_new.drop(
    #     columns=["aggregate_member_count", "counts_updated_at",
    #              "aggregate_at_location_count", "post_count", "member_count", "view_count", "at_location_count",
    #              "updated_at", "category", "category_type",
    #              "geo_lat", "geo_long"])
    # new_groups_in_old = new_groups_in_old.drop(
    #     columns=["aggregate_member_count", "counts_updated_at",
    #              "aggregate_at_location_count", "post_count", "member_count",
    #              "view_count", "at_location_count", "updated_at", "category", "category_type",
    #              "geo_lat", "geo_long"])

    #diff = old_groups_in_new.compare(new_groups_in_old)
    diff = old_groups_in_new.compare(new_groups_in_old)#, keep_shape=True, keep_equal=True)
    diff2 = old_groups_in_new.compare(new_groups_in_old, keep_shape=True, keep_equal=True)
    diff2.columns = diff.columns.map(''.join)
    # ids_ok =diff[((diff["geo_latself"]-diff["geo_latother"]).abs
    #             >0.05) &(abs(diff["geo_longself"]-diff["geo_longother"])>0.05)]
    # diff[(abs(diff["geo_latself"]) >= 0) & (abs(diff["geo_latself"] - diff["geo_latother"]) > 0.5) & (

    print("stop")

if __name__ == '__main__':
    compare_group_data()

