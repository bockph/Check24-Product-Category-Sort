import osmium
import shapely.wkb
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

region = "ukraine"


def merge_two_dicts(x, y):
    z = x.copy()  # start with keys and values of x
    z.update(y)  # modifies z with keys and values of y
    return z


class AdminAreaHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)

        self.areas = []
        self.wkbfab = osmium.geom.WKBFactory()

    def area(self, a):
        if "admin_level" in a.tags and ("name:en" in a.tags or "name:de" in a.tags):
            try:
                if a.tags["name:en"] is  None and a.tags["name:de"] is None:
                    return
                wkbshape = self.wkbfab.create_multipolygon(a)
                shapely_obj = shapely.wkb.loads(wkbshape, hex=True)

                area = {"id": a.id, "geo": shapely_obj}
                area = merge_two_dicts(area, a.tags)

                self.areas.append(area)
            except Exception as e:
                print(e)
                print(a.id, a.tags)



if __name__ == "__main__":

    handler = AdminAreaHandler()

    # path to file to local drive
    # download from https://download.geofabrik.de/index.html
    # osm_file = f"openstreetmap_data/ukraine-latest.osm.pbf"
    osm_file = f"openstreetmap_data/planet-240108.osm.pbf"

    # start data file processing
    handler.apply_file(osm_file, locations=True, idx='flex_mem')

    df = pd.DataFrame(handler.areas)
    gdf = gpd.GeoDataFrame(df, geometry="geo")
    print(gdf)
    in_ukraine = gdf.within(gdf[gdf.admin_level == "2"].geo.iloc[0])

    fig = plt.figure(figsize=(15, 15))
    ax = plt.axes()

    # country boundary
    gdf[(gdf.admin_level == "2")].set_crs(crs=4326).plot(ax=ax, alpha=1, edgecolor="#000", linewidth=2,
                                                         facecolor='none')

    # admin level 4 boundaries
    admin_level_4_gdf = gdf[(in_ukraine & (gdf.admin_level == "4") & (~gdf["ISO3166-2"].isna()))].set_crs(crs=4326)
    admin_level_4_gdf.plot(ax=ax, alpha=.1, facecolor='b', edgecolor="#000", linewidth=1)

    # add labels
    for idx, row in admin_level_4_gdf.iterrows():
        ax.annotate(text=row["name:en"], xy=(row.geo.centroid.x, row.geo.centroid.y),
                    horizontalalignment='center')  # , xy=row.geo.centroid)
