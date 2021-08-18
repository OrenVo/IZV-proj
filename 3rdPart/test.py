import geo
import pandas as pd

gdf = geo.make_geo(pd.read_pickle("accidents.pkl.gz"))
geo.plot_geo(gdf, "geo1.png", False)
geo.plot_cluster(gdf, "geo2.png", False)