#!/usr/bin/python3.8
# coding=utf-8
import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import contextily as ctx
import sklearn.cluster
import numpy as np
# muzeze pridat vlastni knihovny
# %%
# Dictionary of column names and datatypes
column_types = {
    "p1": 'int64',
    "p36": 'int8',
    # "p37": 'int32',
    "p2a": 'datetime64',
    "weekday(p2a)": 'int8',
    "p2b": 'int16',
    "p6": 'int8',
    "p7": 'int8',
    "p8": 'int8',
    "p9": 'int8',
    "p10": 'int8',
    "p11": 'int8',
    "p12": 'int16',
    "p13a": 'int16',
    "p13b": 'int16',
    "p13c": 'int16',
    "p14": 'int64',
    "p15": 'int8',
    "p16": 'int8',
    "p17": 'int8',
    "p18": 'int8',
    "p19": 'int8',
    "p20": 'int8',
    "p21": 'int8',
    "p22": 'int8',
    "p23": 'int8',
    "p24": 'int8',
    "p27": 'int8',
    "p28": 'int8',
    "p34": 'int8',
    "p35": 'int8',
    "p39": 'int8',
    "p44": 'int8',
    "p45a": 'int8',
    "p47": 'int8',
    "p48a": 'int8',
    "p49": 'int8',
    "p50a": 'int8',
    "p50b": 'int8',
    "p51": 'int8',
    "p52": 'int8',
    "p53": 'int64',
    "p55a": 'int8',
    "p57": 'int8',
    "p58": 'int8',
    "a": 'str',
    "b": 'str',
    "d": 'float64',
    "e": 'float64',
    "f": 'float64',
    "g": 'float64',
    "h": 'str',
    "i": 'str',
    "j": 'str',
    "k": 'str',
    "l": 'str',
    "n": 'str',
    "o": 'str',
    "p": 'str',
    "q": 'str',
    "r": 'str',
    "s": 'str',
    "t": 'str',
    "p5a": 'int8'
}


def make_geo(df: pd.DataFrame) -> geopandas.GeoDataFrame:
    """ Konvertovani dataframe do geopandas.GeoDataFrame se spravnym kodovani"""
    df = df.astype(column_types)
    df['p37'] = pd.to_numeric(df['p37'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['n'] = pd.to_numeric(df['n'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['r'] = pd.to_numeric(df['r'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['s'] = pd.to_numeric(df['s'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df.rename(columns={'p2a': 'date'}, inplace=True)
    df.drop(df.loc[~np.isfinite(df['d'])].index, inplace=True)
    df.drop(df.loc[~np.isfinite(df['e'])].index, inplace=True)
    df.drop(df.loc[~np.isfinite(df['f'])].index, inplace=True)
    df.drop(df.loc[~np.isfinite(df['g'])].index, inplace=True)
    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.d, df.e), crs='epsg:5514')  # CRS křovák
    return gdf


def plot_geo(gdf: geopandas.GeoDataFrame, fig_location: str = None,
             show_figure: bool = False):
    """ Vykresleni grafu s dvemi podgrafy podle lokality nehody """
    fig, ax = plt.subplots(1, 2, sharex=True, sharey=True, figsize=(7.75, 4))
    fig.suptitle("Nehody v/mimo obec v Jihočeském kraji")
    ax[0].axis("off")
    ax[1].axis("off")
    gdf2 = gdf.loc[gdf["region"] == "JHC"]
    gdf2 = gdf2.to_crs("epsg:3857")
    gdf2[gdf2["p5a"] == 1].plot(ax=ax[0], color="blue", markersize=1)  # V obci
    ax[0].set_title('Nehody v obci')
    gdf2[gdf2["p5a"] == 2].plot(ax=ax[1], color="red", markersize=1)  # Mimo obec
    ax[1].set_title('Nehody mimo obec')
    ctx.add_basemap(ax[0], crs=gdf2.crs.to_string(), source=ctx.providers.Stamen.TonerLite, zoom=8, alpha=0.9)
    ctx.add_basemap(ax[1], crs=gdf2.crs.to_string(), source=ctx.providers.Stamen.TonerLite, zoom=8, alpha=0.9)
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def plot_cluster(gdf: geopandas.GeoDataFrame, fig_location: str = None,
                 show_figure: bool = False):
    """ Vykresleni grafu s lokalitou vsech nehod v kraji shlukovanych do clusteru """
    fig, ax = plt.subplots(1, 1, sharex=True, sharey=True, figsize=(7.75, 7.75))
    ax.axis("off")
    gdf2 = gdf.loc[gdf["region"] == "STC"]  # Z celého DataFrame vybereme pouze data z vybraného kraje
    gdf2 = gdf2.to_crs("epsg:3857")  # Transformujeme body do crs vhodného pro zobrazení
    coords = np.dstack([gdf2.geometry.x, gdf2.geometry.y]).reshape(-1, 2)
    kmeans = sklearn.cluster.MiniBatchKMeans(n_clusters=10).fit(coords)
    gdf3 = geopandas.GeoDataFrame(geometry=gdf2.geometry, crs=gdf2.crs.to_string())
    gdf3["cluster"] = kmeans.labels_
    gdf3["count"] = 1
    gdf3 = gdf3.dissolve(by="cluster", aggfunc={"count": "sum"})
    cluster_gdf = geopandas.GeoDataFrame(geometry=geopandas.points_from_xy(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1]))
    gdf3 = gdf3.merge(cluster_gdf, left_on="cluster", right_index=True).set_geometry("geometry_y")
    gdf3.plot(ax=ax, markersize=gdf3["count"] / 5, column="count", legend=True, alpha=.6)
    ax.set_title('Nehody ve Strakonickém kraji')
    # Přidáme mapový podklad
    ctx.add_basemap(ax, crs=gdf2.crs.to_string(), source=ctx.providers.Stamen.TonerLite, zoom=8, alpha=0.9)
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()
# %%
if __name__ == "__main__":
    # zde muzete delat libovolne modifikace
    gdf = make_geo(pd.read_pickle("accidents.pkl.gz"))
    plot_geo(gdf, "geo1.png", True)
    plot_cluster(gdf, "geo2.png", True)
