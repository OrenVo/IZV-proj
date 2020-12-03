#!/usr/bin/env python3.8
# coding=utf-8

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os
from sys import stderr

# muzete pridat libovolnou zakladni knihovnu ci knihovnu predstavenou na prednaskach
# dalsi knihovny pak na dotaz


def eprint(*args, **kwargs):
    """Prints error messages to stderr."""
    print(*args, file=stderr, **kwargs)


column_types = {
    "p1": 'int64',
    "p36": 'int8',
    #"p37": 'int32',
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


# Ukol 1: nacteni dat
def get_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:
    if not os.path.exists(filename):
        eprint(f'File {filename} doesn\'t exist.')
        return pd.DataFrame()
    df = pd.read_pickle(filename)
    if verbose:
        print("orig_size={:.1f}".format(sum(df.memory_usage(deep=True))/1_048_576), "MB")
    df.replace('', pd.NA, inplace=True)
    df = df.astype(column_types)
    df['p37'] = pd.to_numeric(df['p37'], errors='coerce').astype('Int32')
    df['n'] = pd.to_numeric(df['n'], errors='coerce').astype('Int32')
    df['r'] = pd.to_numeric(df['r'], errors='coerce').astype('Int32')
    df['s'] = pd.to_numeric(df['s'], errors='coerce').astype('Int32')
    df.rename(columns={'p2a': 'date'}, inplace=True)
    if verbose:
        print("new_size={:.1f}".format(sum(df.memory_usage(deep=True))/1_048_576), "MB")
    return df


# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None, show_figure: bool = False):
    fig, ax = plt.subplots(4, 1, figsize=(7.75, 10.25))
    ax[0].set_xlabel('Regiony')
    ax[1].set_xlabel('Regiony')
    ax[2].set_xlabel('Regiony')
    ax[3].set_xlabel('Regiony')
    sns.barplot(x='region', y='p13a', data=df, order=df['region'].value_counts().index, ax=ax[0], ci=None, estimator=sum)
    ax[0].set_ylabel('')
    sns.barplot(x='region', y='p13b', data=df, order=df['region'].value_counts().index, ax=ax[1], ci=None, estimator=sum)
    ax[1].set_ylabel('')
    sns.barplot(x='region', y='p13c', data=df, order=df['region'].value_counts().index, ax=ax[2], ci=None, estimator=sum)
    ax[2].set_ylabel('')
    sns.countplot(x='region', data=df, order=df['region'].value_counts().index, ax=ax[3])
    ax[3].set_ylabel('')

    if fig_location:
        fig.savefig(fig_location)
    if show_figure is True:
        fig.show()



# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None, show_figure: bool = False):
    pass


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None, show_figure: bool = False):
    pass


if __name__ == "__main__":
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz", True)
    plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
