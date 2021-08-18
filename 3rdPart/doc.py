#!/usr/bin/env python3.8
#%%
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os
import sys


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
car_brand = {
    1:	'ALFA-ROMEO',
    2:	'AUDI',
    3:	'AVIA',
    4:	'BMW',
    5:	'CHEVROLET',
    6:	'CHRYSLER',
    7:	'CITROEN',
    8:	'DACIA',
    9:	'DAEWOO',
    10:	'DAF',
    11:	'DODGE',
    12:	'FIAT ',
    13:	'FORD',
    14:	'GAZ, VOLHA',
    15:	'FERRARI',
    16:	'HONDA',
    17:	'HYUNDAI',
    18:	'IFA',
    19:	'IVECO',
    20:	'JAGUAR',
    21:	'JEEP',
    22:	'LANCIA',
    23:	'LAND ROVER',
    24:	'LIAZ',
    25:	'MAZDA',
    26:	'MERCEDES',
    27:	'MITSUBISHI',
    28:	'MOSKVIČ',
    29:	'NISSAN',
    30:	'OLTCIT',
    31:	'OPEL',
    32:	'PEUGEOT',
    33:	'PORSCHE',
    34:	'PRAGA',
    35:	'RENAULT',
    36:	'ROVER',
    37:	'SAAB',
    38:	'SEAT',
    39:	'ŠKODA',
    40:	'SCANIA',
    41:	'SUBARU',
    42:	'SUZUKI',
    43:	'TATRA',
    44:	'TOYOTA',
    45:	'TRABANT',
    46:	'VAZ',
    47:	'VOLKSWAGEN',
    48:	'VOLVO',
    49:	'WARTBURG',
    50:	'ZASTAVA',
    51:	'AGM',
    52:	'ARO',
    53:	'AUSTIN',
    54:	'BARKAS',
    55:	'DAIHATSU',
    56:	'DATSUN',
    57:	'DESTACAR',
    58:	'ISUZU',
    59:	'KAROSA',
    60:	'KIA',
    61:	'LUBLIN',
    62:	'MAN',
    63:	'MASERATI',
    64:	'MULTICAR',
    65:	'PONTIAC',
    66:	'ROSS',
    67:	'SIMCA',
    68:	'SSANGYONG',
    69:	'TALBOT',
    70:	'TAZ',
    71:	'ZAZ',
}


def get_dataframe(filename: str) -> pd.DataFrame:
    if not os.path.exists(filename):  # Check if file exist
        print(f'File {filename} doesn\'t exist.', file=sys.stderr)
        raise ValueError(f'File: {filename} doesn\'t exists!')
    df = pd.read_pickle(filename)
    df = df.astype(column_types)
    df['p37'] = pd.to_numeric(df['p37'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['n'] = pd.to_numeric(df['n'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['r'] = pd.to_numeric(df['r'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df['s'] = pd.to_numeric(df['s'], errors='coerce').astype('Int32')  # Coerce missing values to NA
    df.rename(columns={'p2a': 'date'}, inplace=True)
    return df


def plot_cars(df: pd.DataFrame, fname: str = None, show: bool = False):
    """ Vykreslí graf nejvíce bouraných značek osobních a nákladních aut. """
    fig, ax = plt.subplots(1, 1)
    df_cars = pd.DataFrame({'znacky': df.loc[(df['p44'] >= 3) & (df['p44'] <= 7)]['p45a']})
    df_cars = df_cars[df_cars['znacky'] != -1]
    df_cars['pocet'] = 1
    df_cars = df_cars.groupby('znacky').count()
    df_cars.rename(index=car_brand, inplace=True)
    df_cars_major = df_cars[df_cars['pocet'] >= 8000]
    sns.barplot(x=df_cars_major.index, y='pocet', data=df_cars_major, order=df_cars_major.sort_values('pocet', ascending=False).index, ax=ax, palette=sns.color_palette("magma", n_colors=16))
    ax.set_xticklabels(ax.get_xticklabels(), rotation=60)
    ax.set_title(f"Nejvíce bourané značky aut v letech {df.date.min().year} - {df.date.max().year}")
    ax.set_xlabel('Značky')
    ax.set_ylabel('Počet nehod na území ČR')
    fig.tight_layout()
    if fname:
        fig.savefig(fname)
    if show:
        fig.show()


def table(df: pd.DataFrame):
    df_new = df[(df['p13a'] != -1) & (df['p13b'] != -1) & (df['p13c'] != -1) & (df['p44'] >= 3) & (df['p44'] <= 7) & (df['p45a'] != 99) & (df['p45a'] != 98) & (df['p45a'] != 0) & (df['p45a'] != 94)]
    df_new = pd.DataFrame({'znacky': df_new['p45a'], 'zraneni': df_new['p13a'] + df_new['p13b'] + df_new['p13c']})
    df_new['pocet'] = 1
    df_new = df_new.groupby('znacky').sum()
    df_new['prumer'] = df_new['zraneni'] / df_new['pocet']
    df_new.rename(index=car_brand, inplace=True)
    df_new.sort_values('prumer', inplace=True, ascending=False)
    df_new = df_new[df_new['pocet'] > 1000]
    print('Tabulka 5 značek s nejvyšší průměrnou škodou na vozidle')
    print(df_new.iloc[0:5].to_latex())


def values(df: pd.DataFrame):
    # Značka auta + nejvyšší škody na autech
    df_new = df[(df['p53'] != -1) & (df['p44'] >= 3) & (df['p44'] <= 7)]
    df_new = pd.DataFrame({'znacky': df_new['p45a'], 'skoda': df_new['p53']})
    df_new['skoda'] = 100 * df_new['skoda']
    df_new['pocet'] = 1
    df_new = df_new.groupby('znacky').sum()
    df_new.rename(index=car_brand, inplace=True)
    df_new['prumerna skoda'] = df_new['skoda'] / df_new['pocet']
    df_new.sort_values('prumerna skoda', inplace=True, ascending=False)
    print("Hodnoty:")
    print(f"\tZnačka auta s nejvyší průměrnou škodou/1 nehodu: {df_new.iloc[0].name} {round(df_new.iloc[0]['prumerna skoda'], 2)}")
    # Značka auta + nejvíce nehod pod vlivem návykových látek
    df_new = df[(df['p11'] != 0) & (df['p11'] != -1) & (df['p53'] != -1) & (df['p44'] >= 3) & (df['p44'] <= 7)]
    df_new = pd.DataFrame({'znacky': df_new['p45a'], 'navikove latky': df_new['p11']})
    df_new['pod vlivem'] = 1
    df_new['pocet'] = 1
    df_new.loc[(df_new['navikove latky'] == 2), 'pod vlivem'] = 0
    df_new.drop("navikove latky", axis=1, inplace=True)
    df_new = df_new.groupby('znacky').sum()
    df_new.rename(index=car_brand, inplace=True)
    df_new = df_new[df_new['pocet'] > 1000]
    df_new['podil pod vlivem'] = df_new['pod vlivem'] / df_new['pocet']
    df_new.sort_values('podil pod vlivem', inplace=True, ascending=False)
    print(f"\tZnačka auta jehož řidiči mají nejčastěji nehodu pod vlivem návykových látek: {df_new.iloc[0].name} {round(df_new.iloc[0]['podil pod vlivem'], 2) * 100}%")
    # Značka auta + nejvíce nehod při vysoké rychlosti
    df_new = df[(df['p12'] != -1) & (df['p53'] != -1) & (df['p44'] >= 3) & (df['p44'] <= 7)]
    df_new = pd.DataFrame({'znacky': df_new['p45a'], 'pricina': df_new['p12']})
    df_new['rychla jizda'] = 0
    df_new['pocet'] = 1
    df_new.loc[(df_new['pricina'] >= 201) & (df_new['pricina'] <= 209), 'rychla jizda'] = 1
    df_new.drop("pricina", axis=1, inplace=True)
    df_new = df_new.groupby('znacky').sum()
    df_new.rename(index=car_brand, inplace=True)
    df_new = df_new[df_new['pocet'] > 1000]
    df_new['podil rychle jizdy'] = df_new['rychla jizda'] / df_new['pocet']
    df_new.sort_values('podil rychle jizdy', inplace=True, ascending=False)
    print(f"\tZnačka auta jehož řidiči mají nejčastěji nehodu kvůli vysoké rychlosti: {df_new.iloc[0].name} {round(df_new.iloc[0]['podil rychle jizdy'], 2) * 100}%")
#%%
if __name__ == '__main__':
    df = get_dataframe('accidents.pkl.gz')
    plot_cars(df, 'fig.png')
    table(df)
    values(df)
