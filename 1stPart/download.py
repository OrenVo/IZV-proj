#!/usr/bin/python3
####################################################
# Author: Vojtěch Ulej (xulejv00)                  #
# Created: 28.10. 2020                             #
# Description: Implementation of data downloader   #
####################################################

import requests
import zipfile
import pickle
import os
import sys
import csv
import gzip
import time
from bs4 import BeautifulSoup
from zipfile import ZipFile
import numpy as np
from io import TextIOWrapper

# Abbreviations of regions assigned to names of csv files
regions_dict = {
            "PHA" : "00.csv",
            "STC" : "01.csv",
            "JHC" : "02.csv",
            "PLK" : "03.csv",
            "KVK" : "19.csv",
            "ULK" : "04.csv",
            "LBK" : "18.csv",
            "HKK" : "05.csv",
            "PAK" : "17.csv",
            "OLK" : "14.csv",
            "MSK" : "07.csv",
            "JHM" : "06.csv",
            "ZLK" : "15.csv",
            "VYS" : "16.csv",
}

# Names of each collum in csv files
coll_names = ["Region",                                     # viz klíče z regions_dict
              "ID",                                         # p1; identifikační číslo
              "Druh pozemní komunikace",                    # p36
              "Č. pozemní komunikace",                      # p37
              "Datum a čas",                                # p2a
              "Den v týdnu",                                # p2a(weekday)
              "Čas",                                        # p2b
              "Druh nehody",                                # p6
              "Druh srážky",                                # p7
              "Druh pevné překážky",                        # p8
              "Charakter",                                  # p9
              "Zavinění nehody",                            # p10
              "Alkohol u viníka",                           # p11
              "Hl. příčina",                                # p12
              "Usmrceno osob",                              # p13a
              "Těžce zraněno osob",                         # p13b
              "Lehce zraněno osob",                         # p13c
              "Hmotná škoda",                               # p14
              "Povrch vozovky",                             # p15
              "Stav povrchu vozovky v době nehody",         # p16
              "Stav komunikace",                            # p17
              "Povětrnostní podmínky v době n.",            # p18
              "Viditelnost",                                # p19
              "Rozhledové poměry",                          # p20
              "Dělení komunikace",                          # p21
              "Situování nehody na komunikaci",             # p22
              "Řízení provozu v době n.",                   # p23
              "Místní úprava přednosti v jízdě",            # p24
              "Specifická místa a objekty v místě",         # p27
              "Směrové poměry",                             # p28
              "Počet zúčastněných vozidel",                 # p34
              "Místo dopravní nehody",                      # p35
              "Druh křižující komunikace",                  # p39
              "Druh vozidla",                               # p44
              "Výrobní značka vozidla",                     # p45a
              "Rok výroby vozidla",                         # p47
              "Charakteristika vozidla",                    # p48a
              "Smyk",                                       # p49
              "Vozidlo po nehodě",                          # p50a
              "Únik provozních, přepravovaných hmot",       # p50b
              "Způsob vyproštění osob z vozidla",           # p51
              "Směr jízdy nebo postavení vozidla",          # p52
              "Škoda na vozidle",                           # p53
              "Kategorie řidiče",                           # p55a
              "Stav řidiče",                                # p57
              "Vnější ovlivnění řidiče",                    # p58
              "a",                                          # a
              "b",                                          # b
              "Souřadnice X",                               # d
              "Souřadnice Y",                               # e
              "f",                                          # f
              "g",                                          # g
              "h",                                          # h
              "i",                                          # i
              "j",                                          # j
              "k",                                          # k
              "l",                                          # l
              "n",                                          # n
              "o",                                          # o
              "p",                                          # p
              "g",                                          # g
              "r",                                          # r
              "s",                                          # s
              "t",                                          # t
              "Lokalita nehody",                            # p5a
              ]
# Works only till year 2020 files for 2021 would not be added
class DataDownloader:

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        self.url = url
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
        self.folder = folder
        if not os.path.exists(folder):
            os.makedirs(folder)
        self.cache_filename = cache_filename
        self.parsed_region = None
        self.data_downloaded = False
        # Files processed by data downloader for last years
        # Those file names are hardcoded, because I don't expect that file names for older years would be changed
        # If those filenames would change these values needs to be changed as-well
        self.files_to_process = ["datagis2016.zip", "datagis-rok-2017.zip", "datagis-rok-2018.zip", "datagis-rok-2019.zip"]

    def download_data(self):
        """Downloads data from url to folder"""
        s = requests.session()
        jar = requests.cookies.RequestsCookieJar()
        s.cookies = jar
        resp = s.get(self.url, headers=self.headers)
        soup = BeautifulSoup(resp.content, 'html.parser')
        download_links = [x['href'] for x in soup.find_all('a', class_= "btn btn-sm btn-primary")]
        self.choose_files_to_download([x[5:] for x in download_links])
        for link in download_links: # Donwload only latest files (for years 2016-2019 are hardcoded to self.files_to_process, 2020 is choosed by function self.choose_files_to_download)
            if link[5:] in self.files_to_process:
                self.download_file(link)
        self.data_downloaded = True

    # Choose latest file for year 2020
    def choose_files_to_download(self, files_names):
        files_20 = [f for f in files_names if "2020" in f]
        for file in files_20:
            if 'rok' in file:
                self.files_to_process.append(file)
                return
        files_20.sort(reverse=True,key=lambda x:int(x[8:10]))
        self.files_to_process.append(files_20[0])



    def download_file(self, file_url_path):
        s = requests.session()
        jar = requests.cookies.RequestsCookieJar()
        s.cookies = jar
        with s.get(self.url + file_url_path, headers=self.headers, stream = True) as resp:
            if not os.path.exists(self.folder + '/' + file_url_path.split('/')[-1]): # Checking if file is already downloaded
                with open(self.folder + '/' + file_url_path.split('/')[-1],'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

    def parse_region_data(self, region):
        if not self.data_downloaded:
            self.download_data()
        if not region in regions_dict:          # Check if region is valid
            print(f"Region {region} not known!", file=sys.stderr)
            return

        csv_fname = regions_dict[region]        # csv file name from dictionary
        zip_files = [zip_file for zip_file in os.listdir(self.folder) if zipfile.is_zipfile(self.folder + '/' + zip_file)]  # filter non zip files from dir
        rows = []
        for zip_file in self.files_to_process:
            with ZipFile( self.folder + '/' + zip_file, 'r') as zip:
                if not csv_fname in zip.namelist():
                    continue
                with zip.open(csv_fname, 'r') as data:
                    reader = csv.reader(TextIOWrapper(data, encoding = "windows-1250"), delimiter=';')
                    for row in reader:
                        # Check and filter data
                        try:
                            row[3] = np.datetime64(row[3])
                        except ValueError: # Skip whole row (cannot get date)
                            row[3] = np.datetime64('NaT')
                        for r in [range(0,3), range(4,45), [len(row)-1]]:
                            for i in r:
                                try:
                                    row[i] = int(row[i])
                                except ValueError:
                                    row[i] = -1

                        for i in range(47,51):
                            try:
                                row[i] = float(row[i].replace(',', '.', 1))
                            except ValueError:
                                row[i] = -1.0
                        rows.append(row)
        np_arr = np.array(rows, order='F')
        #for i in range(0,np_arr.shape[1]): #
        #    np_arr[:, i][np_arr[:, i] == ''] = '-1' # Change missing value to int string representation will be representet as max for uint
        list = [np.full(fill_value=region,shape=np_arr.shape[0])] # Create colum full of region name
        try:
            list.append(np.array(np_arr[:, 0], dtype=np.uint64, order='C'))
            list.append(np.array(np_arr[:, 1], dtype=np.int8, order='C'))
            list.append(np.array(np_arr[:, 2], dtype=np.int32, order='C'))
            list.append(np.array(np_arr[:, 3], dtype=np.datetime64, order='C'))
            list.append(np.array(np_arr[:, 4], dtype=np.uint8, order='C'))
            # Time will be presented as it was given; unknown time will be set to -1
            np_arr[:, 5][np_arr[:, 5] == '2560'] = '-1'
            np_arr[:, 5][np_arr[:, 5] == '25'] = '-1'
            list.append(np.array(np_arr[:, 5], dtype=np.int16, order='C'))

            for i in range(6,16):
                if i in range(12,16): # p12 - p13c uint16
                    list.append(np.array(np_arr[:, i], dtype=np.uint16, order='C'))
                else:       # p6 - p11 uint8
                    list.append(np.array(np_arr[:, i], dtype=np.uint8, order='C'))

            list.append(np.array(np_arr[:, 16], dtype=np.int64, order='C')) # Hmotná škoda p14
            #np_arr[:, 34][np_arr[:, 34] == 'XX'] = '-1'
            for i in range(17,41): # p15 - p45a - p
                list.append(np.array(np_arr[:, i], dtype=np.uint8, order='C'))
            list.append(np.array(np_arr[:, 41], dtype=np.uint64, order='C')) # Škoda na vozidle p53
            for i in range(42,45): # p55a - p58
                list.append(np.array(np_arr[:, i], dtype=np.uint8, order='C'))

            list.append(np.array(np_arr[:, 45], order='C')) # a
            list.append(np.array(np_arr[:, 46], order='C')) # b

            for i in range(47,51):
                list.append(np.array(np_arr[:, i], dtype=np.single, order='C')) # d GPS X; e GPS Y; f and g some float

            for i in range(51,63): # h - t store as string; its usually blank
                list.append(np.array(np_arr[:, i], order='C'))
            list.append(np.array(np_arr[:, 63], dtype=np.uint8, order='C'))
        except ValueError as er:
            print(region)
            raise er
        assert(len(list) == 65)
        assert (len(list) == len(coll_names))

        return coll_names.copy(), list.copy()

    def get_list(self, regions = None):
        parsed_regions = []
        if regions is None:
            regions = regions_dict.keys()
        for reg in regions:
            out_fname = self.cache_filename.format(reg)
            if self.parsed_region is not None:
                if np.all(self.parsed_region[1][0] == reg):  # Check if region is parsed and cached
                    parsed_regions.append(self.parsed_region[1])
                    continue
            if os.path.exists(self.folder + '/' + out_fname): # Check if region is parsed and cached in file
                with gzip.open(self.folder + '/' + out_fname,'rb') as pkl:
                    parsed_regions.append(pickle.load(pkl)[1])
                    continue
            self.parsed_region = self.parse_region_data(reg)
            with gzip.open(self.folder + '/' + out_fname,'wb') as of: # Save result to cache
                pickle.dump(self.parsed_region,of)
            parsed_regions.append(self.parsed_region[1])
        it = iter(parsed_regions)
        result = next(it) # final numpy array declaration
        for arrays in it:
            for i, a in enumerate(result):
                result[i] = np.concatenate((a,arrays[i]))
        return coll_names.copy(), result.copy()


if __name__ == "__main__":
    regions = ["PHA", "JHC", "JHM"]
    a = DataDownloader()
    a.download_data()
    start = time.time()
    data = a.get_list(regions)
    print("Sloupce:")
    for c in data[0]:
        print(c, end=" | ")
    print("\n\nKraj | počet záznamů" )
    print(regions[0], " |", np.count_nonzero(data[1][0] == regions[0]))
    print(regions[1], " |", np.count_nonzero(data[1][0] == regions[1]))
    print(regions[2], " |", np.count_nonzero(data[1][0] == regions[2]))
    print("\nCelkem záznamů:",data[1][0].shape[0])


