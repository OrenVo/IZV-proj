####################################################
# Author: Vojtěch Ulej (xulejv00)                  #
# Created: 28.10. 2020                             #
# Description: Implementation of data downloader   #
####################################################

import requests
import zipfile
import os
import sys
import csv
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
# Files processed by data downloader
files_to_process = ["datagis2016.zip", "datagis-rok-2017.zip", "datagis-rok-2018.zip", "datagis-rok-2019.zip"]

# Names of each collum in csv files
coll_names = ["Region",                                     # viz klíče z regions_dict
              "ID",                                         # p1; identifikační číslo
              "Druh pozemní komunikace",                    # p36
              "Č. pozemní komunikace",                      # p37
              "Datum",                                      # p2a
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
              "Nepopsáno",                                  # a
              "Nepopsáno",                                  # b
              "Souřadnice X",                               # d
              "Souřadnice Y",                               # e
              "Nepopsáno",                                  # f
              "Nepopsáno",                                  # g
              "Nepopsáno",                                  # h
              "Nepopsáno",                                  # i
              "Nepopsáno",                                  # j
              "Nepopsáno",                                  # k
              "Nepopsáno",                                  # l
              "Nepopsáno",                                  # n
              "Nepopsáno",                                  # o
              "Nepopsáno",                                  # p
              "Nepopsáno",                                  # q
              "Nepopsáno",                                  # g
              "Nepopsáno",                                  # r
              "Nepopsáno",                                  # s
              "Nepopsáno",                                  # t
              "Lokalita nehody"]                            # p5a


def add_file_to_proccess(file):
    """ Adds file to list of files to be processed """
    files_to_process.append(file)

class DataDownloader:

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        self.url = url
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
        self.folder = folder
        if not os.path.exists(folder):
            os.makedirs(folder)
        self.cache_filename = cache_filename
        self.parsed_region = None

    def download_data(self):
        """Downloads data from url to folder"""
        s = requests.session()
        jar = requests.cookies.RequestsCookieJar()
        s.cookies = jar
        resp = s.get(self.url, headers=self.headers)
        soup = BeautifulSoup(resp.content, 'html.parser')
        download_links = [x['href'] for x in soup.find_all('a', class_= "btn btn-sm btn-primary")]
        for link in download_links:
            self.download_file(link)



    def download_file(self, file_url_path):
        s = requests.session()
        jar = requests.cookies.RequestsCookieJar()
        s.cookies = jar
        with s.get(self.url + file_url_path, headers=self.headers, stream = True) as resp:
            if not os.path.exists(self.folder + '/' + file_url_path.split('/')[-1]):
                with open(self.folder + '/' + file_url_path.split('/')[-1],'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

    def parse_region_data(self, region):
        if not region in regions_dict:
            print(f"Region {region} not known!", file=sys.stderr)
            return
        if self.parsed_region[1][0] == region:
            return self.parsed_region
        csv_fname = regions_dict[region]
        zip_files = [zip_file for zip_file in os.listdir(self.folder) if zipfile.is_zipfile(self.folder + '/' + zip_file)]
        for file in files_to_process:
            if not file in zip_files:
                self.download_file('data/' + file)
        for zip_file in zip_files:
            with ZipFile( self.folder + '/' + zip_file, 'r') as zip:
                if not csv_fname in zip.namelist():
                    continue
                with zip.open(csv_fname, 'r') as data:
                    reader = csv.reader(TextIOWrapper(data, encoding = "windows-1250"), delimiter=';')



    def get_list(self, regions = None):
        for reg in regions:
            out_fname = self.cache_filename.format(region)
            ...

if __name__ == "__main__":
    #zip_files = [zip_file for zip_file in os.listdir('data/') if zipfile.is_zipfile('data/'+zip_file)]
    #files = None
    #f = zip_files[0]
    #with ZipFile('data/' + f, 'r') as zip:
    #    files = zip.namelist()
    #    print(f"Reading {files[0]} file.")
    #    with zip.open(files[0], 'r') as data:
    #        reader = csv.reader(TextIOWrapper(data, encoding = "windows-1250"), delimiter=';')
    #        for row in reader:
    #            print(row)
    #            break
    add_file_to_proccess("datagis-09-2020.zip")
    a = DataDownloader()
    a.parse_region_data("PHA")