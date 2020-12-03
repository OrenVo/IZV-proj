#!/usr/bin/python3
#####################################################
# Author: Vojtěch Ulej (xulejv00)                   #
# Created: 4. 11. 2020                              #
# Description: Implementation of function plot_stat #
#####################################################

import download
import os
from sys import argv
import code
import argparse
import numpy as np
import matplotlib.pyplot as plt

# Function takes data from data_source and plots them
def plot_stat(data_source, fig_location=None, show_figure=False):
    regs = data_source[1][0]  # np.array of regions
    dates = data_source[1][4] # np.array of dates
    regions = np.unique(regs) # array of unique regions
    years = set() # All years from dataset
    for d in dates:
        years.add(d.astype(object).year)
    years = sorted(years)
    accidents_by_reg_year = [] # Accidents in region and year (list of tupple(region, year, accidents_count))
    for r in regions:
        accidents_dates_by_reg = dates[regs == r]   # Accidents dates in region r
        for i in range(len(years)):
            if i != len(years) - 1:
                # Count of accidents in year years[i]
                # This is calculated ass accidents till year years[i] - accidents till year years[i+1]
                # eg. Accident for year >= 2016 - year >= 2017
                count = np.count_nonzero(accidents_dates_by_reg >= np.datetime64(str(years[i]))) - np.count_nonzero(
                    accidents_dates_by_reg >= np.datetime64(str(years[i + 1])))
            else:  # last year
                count = np.count_nonzero(accidents_dates_by_reg >= np.datetime64(str(years[i])))
            accidents_by_reg_year.append((r, years[i], count))
    # Sort accidents by year and count (reverse) so first is region with highest count of accidents in highest year (2020)
    accidents_by_reg_year.sort(key=lambda x: x[2], reverse=True)  # Sort by count
    accidents_by_reg_year.sort(key=lambda x: x[1], reverse=True)  # Sort by year
    i = 0
    data_to_plot = [] # every element is tuple of tuples(region,year,accident_count) where year is same
    while i != len(accidents_by_reg_year): #
        data_to_plot.append(tuple(accidents_by_reg_year[i:i + len(regions)]))
        i += len(regions)
    # Figure for ploting
    fig, ax = plt.subplots(len(data_to_plot),figsize=(7.75,10.25))  # figsize = a4 format in inches (nicer graphs for more regions)
    # This cycle creates subplots for each year
    for idx, data in enumerate(data_to_plot):
        rects = ax[idx].bar([i[0] for i in data], [i[2] for i in data])
        # Annotation
        for i, rect in enumerate(rects):
            ax[idx].annotate(str(i+1) + '.',
                        xy=(rect.get_x() + rect.get_width() / 2, rect.get_height()),
                        xytext=(0, 1),
                        textcoords="offset points",
                        ha='center', va='bottom'
                        )
        ax[idx].set_title(str(data[0][1])) # Title of subplot is year
        y_l = ax[idx].get_ylim()
        ax[idx].set_ylim([y_l[0],y_l[1]+((y_l[1]/100)*15)]) # Make room for annotation (adding 15% to y max)
        ax[idx].set_ylabel('  ') # Make room for figure text 'Počet nehod'
        if data is data_to_plot[-1]: # if making last plot create x lable (same for every subplot so its not neccessery to write it to every subplot)
            ax[idx].set_xlabel('Kraje')
    fig.suptitle('Srovnání počtu nehod v krajích za jednotlivé roky')
    fig.text(0.015, 0.5, 'Počet nehod', va='center', rotation='vertical')
    fig.tight_layout()
    if fig_location is not None: # if location specified save figure
        dire = '/'.join(fig_location.split('/')[:-1]) # directories name
        if dire != '':
            if not os.path.exists(dire):
                os.makedirs(dire) # create dir
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


if __name__ == "__main__":
    DataDownloader = download.DataDownloader
    parser = argparse.ArgumentParser(description='Plot data for regions given to function plot_stat.')
    # To this variables will be stored values from cmd arguments
    show_figure = False
    fig_location = None
    ############################################################
    parser.add_argument('--show_figure', dest='show_figure', action='store_const',const=True, default=False)
    parser.add_argument('--fig_location', dest='fig_location', type=str, action='store', default=None)
    args,_ = parser.parse_known_args(argv[1:])
    show_figure = args.show_figure
    fig_location = args.fig_location
    # Example how to run function:
    # plot_stat(DataDownloader().get_list(),show_figure=True,fig_location='data/plot.png')
    # Running interactive console so user can run function
    code.interact(local=locals())

