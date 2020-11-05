#!/usr/bin/python3
import numpy as np
import download
import os
import matplotlib.pyplot as plt


def plot_stat(data_source, fig_location=None, show_figure=False):
    regs = data_source[1][0]
    dates = data_source[1][4]
    regions = np.unique(regs)
    years = set()
    for d in dates:
        years.add(d.astype(object).year)
    years = sorted(years)
    accidents_by_reg_year = []
    for r in regions:
        accidents_dates_by_reg = dates[regs == r]
        for i in range(len(years)):
            if i != len(years) - 1:
                count = np.count_nonzero(accidents_dates_by_reg >= np.datetime64(str(years[i]))) - np.count_nonzero(
                    accidents_dates_by_reg >= np.datetime64(str(years[i + 1])))
            else:  # last year
                count = np.count_nonzero(accidents_dates_by_reg >= np.datetime64(str(years[i])))
            accidents_by_reg_year.append((r, years[i], count))
    accidents_by_reg_year.sort(key=lambda x: x[2], reverse=True)  # Sort by count
    accidents_by_reg_year.sort(key=lambda x: x[1], reverse=True)  # Sort by year
    i = 0
    data_to_plot = []
    while i != len(accidents_by_reg_year):
        data_to_plot.append(tuple(accidents_by_reg_year[i:i + len(regions)]))
        i += len(regions)
    for data in data_to_plot:
        # TODO podgraf pro každý rok (něco jako iss)
        fig, ax = plt.subplots()
        rects = ax.bar([i[0] for i in data], [i[2] for i in data])
        for i, rect in enumerate(rects):
            ax.annotate(str(i+1) + '.',
                        xy=(rect.get_x() + rect.get_width() / 2, rect.get_height()),
                        xytext=(0, 1),
                        textcoords="offset points",
                        ha='center', va='bottom'
                        )
        plt.ylabel('Počet nehod')
        plt.xlabel('Kraje')
        plt.title('Srovnání počtu nehod v krajích za rok ' + str(data[0][1]))
        fig.tight_layout()
        if show_figure:
            plt.show()
        if fig_location is not None:
            if not os.path.exists('/'.join(fig_location.split('/')[:-1])):
                os.makedirs('/'.join(fig_location.split('/')[:-1])) # create dir
            plt.savefig(fig_location) # TODO doesn't work


if __name__ == "__main__":
    plot_stat(data_source=download.DataDownloader().get_list(['JHC', 'PHA', 'JHM']), fig_location='data/plot.png', show_figure=True)
    data_source = download.DataDownloader().get_list(['JHC', 'PHA', 'JHM'])
