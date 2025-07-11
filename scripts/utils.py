from scripts import config
import os
import datetime
from dateutil import relativedelta


def get_mediawiki_paths(languagecode):

    year_month = (datetime.date.today() -
                  relativedelta.relativedelta(months=1)).strftime('%Y-%m')

    if not os.path.exists(config.dumps_path + f"/{year_month}"):
        year_month = (datetime.date.today() -
                      relativedelta.relativedelta(months=2)).strftime('%Y-%m')

    path = config.dumps_path + f"/{year_month}"

    dir_name = languagecode + "wiki/"
    cy = datetime.datetime.now().year
    d_paths = []

    yearly = False
    all_time = False

    file_name = f'{year_month}.{languagecode}wiki.all-time.tsv.bz2'
    abs_path = path + dir_name + file_name
    print(abs_path)
    if os.path.isfile(abs_path):
        d_paths.append(abs_path)
        all_time = True

    if not all_time:
        for year in range(2001, cy+1):
            file_name = f'{year_month}.{languagecode}wiki.{year}.tsv.bz2'
            abs_path = path + dir_name + file_name
            print(abs_path)
            if os.path.isfile(abs_path):
                d_paths.append(abs_path)
                yearly = True

    if not yearly or not all_time:
        for year in range(2001, cy+1):
            for month in range(1, 13):
                file_name = f'{year_month}.{languagecode}wiki.{year}-{month:02d}.tsv.bz2'
                abs_path = path + dir_name + file_name
                print(abs_path)
                if os.path.isfile(abs_path):
                    d_paths.append(abs_path)

    return d_paths, year_month
