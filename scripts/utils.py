from scripts import config
import os
import datetime
from dateutil import relativedelta


def get_mediawiki_paths(languagecode):

    year_month = (datetime.date.today() -
                  relativedelta.relativedelta(months=1)).strftime('%Y-%m')

    path = os.path.join(config.dumps_path, year_month)

    if not os.path.exists(path):
        year_month = (datetime.date.today() -
                      relativedelta.relativedelta(months=2)).strftime('%Y-%m')
        path = os.path.join(config.dumps_path, year_month)

    dir_name = languagecode + "wiki"
    path_dir = os.path.join(path, dir)
    cy = datetime.datetime.now().year
    d_paths = []

    file_name = f'{year_month}.{languagecode}wiki.all-time.tsv.bz2'
    abs_path = os.path.join(path_dir, file_name)

    if os.path.isfile(abs_path):
        d_paths.append(abs_path)
    else:

        if languagecode == 'en':
            for year in range(2001, cy+1):
                for month in range(1, 13):
                    file_name = f'{year_month}.{languagecode}wiki.{year}-{month:02d}.tsv.bz2'
                    abs_path = path + dir_name + file_name

                    if os.path.isfile(abs_path):
                        d_paths.append(abs_path)
        else:
            for year in range(2001, cy+1):
                file_name = f'{year_month}.{languagecode}wiki.{year}.tsv.bz2'
                abs_path = path + dir_name + file_name

                if os.path.isfile(abs_path):
                    d_paths.append(abs_path)

    print(d_paths)

    return d_paths, year_month
