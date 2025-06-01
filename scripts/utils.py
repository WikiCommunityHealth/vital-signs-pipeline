from scripts import config
import os
import datetime
from dateutil import relativedelta

year_month = (datetime.date.today() -
              relativedelta.relativedelta(months=2)).strftime('%Y-%m')


def get_mediawiki_paths(languagecode):
    dir_name = languagecode + "wiki/"
    cy = datetime.datetime.now().year
    d_paths = []

    if dir_name == "itwiki/":
    # Cerca i file annuali
        for year in range(1999, cy+1):
            file_name = f'{year_month}.{languagecode}wiki.{year}.tsv.bz2'
            dumps_path = config.dumps_path + dir_name + file_name
            print(dumps_path)
            if os.path.isfile(dumps_path):
                d_paths.append(dumps_path)
    else:
        file_name = f'{year_month}.{languagecode}wiki.all-time.tsv.bz2'
        dumps_path = config.dumps_path + dir_name + file_name
        print(dumps_path)
        if os.path.isfile(dumps_path):
            d_paths.append(dumps_path)

    return d_paths, year_month
