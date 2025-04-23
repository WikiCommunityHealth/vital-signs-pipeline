from scripts import config
import os
import datetime
from dateutil import relativedelta

cycle_year_month = (datetime.date.today() -
                        relativedelta.relativedelta(months=2)).strftime('%Y-%m')

def get_cleaned_subdirectories(path=config.dumps_path):
    subdirs = [d for d in os.listdir(
        path) if os.path.isdir(os.path.join(path, d))]
    # Rimuove "wiki" dalla fine
    cleaned_names = [d[:-4] if d.endswith("wiki") else d for d in subdirs]
    return cleaned_names


def get_mediawiki_paths(languagecode):
    cym = cycle_year_month
    cy = datetime.datetime.now().year
    d_paths = []


    # Percorso del file all-time
    dumps_path = f'{config.dumps_path}/{languagecode}wiki/{cym}.{languagecode}wiki.all-time.tsv.bz2'
    if os.path.isfile(dumps_path):
        d_paths.append(dumps_path)
    else:
        # Cerca i file annuali
        for year in range(1999, cy):
            dumps_path = f'{config.dumps_path}/{languagecode}wiki/{year}.tsv.bz2'
            if os.path.isfile(dumps_path):
                d_paths.append(dumps_path)

        # Cerca i file mensili se non trova quelli annuali
        if not d_paths:
            for year in range(1999, cy):
                for month in range(1, 13):
                    month_str = f"{month:02d}"
                    dumps_path = f'{config.dumps_path}/{languagecode}wiki/{year}-{month_str}.tsv.bz2'
                    if os.path.isfile(dumps_path):
                        d_paths.append(dumps_path)

    return d_paths, cym