from scripts import config
import os
import sqlite3
import csv

import pandas as pd

import logging

logger = logging.getLogger(__name__)


def cross_wiki_editor_metrics(wikilanguagecodes):

    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    query = ("CREATE TABLE IF NOT EXISTS allwiki_editors (lang text, user_name text, edit_count integer, year_month_first_edit text, lustrum_first_edit text, PRIMARY KEY (lang, user_name));")
    cursor.execute(query)

    for languagecode in wikilanguagecodes:
        query = 'INSERT INTO allwiki_editors SELECT "'+languagecode + \
            '", user_name, edit_count, year_month_first_edit, lustrum_first_edit FROM ' + \
                languagecode+'wiki_editors WHERE user_name != "";'
        cursor.execute(query)
        conn.commit()

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
    edfile2 = open(config.databases_path+'temporary_editor_metrics.txt', "w")

    query = 'SELECT user_name, lang, edit_count, year_month_first_edit, lustrum_first_edit FROM allwiki_editors ORDER BY user_name, edit_count DESC;'

    columns = [
        "user_name",          # string
        "prim_lang",          # string (primary language)
        "prim_ecount",        # int (number of edits in primary language)
        "tot_ecount",        # int (total edit count)
        "n_langs",           # int (number of languages used)
        "prim_ym_first_e",   # ym first edit primary lang
        "prim_lus_first_e"   # lustrum first edit prim
    ]

    df = pd.DataFrame(columns=columns)

    for row in cursor.execute(query):

        user_name = row[0]
        lang = row[1]
        try:
            edit_count = int(row[2])
        except:
            edit_count = 0

        try:
            year_month_first_edit = str(row[3])
        except:
            year_month_first_edit = ''

        try:
            lustrum_first_edit = str(row[4])
        except:
            lustrum_first_edit = ''

        if user_name in df['user_name'].values:
            if edit_count > 4 and lang != "meta":
                df.loc[df['user_name'] == user_name, 'n_langs'] += 1

            if (edit_count >= df.loc[df['user_name'] == user_name, 'prim_ecount'] and lang != "meta"):
                df.loc[df['user_name'] == user_name, 'prim_lang'] = lang
                df.loc[df['user_name'] == user_name, 'prim_ecount'] = edit_count

                df.loc[df['user_name'] == user_name,
                       'prim_ym_first_e'] = year_month_first_edit
                df.loc[df['user_name'] == user_name,
                       'prim_lus_first_e'] = lustrum_first_edit

            df.loc[df['user_name'] == user_name, 'tot_ecount'] += edit_count
        else:
            new_row = {
                'user_name': user_name,
                'prim_lang': lang,
                'prim_ecount': edit_count,
                'tot_ecount': edit_count,
                'n_langs': 1,
                'prim_ym_first_e': year_month_first_edit,
                'prim_lus_first_e': lustrum_first_edit
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    for row in df.itertuples(index=False):
        edfile2.write(
            f"{row.prim_lang}\t{row.prim_ecount}\t{row.tot_ecount}\t{row.n_langs}\t"
            f"{row.prim_ym_first_e}\t{row.prim_lus_first_e}\t{row.user_name}\n"
        )

    query = "DROP TABLE allwiki_editors;"
    cursor.execute(query)
    conn.commit()

    ###
    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        print(languagecode)
        a_file = open(config.databases_path+"temporary_editor_metrics.txt")
        parameters = csv.reader(a_file, delimiter="\t", quotechar='|')

        query = 'UPDATE '+languagecode + \
            'wiki_editors SET (primarylang, primary_ecount, totallangs_ecount, numberlangs, primary_year_month_first_edit, primary_lustrum_first_edit) = (?,?,?,?,?,?) WHERE user_name = ?;'

        cursor.executemany(query, parameters)
        conn.commit()

    logger.info("Calculated cross wiki metrics")

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
