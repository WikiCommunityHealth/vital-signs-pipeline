import pandas as pd
import sqlite3
import csv
import os

from scripts import config
import logging

logger = logging.getLogger(__name__)


def cross_wiki_editor_metrics(wikilanguagecodes):
    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    # create temporary table
    try:
        cursor.execute("DROP TABLE allwiki_editors;")
    except:
        pass

    cursor.execute("""
        CREATE TABLE allwiki_editors (
            lang TEXT,
            user_name TEXT,
            edit_count INTEGER,
            year_month_first_edit TEXT,
            lustrum_first_edit TEXT
        );
    """)

    # fill the tmp table with the data from the xwiki_editors tables
    for languagecode in wikilanguagecodes:
        query = f'''
            INSERT INTO allwiki_editors
            SELECT "{languagecode}", user_name, edit_count, year_month_first_edit, lustrum_first_edit
            FROM {languagecode}wiki_editors
            WHERE user_name != "";
        '''
        cursor.execute(query)
        conn.commit()

    # create a dataframe with all the data from the tmp table
    df = pd.read_sql_query("SELECT * FROM allwiki_editors", conn)
    # conversion from str to int
    df["edit_count"] = pd.to_numeric(
        df["edit_count"], errors="coerce").fillna(0).astype(int)

    # remove rows without username and the data from metawiki_editors (not a language)
    df = df[df["user_name"] != ""]
    df_filtered = df[df["lang"] != "meta"]

    # calculate the total number of edits for every user
    totals = df.groupby("user_name")["edit_count"].sum().reset_index()
    totals = totals.rename(columns={"edit_count": "tot_ecount"})

    # calculate the number of languages per user
    langs_over_4 = df_filtered[df_filtered["edit_count"] > 4]
    n_langs = langs_over_4.groupby(
        "user_name").size().reset_index(name="n_langs")

    # find the primary language
    idx = df_filtered.groupby("user_name")["edit_count"].idxmax()
    prim_lang_df = df_filtered.loc[idx, [
        "user_name", "lang", "edit_count", "year_month_first_edit", "lustrum_first_edit"]]
    prim_lang_df = prim_lang_df.rename(columns={
        "lang": "prim_lang",
        "edit_count": "prim_ecount",
        "year_month_first_edit": "prim_ym_first_e",
        "lustrum_first_edit": "prim_lus_first_e"
    })

    result = prim_lang_df.merge(totals, on="user_name").merge(
        n_langs, on="user_name", how="left")
    result["n_langs"] = result["n_langs"].fillna(1).astype(int)

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass

    edfile = open(config.databases_path + 'temporary_editor_metrics.txt', "w")
    for row in result.itertuples(index=False):
        edfile.write(
            f"{row.prim_lang}\t{row.prim_ecount}\t{row.tot_ecount}\t{row.n_langs}\t"
            f"{row.prim_ym_first_e}\t{row.prim_lus_first_e}\t{row.user_name}\n"
        )
    edfile.close()

    cursor.execute("DROP TABLE allwiki_editors;")
    conn.commit()

    # update xwiki_editors 
    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        with open(config.databases_path + "temporary_editor_metrics.txt") as a_file:
            parameters = csv.reader(a_file, delimiter="\t", quotechar='|')

            query = f'''
                UPDATE {languagecode}wiki_editors
                SET (
                    primarylang,
                    primary_ecount,
                    totallangs_ecount,
                    numberlangs,
                    primary_year_month_first_edit,
                    primary_lustrum_first_edit
                ) = (?, ?, ?, ?, ?, ?)
                WHERE user_name = ?;
            '''
            cursor.executemany(query, parameters)
            conn.commit()

    logger.info("Calculated cross wiki metrics")

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
