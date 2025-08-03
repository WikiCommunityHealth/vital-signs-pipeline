import pandas as pd
from sqlalchemy import text
import logging

from scripts import config

logger = logging.getLogger(__name__)

def cross_wiki_editor_metrics(wikilanguagecodes, engine):
    all_rows = []

    with engine.begin() as conn:
        # Estrai i dati da tutte le tabelle wiki_editors in un unico DataFrame
        for languagecode in wikilanguagecodes:
            query = f"""
                SELECT '{languagecode}' AS lang, user_name, edit_count, year_month_first_edit, lustrum_first_edit
                FROM {languagecode}wiki_editors
                WHERE user_name != '';
            """
            df = pd.read_sql(query, conn)
            all_rows.append(df)

        df = pd.concat(all_rows, ignore_index=True)
        df["edit_count"] = pd.to_numeric(df["edit_count"], errors="coerce").fillna(0).astype(int)
        df = df[df["user_name"] != ""]
        df_filtered = df[df["lang"] != "meta"]

        totals = df.groupby("user_name")["edit_count"].sum().reset_index()
        totals = totals.rename(columns={"edit_count": "tot_ecount"})

        langs_over_4 = df_filtered[df_filtered["edit_count"] > 4]
        n_langs = langs_over_4.groupby("user_name").size().reset_index(name="n_langs")

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

        # Aggiorna ogni tabella wiki_editors con le nuove metriche
        for languagecode in wikilanguagecodes:
            update_df = result.copy()
            for row in update_df.itertuples(index=False):
                query = text(f"""
                    UPDATE {languagecode}wiki_editors SET
                        primarylang = :prim_lang,
                        primary_ecount = :prim_ecount,
                        totallangs_ecount = :tot_ecount,
                        numberlangs = :n_langs,
                        primary_year_month_first_edit = :prim_ym_first_e,
                        primary_lustrum_first_edit = :prim_lus_first_e
                    WHERE user_name = :user_name
                """)
                conn.execute(query, {
                    "prim_lang": row.prim_lang,
                    "prim_ecount": int(row.prim_ecount),
                    "tot_ecount": int(row.tot_ecount),
                    "n_langs": int(row.n_langs),
                    "prim_ym_first_e": row.prim_ym_first_e,
                    "prim_lus_first_e": row.prim_lus_first_e,
                    "user_name": row.user_name
                })

        logger.info("Calculated cross wiki metrics")
