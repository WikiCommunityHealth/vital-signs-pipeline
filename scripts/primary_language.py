from scripts import config
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)


def cross_wiki_editor_metrics(wikilanguagecodes):
    engine = create_engine(config.db_uri_editors, pool_pre_ping=True)
    unions = []
    for lang in wikilanguagecodes:
        unions.append(f"""
            SELECT
                '{lang}'::text AS lang,
                user_id,
                user_name,
                edit_count,
                year_month_first_edit,
                lustrum_first_edit
            FROM {lang}wiki_editors
            WHERE user_id IS NOT NULL
        """)
    union_sql = "\nUNION ALL\n".join(unions)

    base_sql = f"""
    WITH all_editors AS (
        {union_sql}
    ),
    filtered AS (
        SELECT * FROM all_editors WHERE lang <> 'meta'
    ),
    totals AS (
        SELECT user_id, SUM(edit_count)::int AS tot_ecount
        FROM all_editors
        GROUP BY user_id
    ),
    n_langs AS (
        SELECT user_id,
               GREATEST(1, COUNT(*) FILTER (WHERE edit_count > 4))::int AS n_langs
        FROM filtered
        GROUP BY user_id
    ),
    prim AS (
        SELECT
            user_id,
            -- scegli anche uno user_name "canonico" dalla lingua primaria
            user_name,
            lang        AS prim_lang,
            edit_count::int AS prim_ecount,
            year_month_first_edit AS prim_ym_first_e,
            lustrum_first_edit   AS prim_lus_first_e,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY edit_count DESC, lang ASC, user_name ASC
            ) AS rk
        FROM filtered
    ),
    result AS (
        SELECT
            p.user_id,
            p.user_name,
            p.prim_lang,
            p.prim_ecount,
            t.tot_ecount,
            n.n_langs,
            p.prim_ym_first_e,
            p.prim_lus_first_e
        FROM prim p
        JOIN totals t USING (user_id)
        JOIN n_langs n USING (user_id)
        WHERE p.rk = 1
    )
    """

    with engine.begin() as conn:
        for lang in wikilanguagecodes:
            conn.execute(text(f"""
                {base_sql}
                UPDATE {lang}wiki_editors AS e
                SET primarylang                    = r.prim_lang,
                    primary_ecount                 = r.prim_ecount,
                    totallangs_ecount              = r.tot_ecount,
                    numberlangs                    = r.n_langs,
                    primary_year_month_first_edit  = r.prim_ym_first_e,
                    primary_lustrum_first_edit     = r.prim_lus_first_e, 
                    user_name = COALESCE(e.user_name, r.user_name)
                FROM result r
                WHERE e.user_id = r.user_id;
            """))
