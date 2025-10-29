import logging
from sqlalchemy import create_engine, text
from scripts import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EDITORS_FINAL = "enwiki_editors"
METRICS_FINAL = "enwiki_editor_metrics"

def join_tables(paths: list):
    engine = create_engine(config.db_uri_editors, pool_pre_ping=True)

    with engine.begin() as conn:
        for p in paths:
            editors_stg = f"{EDITORS_FINAL}_{p}"
            metrics_stg = f"{METRICS_FINAL}_{p}"

            logger.info("Merging %s -> %s", editors_stg, EDITORS_FINAL)
            conn.execute(text(f"""
                INSERT INTO {EDITORS_FINAL} AS tgt
                SELECT * FROM {editors_stg}
                ON CONFLICT (user_id) DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    registration_date = EXCLUDED.registration_date,
                    year_month_registration = EXCLUDED.year_month_registration,
                    first_edit_timestamp = EXCLUDED.first_edit_timestamp,
                    year_month_first_edit = EXCLUDED.year_month_first_edit,
                    year_first_edit = EXCLUDED.year_first_edit,
                    lustrum_first_edit = EXCLUDED.lustrum_first_edit,
                    bot = EXCLUDED.bot,
                    user_flags = EXCLUDED.user_flags,
                    last_edit_timestamp = EXCLUDED.last_edit_timestamp,
                    year_last_edit = EXCLUDED.year_last_edit,
                    lifetime_days = EXCLUDED.lifetime_days,
                    days_since_last_edit = EXCLUDED.days_since_last_edit,
                    survived60d = EXCLUDED.survived60d,
                    edit_count = EXCLUDED.edit_count
            """))

            logger.info("Merging %s -> %s", metrics_stg, METRICS_FINAL)
            conn.execute(text(f"""
                INSERT INTO {METRICS_FINAL}
                SELECT * FROM {metrics_stg}
                ON CONFLICT DO NOTHING
            """))

            logger.info("Dropping %s", editors_stg)
            conn.execute(text(f'DROP TABLE IF EXISTS {editors_stg}'))
            logger.info("Dropping %s", metrics_stg)
            conn.execute(text(f'DROP TABLE IF EXISTS {metrics_stg}'))

