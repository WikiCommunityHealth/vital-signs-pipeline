import sqlite3
import logging
from sqlalchemy import engine, text
from scripts import config


logger = logging.getLogger(__name__)


def create_db(wikilanguagecodes):

    

    engine_editors = engine.create(config.db_uri_editors)
    engine_web = engine.create(config.db_uri_web)

    with engine_editors.connect() as conn:

        # EDITORS DB
        for languagecode in wikilanguagecodes:
            table_name = languagecode+'wiki_editors'
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            except Exception as e:
                logger.warning(f"Failed to drop {table_name}: {e}")
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                user_id INTEGER,
                user_name TEXT PRIMARY KEY,
                bot TEXT,
                user_flags TEXT,
                highest_flag TEXT,
                highest_flag_year_month TEXT,
                gender TEXT,
                primarybinary INTEGER,
                primarylang TEXT,
                edit_count INTEGER,
                primary_ecount INTEGER,
                totallangs_ecount INTEGER,
                primary_year_month_first_edit TEXT,
                primary_lustrum_first_edit TEXT,
                numberlangs INTEGER,
                registration_date TEXT,
                year_month_registration TEXT,
                first_edit_timestamp TEXT,
                year_month_first_edit TEXT,
                year_first_edit TEXT,
                lustrum_first_edit TEXT,
                survived60d TEXT,
                last_edit_timestamp TEXT,
                year_last_edit TEXT,
                lifetime_days INTEGER,
                days_since_last_edit INTEGER
            )
            """
            conn.execute(text(query))

            logger.info(
                f"Created Table {table_name} inside vital signs db")

            table_name2 = languagecode+'wiki_editor_metrics'

            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name2};"))
            except Exception as e:
                logger.warning(f"Failed to drop {table_name2}: {e}")

            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name2} (
                user_id INTEGER,
                user_name TEXT,
                abs_value REAL,
                rel_value REAL,
                metric_name TEXT,
                year_month TEXT,
                timestamp TEXT,
                PRIMARY KEY (user_id, metric_name, year_month, timestamp)
            )
            """
            conn.execute(text(query))

            logger.info(
                f"Created Table {table_name} inside vital signs db")

    with engine_web.connect() as conn2:
        # VITAL SIGNS DB
        table_name = 'vital_signs_metrics'
        try:
            conn2.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        except Exception as e:
            logger.warning(f"Failed to drop {table_name}: {e}")
        query = """
        CREATE TABLE IF NOT EXISTS vital_signs_metrics (
            langcode TEXT,
            year_year_month TEXT,
            year_month TEXT,
            topic TEXT,
            m1 TEXT,
            m1_calculation TEXT,
            m1_value TEXT,
            m2 TEXT,
            m2_calculation TEXT,
            m2_value TEXT,
            m1_count FLOAT,
            m2_count FLOAT,
            PRIMARY KEY (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value)
        )
        """
        conn2.execute(text(query))
        logger.info(
            f"Created Table {table_name} inside vital signs db")
