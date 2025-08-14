from scripts import config
from sqlalchemy import create_engine, text
import logging


def compute_wiki_vital_signs(languagecode):

    logger = logging.getLogger(languagecode + '' + __name__)

    # Connessione ai database con SQLAlchemy
    engine_editors = create_engine(config.db_uri_editors)
    engine_web = create_engine(config.db_uri_web)

    # Query di inserimento (Postgres-style)
    query_cm = text("""
        INSERT INTO vital_signs_metrics 
        (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value, m1_count, m2_count)
        VALUES (:langcode, :year_year_month, :year_month, :topic, :m1, :m1_calculation, :m1_value, :m2, :m2_calculation, :m2_value, :m1_count, :m2_count)
        ON CONFLICT DO NOTHING
    """)

    # Creazione tabella se non esiste
    table_name = 'vital_signs_metrics'
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            PRIMARY KEY (
                langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, 
                m2, m2_calculation, m2_value
            )
        )
    """
    # Esegui la query per creare la tabella
    with engine_web.begin() as conn:
        conn.execute(text(create_table_query))

    def retention(conn_editors, conn_web):
        # MONTHLY REGISTERED & FIRST EDIT BASELINES
        parameters = []
        registered_baseline = {}

        query = text(f'''
            SELECT count(distinct user_id), year_month_registration
            FROM {languagecode}wiki_editors
            GROUP BY year_month_registration ORDER BY year_month_registration ASC
        ''')
        result = conn_editors.execute(query)
        for value, year_month in result:
            logger.info(f'RETENTIONN: ({value}, {year_month})')
            if not year_month:
                continue
            try:
                registered_baseline[year_month] = int(value)
            except Exception:
                pass
            parameters.append(dict(
                langcode=languagecode, year_year_month='ym', year_month=year_month,
                topic='retention', m1='register', m1_calculation='threshold', m1_value=1,
                m2='', m2_calculation='', m2_value='', m1_count=value, m2_count=0
            ))

        retention_baseline = {}
        query = text(f'''
            SELECT count(distinct user_id), year_month_first_edit
            FROM {languagecode}wiki_editors
            GROUP BY year_month_first_edit ORDER BY year_month_first_edit ASC
        ''')
        result = conn_editors.execute(query)
        for value, year_month in result:
            if not year_month:
                continue
            try:
                retention_baseline[year_month] = int(value)
            except Exception:
                pass
            parameters.append(dict(
                langcode=languagecode, year_year_month='ym', year_month=year_month,
                topic='retention', m1='first_edit', m1_calculation='threshold', m1_value=1,
                m2='', m2_calculation='', m2_value='', m1_count=value, m2_count=0
            ))
            m1_count = registered_baseline.get(year_month, 0)
            parameters.append(dict(
                langcode=languagecode, year_year_month='ym', year_month=year_month,
                topic='retention', m1='register', m1_calculation='threshold', m1_value=1,
                m2='first_edit', m2_calculation='threshold', m2_value=1, m1_count=m1_count, m2_count=value
            ))

        # INSERT BASELINES
        conn_web.execute(query_cm, parameters)

        # RETENTION METRICS (AFTER FIRST EDIT)
        parameters = []
        queries_retention_dict = {
            '24h': text(f'''
            SELECT count(distinct ch.user_id), ch.year_month_first_edit
            FROM {languagecode}wiki_editors ch
            INNER JOIN {languagecode}wiki_editor_metrics ce
            ON ch.user_id = ce.user_id
            WHERE ce.metric_name = 'edit_count_24h' AND CAST(ce.abs_value AS REAL) > 0 AND ch.bot = 'editor'
            GROUP BY ch.year_month_first_edit ORDER BY ch.year_month_first_edit ASC
            '''),
            '7d': text(f'''
            SELECT count(distinct ch.user_id), ch.year_month_first_edit
            FROM {languagecode}wiki_editors ch
            INNER JOIN {languagecode}wiki_editor_metrics ce
            ON ch.user_id = ce.user_id
            WHERE ce.metric_name = 'edit_count_7d' AND CAST(ce.abs_value AS REAL) > 0 AND ch.bot = 'editor'
            GROUP BY ch.year_month_first_edit ORDER BY ch.year_month_first_edit ASC
            '''),
            '30d': text(f'''
            SELECT count(distinct ch.user_id), ch.year_month_first_edit
            FROM {languagecode}wiki_editors ch
            INNER JOIN {languagecode}wiki_editor_metrics ce
            ON ch.user_id = ce.user_id
            WHERE ce.metric_name = 'edit_count_30d' AND CAST(ce.abs_value AS REAL) > 0 AND ch.bot = 'editor'
            GROUP BY ch.year_month_first_edit ORDER BY ch.year_month_first_edit ASC
            '''),
            '60d': text(f'''
            SELECT count(distinct ch.user_id), ch.year_month_first_edit
            FROM {languagecode}wiki_editors ch
            INNER JOIN {languagecode}wiki_editor_metrics ce
            ON ch.user_id = ce.user_id
            WHERE ce.metric_name = 'edit_count_60d' AND CAST(ce.abs_value AS REAL) > 0 AND ch.bot = 'editor'
            GROUP BY ch.year_month_first_edit ORDER BY ch.year_month_first_edit ASC
            '''),
            '365d': text(f'''
            SELECT count(distinct user_id), year_month_first_edit
            FROM {languagecode}wiki_editors
            WHERE lifetime_days >= 365 AND bot = 'editor'
            GROUP BY year_month_first_edit ORDER BY year_month_first_edit
            '''),
            '730d': text(f'''
            SELECT count(distinct user_id), year_month_first_edit
            FROM {languagecode}wiki_editors
            WHERE lifetime_days >= 730 AND bot = 'editor'
            GROUP BY year_month_first_edit ORDER BY year_month_first_edit
            ''')
        }

        for metric_name, query in queries_retention_dict.items():
            for value, year_month in conn_editors.execute(query):
                if not year_month:
                    continue
                m1_count = retention_baseline.get(year_month, 0)
                parameters.append(dict(
                    langcode=languagecode, year_year_month='ym', year_month=year_month,
                    topic='retention', m1='first_edit', m1_calculation='threshold', m1_value=1,
                    m2='edited_after_time', m2_calculation='threshold', m2_value=metric_name,
                    m1_count=m1_count, m2_count=value
                ))
                m1_count = registered_baseline.get(year_month, 0)
                parameters.append(dict(
                    langcode=languagecode, year_year_month='ym', year_month=year_month,
                    topic='retention', m1='register', m1_calculation='threshold', m1_value=1,
                    m2='edited_after_time', m2_calculation='threshold', m2_value=metric_name,
                    m1_count=m1_count, m2_count=value
                ))

        conn_web.execute(query_cm, parameters)

    with engine_editors.connect() as conn_editors, engine_web.begin() as conn_web:
        retention(conn_editors, conn_web)
        logger.info("Calculated editors retention")

    def stability_balance_special_global_flags_functions(conn_editors, conn_web):

        # year month or year
        for t in ['ym', 'y']:

            # ACTIVE EDITORS
            # active_editors    monthly_edits   threshold   5, 100
            active_editors_5_year_month = {}
            active_editors_100_year_month = {}

            values = [5, 100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST (e1.abs_value AS REAL) >= {v}
                        GROUP BY e1.year_month ORDER BY e1.year_month
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST (e1.abs_value AS REAL) >= {v}
                        GROUP BY 2 ORDER BY 2
                    ''')

                for row in conn_editors.execute(query):
                    # print (row)
                    m1_count = row[0]
                    year_month = row[1]

                    if v == 5:
                        active_editors_5_year_month[year_month] = m1_count
                    if v == 100:
                        active_editors_100_year_month[year_month] = m1_count

                    if year_month == '' or year_month == None:
                        continue

                    parameters.append(dict(
                        langcode=languagecode, year_year_month=t, year_month=year_month,
                        topic='active_editors', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                        m2='', m2_calculation='', m2_value='', m1_count=m1_count, m2_count=0
                    ))

            conn_web.execute(query_cm, parameters)

            # active_editors    monthly_edits   bin 1, 5, 10, 50, 100, 500, 1000
            parameters = []
            values = [1, 5, 10, 50, 100, 500, 1000, 5000, 10000]
            for x in range(0, len(values)):
                v = values[x]
                if x < len(values)-1:
                    w = values[x+1]

                    if t == 'ym':
                        query = text(f'''
                            SELECT count(distinct e1.user_id), e1.year_month 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                            WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" 
                            AND CAST(e1.abs_value AS REAL) >= {v} AND CAST(e1.abs_value AS REAL) < {w}
                            GROUP BY e1.year_month ORDER BY e1.year_month
                        ''')
                    else:
                        query = text(f'''
                            SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                            WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" 
                            AND CAST(e1.abs_value AS REAL) >= {v} AND CAST(e1.abs_value AS REAL) < {w}
                            GROUP BY 2 ORDER BY 2
                        ''')

                    w = w - 1
                else:
                    w = 'inf'

                    if t == 'ym':
                        query = text(f'''
                            SELECT count(distinct e1.user_id), e1.year_month 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                            WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                            GROUP BY e1.year_month ORDER BY e1.year_month
                        ''')
                    else:
                        query = text(f'''
                            SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                            WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                            GROUP BY 2 ORDER BY 2
                        ''')

                # print (query)
                for row in conn_editors.execute(query):
                    # print (row)
                    m1_count = row[0]
                    year_month = row[1]
                    if year_month == '':
                        continue
                    parameters.append(dict(
                        langcode=languagecode, year_year_month=t, year_month=year_month,
                        topic='active_editors', m1='monthly_edits', m1_calculation='bin', m1_value=str(v)+'_'+str(w),
                        m2='', m2_calculation='', m2_value='', m1_count=m1_count, m2_count=0
                    ))

            conn_web.execute(query_cm, parameters)

            # STABILITY
            # active_editors  monthly_edits   threshold   5   active_months   bin 1-10, 10-20, 30-40,... to 150
            values = [5, 100]
            parameters = []

            stability_active_editors_5 = {}
            stability_active_editors_100 = {}

            for v in values:

                active_months_row = {
                    (2, 2): '2', (3, 6): '3-6', (7, 12): '7-12', (13, 24): '13-24', (25, 5000): '+24'}

                for interval, label in active_months_row.items():

                    if t == 'ym':
                        query = text(f'''
                            SELECT count(distinct e2.user_id), e2.year_month 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editor_metrics e2 ON e1.user_id = e2.user_id 
                            INNER JOIN {languagecode}wiki_editors e3 ON e1.user_id = e3.user_id 
                            WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                            AND e2.metric_name = "active_months_row" AND CAST(e2.abs_value AS REAL) BETWEEN {interval[0]} AND {interval[1]} 
                            AND e1.year_month = e2.year_month 
                            GROUP BY e2.year_month
                        ''')
                    else:
                        query = text(f'''
                            SELECT count(distinct e2.user_id), substr(e2.year_month, 1, 4) 
                            FROM {languagecode}wiki_editor_metrics e1 
                            INNER JOIN {languagecode}wiki_editor_metrics e2 ON e1.user_id = e2.user_id  
                            INNER JOIN {languagecode}wiki_editors e3 ON e1.user_id = e3.user_id 
                            WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                            AND e2.metric_name = "active_months_row" AND CAST(e2.abs_value AS REAL) BETWEEN {interval[0]} AND {interval[1]} 
                            AND e1.year_month = e2.year_month 
                            GROUP BY 2
                        ''')

                    for row in conn_editors.execute(query):

                        m2_count = row[0]
                        year_month = row[1]
                        m2_value = label

                        if year_month == '' or year_month == None:
                            continue

                        if v == 5:
                            try:
                                parameters.append(dict(
                                    langcode=languagecode, year_year_month=t, year_month=year_month,
                                    topic='stability', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                                    m2='active_months_row', m2_calculation='bin', m2_value=m2_value,
                                    m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                                ))
                            except:
                                continue

                            try:
                                stability_active_editors_5[year_month] = stability_active_editors_5[year_month]+m2_count
                            except:
                                stability_active_editors_5[year_month] = m2_count

                        if v == 100:
                            try:
                                parameters.append(dict(
                                    langcode=languagecode, year_year_month=t, year_month=year_month,
                                    topic='stability', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                                    m2='active_months_row', m2_calculation='bin', m2_value=m2_value,
                                    m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                                ))
                            except:
                                continue

                            try:
                                stability_active_editors_100[year_month] = stability_active_editors_100[year_month]+m2_count
                            except:
                                stability_active_editors_100[year_month] = m2_count

            for year_month, value in stability_active_editors_5.items():
                parameters.append(dict(
                    langcode=languagecode, year_year_month=t, year_month=year_month,
                    topic='stability', m1='monthly_edits', m1_calculation='threshold', m1_value=5,
                    m2='active_months_row', m2_calculation='bin', m2_value='1',
                    m1_count=active_editors_5_year_month[year_month], m2_count=active_editors_5_year_month[year_month] - value
                ))

            for year_month, value in stability_active_editors_100.items():
                parameters.append(dict(
                    langcode=languagecode, year_year_month=t, year_month=year_month,
                    topic='stability', m1='monthly_edits', m1_calculation='threshold', m1_value=100,
                    m2='active_months_row', m2_calculation='bin', m2_value='1',
                    m1_count=active_editors_100_year_month[
                        year_month], m2_count=active_editors_100_year_month[year_month] - value
                ))

            conn_web.execute(query_cm, parameters)

            logger.info("Calculated editors stability")

            # BALANCE

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021
                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY e1.year_month, e2.lustrum_first_edit
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY 2, 3
                    ''')

                for row in conn_editors.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='balance', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                        ))
                    if v == 100:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='balance', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                        ))

            conn_web.execute(query_cm, parameters)
            logger.info("Calculated editors stability")

            # SPECIAL FUNCTIONS
            # TECHNICAL EDITORS

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits_technical" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY e1.year_month, e2.lustrum_first_edit
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits_technical" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY 2, 3
                    ''')

                for row in conn_editors.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='technical_editors', m1='monthly_edits_technical', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                        ))
                    if v == 100:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='technical_editors', m1='monthly_edits_technical', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                        ))

            conn_web.execute(query_cm, parameters)

            logger.info("Calculated technical editors metrics")
            # COORDINATORS

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021
                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits_coordination" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY e1.year_month, e2.lustrum_first_edit
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits_coordination" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY 2, 3
                    ''')

                for row in conn_editors.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='coordinators', m1='monthly_edits_coordination', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                        ))
                    if v == 100:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='coordinators', m1='monthly_edits_coordination', m1_calculation='threshold', m1_value=v,
                            m2='lustrum_first_edit', m2_calculation='bin', m2_value=lustrum_first_edit,
                            m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                        ))

            conn_web.execute(query_cm, parameters)

            logger.info("Calculated coordinators metrics")

            # GLOBAL / PRIMARY

            values = [5, 100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month, e2.primarylang 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.bot = "editor" 
                        GROUP BY e1.year_month, e2.primarylang
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.primarylang 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.bot = "editor" 
                        GROUP BY 2, 3
                    ''')

                for row in conn_editors.execute(query):

                    m2_count = row[0]
                    year_month = row[1]
                    primarylang = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='primary_editors', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                            m2='primarylang', m2_calculation='bin', m2_value=primarylang,
                            m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                        ))
                    if v == 100:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='primary_editors', m1='monthly_edits', m1_calculation='threshold', m1_value=v,
                            m2='primarylang', m2_calculation='bin', m2_value=primarylang,
                            m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                        ))

            conn_web.execute(query_cm, parameters)
            logger.info("Calculated global metrics")

            # FLAGS AMONG ACTIVE EDITORS

            # active_editors    monthly_edits   threshold   5   flag    name    sysop, autopatrolled, bureaucrat, etc.
            values = [5, 100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = text(f'''
                        SELECT count(distinct e1.user_id), e1.year_month, e2.highest_flag 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY e1.year_month, e2.highest_flag
                    ''')
                else:
                    query = text(f'''
                        SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.highest_flag 
                        FROM {languagecode}wiki_editor_metrics e1 
                        INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                        WHERE e1.metric_name = "monthly_edits" AND CAST(e1.abs_value AS REAL) >= {v}
                        AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" 
                        GROUP BY 2, 3
                    ''')

                for row in conn_editors.execute(query):
                    m2_count = row[0]
                    year_month = row[1]
                    m2_value = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='flags', m1='monthly_edits', m1_calculation='threshold', m1_value=5,
                            m2='highest_flag', m2_calculation='name', m2_value=m2_value,
                            m1_count=active_editors_5_year_month[year_month], m2_count=m2_count
                        ))
                    if v == 100:
                        parameters.append(dict(
                            langcode=languagecode, year_year_month=t, year_month=year_month,
                            topic='flags', m1='monthly_edits', m1_calculation='threshold', m1_value=100,
                            m2='highest_flag', m2_calculation='name', m2_value=m2_value,
                            m1_count=active_editors_100_year_month[year_month], m2_count=m2_count
                        ))

            conn_web.execute(query_cm, parameters)
            logger.info("Calculated flags among active editors")

    with engine_editors.connect() as conn_editors, engine_web.begin() as conn_web:
        stability_balance_special_global_flags_functions(
            conn_editors, conn_web)

    def administrators(conn_editors, conn_web):

        parameters = []
        for metric_name in ['granted_flag', 'removed_flag', 'highest_flag']:

            if metric_name == 'highest_flag':
                query = text(f'''
                    SELECT count(distinct e1.user_id), e1.highest_flag, substr(e1.year_month_first_edit, 1, 4), e1.lustrum_first_edit 
                    FROM {languagecode}wiki_editors e1 
                    WHERE e1.highest_flag IS NOT NULL AND e1.bot = "editor" 
                    GROUP BY 2, 3 ORDER BY 2, 3
                ''')

            else:
                query = text(f'''
                    SELECT count(distinct e1.user_id), e1.abs_value, substr(e1.year_month, 1, 4), e2.lustrum_first_edit 
                    FROM {languagecode}wiki_editor_metrics e1 
                    INNER JOIN {languagecode}wiki_editors e2 ON e1.user_id = e2.user_id 
                    WHERE e1.metric_name = "{metric_name}" AND e2.bot = "editor" 
                    GROUP BY 2, 3, 4
                ''')

            for row in conn_editors.execute(query):

                m2_count = row[0]
                m1_value = row[1]
                year_month = row[2]
                m2_value = row[3]

                parameters.append(dict(
                    langcode=languagecode, year_year_month='y', year_month=year_month,
                    topic='flags', m1=metric_name, m1_calculation='name', m1_value=m1_value,
                    m2='lustrum_first_edit', m2_calculation='bin', m2_value=m2_value,
                    m1_count=0, m2_count=m2_count
                ))

        conn_web.execute(query_cm, parameters)

    with engine_editors.connect() as conn_editors, engine_web.begin() as conn_web:
        administrators(conn_editors, conn_web)
        logger.info("Calculated admin metrics")
