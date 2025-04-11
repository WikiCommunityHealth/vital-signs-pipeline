from scripts import config

import sqlite3


def compute_wiki_vital_signs(languagecode):

    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()
    conn2 = sqlite3.connect(config.databases_path + config.vital_signs_web_db)
    cursor2 = conn2.cursor()

    query_cm = 'INSERT OR IGNORE INTO vital_signs_metrics (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value, m1_count, m2_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);'

    # VITAL SIGNS DB
    table_name = 'vital_signs_metrics'
    query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (langcode text, year_year_month text, year_month text, topic text, m1 text, m1_calculation text, m1_value text, m2 text, m2_calculation text, m2_value text, m1_count float, m2_count float, PRIMARY KEY (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value))")
    cursor2.execute(query)

    def retention():

       # monthly_registered_first_edit
        parameters = []
        registered_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_registration FROM ' + \
            languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value = row[0]
            year_month = row[1]
            if year_month == '' or year_month == None:
                continue
            try:
                registered_baseline[year_month] = int(value)
            except:
                pass
            parameters.append((languagecode, 'ym', year_month, 'retention',
                              'register', 'threshold', 1, None, None, None, value, None))

        retention_baseline = {}
        query = 'SELECT count(distinct user_id), year_month_first_edit FROM ' + \
            languagecode+'wiki_editors GROUP BY 2 ORDER BY 2 ASC;'
        for row in cursor.execute(query):
            value = row[0]
            year_month = row[1]
            if year_month == '' or year_month == None:
                continue

            try:
                retention_baseline[year_month] = int(value)
            except:
                pass

            parameters.append((languagecode, 'ym', year_month, 'retention',
                              'first_edit', 'threshold', 1, None, None, None, value, None))

            try:
                m1_count = registered_baseline[year_month]
            except:
                m1_count = 0

            parameters.append((languagecode, 'ym', year_month, 'retention', 'register',
                              'threshold', 1, 'first_edit', 'threshold', 1, m1_count, value))

        cursor2.executemany(query_cm, parameters)
        conn2.commit()

        parameters = []
        queries_retention_dict = {}

        # RETENTION
        # number of editors who edited at least once 24h after the first edit
        queries_retention_dict['24h'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode + \
            'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_24h" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 7 days after the first edit
        queries_retention_dict['7d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode + \
            'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_7d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 30 days after the first edit
        queries_retention_dict['30d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode + \
            'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_30d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 60 days after the first edit
        queries_retention_dict['60d'] = 'SELECT count(distinct ch.user_id), ch.year_month_first_edit FROM '+languagecode+'wiki_editors ch INNER JOIN '+languagecode + \
            'wiki_editor_metrics ce ON ch.user_id = ce.user_id WHERE ce.metric_name = "edit_count_60d" AND ce.abs_value > 0 AND ch.bot = "editor" GROUP BY 2 ORDER BY 2 ASC;'

        # number of editors who edited at least once 365 days after the first edit
        queries_retention_dict['365d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM ' + \
            languagecode+'wiki_editors WHERE lifetime_days >= 365 AND bot = "editor" GROUP BY 2 ORDER BY 2;'

        # number of editors who edited at least once 730 days after the first edit
        queries_retention_dict['730d'] = 'SELECT count(distinct user_id), year_month_first_edit FROM ' + \
            languagecode+'wiki_editors WHERE lifetime_days >= 730 AND bot = "editor" GROUP BY 2 ORDER BY 2;'

        for metric_name, query in queries_retention_dict.items():
            for row in cursor.execute(query):
                value = row[0]
                year_month = row[1]
                if year_month == '' or year_month == None:
                    continue

                try:
                    m1_count = retention_baseline[year_month]
                except:
                    m1_count = 0
                parameters.append((languagecode, 'ym', year_month, 'retention', 'first_edit',
                                  'threshold', 1, 'edited_after_time', 'threshold', metric_name, m1_count, value))

                try:
                    m1_count = registered_baseline[year_month]
                except:
                    m1_count = 0
                parameters.append((languagecode, 'ym', year_month, 'retention', 'register',
                                  'threshold', 1, 'edited_after_time', 'threshold', metric_name, m1_count, value))

        cursor2.executemany(query_cm, parameters)
        conn2.commit()

    retention()
    print('retention')

    def stability_balance_special_global_flags_functions():

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
                    query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' GROUP BY e1.year_month ORDER BY e1.year_month;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' GROUP BY 2 ORDER BY 2;'

                for row in cursor.execute(query):
                    # print (row)
                    m1_count = row[0]
                    year_month = row[1]

                    if v == 5:
                        active_editors_5_year_month[year_month] = m1_count
                    if v == 100:
                        active_editors_100_year_month[year_month] = m1_count

                    if year_month == '' or year_month == None:
                        continue

                    parameters.append((languagecode, t, year_month, 'active_editors',
                                      'monthly_edits', 'threshold', v, None, None, None, m1_count, None))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # active_editors    monthly_edits   bin 1, 5, 10, 50, 100, 500, 1000
            parameters = []
            values = [1, 5, 10, 50, 100, 500, 1000, 5000, 10000]
            for x in range(0, len(values)):
                v = values[x]
                if x < len(values)-1:
                    w = values[x+1]

                    if t == 'ym':
                        query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode + \
                            'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND metric_name = "monthly_edits" AND abs_value >= ' + \
                                str(v)+' AND abs_value < '+str(w) + \
                            ' GROUP BY e1.year_month ORDER BY e1.year_month'
                    else:
                        query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode + \
                            'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND metric_name = "monthly_edits" AND abs_value >= ' + \
                                str(v)+' AND abs_value < ' + \
                            str(w)+' GROUP BY 2 ORDER BY 2'

                    w = w - 1
                else:
                    w = 'inf'

                    if t == 'ym':
                        query = 'SELECT count(distinct e1.user_id), e1.year_month FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode + \
                            'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                                str(v)+' GROUP BY e1.year_month ORDER BY e1.year_month;'
                    else:
                        query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics  e1 INNER JOIN '+languagecode + \
                            'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e2.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                                str(v)+' GROUP BY 2 ORDER BY 2;'

                # print (query)
                for row in cursor.execute(query):
                    # print (row)
                    m1_count = row[0]
                    year_month = row[1]
                    if year_month == '':
                        continue
                    parameters.append((languagecode, t, year_month, 'active_editors', 'monthly_edits', 'bin', str(
                        v)+'_'+str(w), None, None, None, m1_count, None))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

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
                        query = 'SELECT count(distinct e2.user_id), e2.year_month FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id INNER JOIN '+languagecode + \
                            'wiki_editors e3 ON e1.user_id = e3.user_id WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                                str(v)+' AND e2.metric_name = "active_months_row" AND e2.abs_value BETWEEN '+str(
                                    interval[0])+' AND '+str(interval[1])+' AND e1.year_month = e2.year_month GROUP by e2.year_month;'
                    else:
                        query = 'SELECT count(distinct e2.user_id), substr(e2.year_month, 1, 4) FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode+'wiki_editor_metrics e2 ON e1.user_id = e2.user_id  INNER JOIN '+languagecode + \
                            'wiki_editors e3 ON e1.user_id = e3.user_id WHERE e3.bot = "editor" AND e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                                str(v)+' AND e2.metric_name = "active_months_row" AND e2.abs_value BETWEEN '+str(
                                    interval[0])+' AND '+str(interval[1])+' AND e1.year_month = e2.year_month GROUP by 2;'

                    for row in cursor.execute(query):

                        m2_count = row[0]
                        year_month = row[1]
                        m2_value = label

                        if year_month == '' or year_month == None:
                            continue

                        if v == 5:
                            try:
                                parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', v,
                                                  "active_months_row", 'bin', m2_value, active_editors_5_year_month[year_month], m2_count))
                            except:
                                continue

                            try:
                                stability_active_editors_5[year_month] = stability_active_editors_5[year_month]+m2_count
                            except:
                                stability_active_editors_5[year_month] = m2_count

                        if v == 100:
                            try:
                                parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', v,
                                                  "active_months_row", 'bin', m2_value, active_editors_100_year_month[year_month], m2_count))
                            except:
                                continue

                            try:
                                stability_active_editors_100[year_month] = stability_active_editors_100[year_month]+m2_count
                            except:
                                stability_active_editors_100[year_month] = m2_count

            for year_month, value in stability_active_editors_5.items():
                parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', 5, "active_months_row",
                                  'bin', '1', active_editors_5_year_month[year_month], active_editors_5_year_month[year_month] - value))

            for year_month, value in stability_active_editors_100.items():
                parameters.append((languagecode, t, year_month, 'stability', 'monthly_edits', 'threshold', 100, "active_months_row",
                                  'bin', '1', active_editors_100_year_month[year_month], active_editors_100_year_month[year_month] - value))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # BALANCE

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'

                for row in cursor.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append((languagecode, t, year_month, 'balance', 'monthly_edits', 'threshold', v,
                                          'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100:
                        parameters.append((languagecode, t, year_month, 'balance', 'monthly_edits', 'threshold', v,
                                          'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # SPECIAL FUNCTIONS
            # TECHNICAL EDITORS

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_technical" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_technical" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'

                for row in cursor.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append((languagecode, t, year_month, 'technical_editors', 'monthly_edits_technical', 'threshold',
                                          v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100:
                        parameters.append((languagecode, t, year_month, 'technical_editors', 'monthly_edits_technical', 'threshold',
                                          v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # COORDINATORS

            values = [5, 100]
            parameters = []
            for v in values:

                # active_editors    monthly_edits   threshold   5   lustrum_first_edit  bin 2001, 2006, 2011, 2016, 2021

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_coordination" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.lustrum_first_edit;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits_coordination" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.lustrum_first_edit IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3;'

                for row in cursor.execute(query):
                    # print (row)
                    m2_count = row[0]
                    year_month = row[1]
                    lustrum_first_edit = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append((languagecode, t, year_month, 'coordinators', 'monthly_edits_coordination', 'threshold',
                                          v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_5_year_month[year_month], m2_count))
                    if v == 100:
                        parameters.append((languagecode, t, year_month, 'coordinators', 'monthly_edits_coordination', 'threshold',
                                          v, 'lustrum_first_edit', 'bin', lustrum_first_edit, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # GLOBAL / PRIMARY

            values = [5, 100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.primarylang FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.bot = "editor" GROUP BY e1.year_month, e2.primarylang;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.primarylang FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2  on e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.bot = "editor" GROUP BY 2, 3'

                for row in cursor.execute(query):

                    m2_count = row[0]
                    year_month = row[1]
                    primarylang = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append((languagecode, t, year_month, 'primary_editors', 'monthly_edits', 'threshold',
                                          v, 'primarylang', 'bin', primarylang, active_editors_5_year_month[year_month], m2_count))
                    if v == 100:
                        parameters.append((languagecode, t, year_month, 'primary_editors', 'monthly_edits', 'threshold',
                                          v, 'primarylang', 'bin', primarylang, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

            # FLAGS AMONG ACTIVE EDITORS

            # active_editors    monthly_edits   threshold   5   flag    name    sysop, autopatrolled, bureaucrat, etc.
            values = [5, 100]
            parameters = []
            for v in values:

                if t == 'ym':
                    query = 'SELECT count(distinct e1.user_id), e1.year_month, e2.highest_flag FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" GROUP BY e1.year_month, e2.highest_flag;'
                else:
                    query = 'SELECT count(distinct e1.user_id), substr(e1.year_month, 1, 4), e2.highest_flag FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN '+languagecode + \
                        'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "monthly_edits" AND e1.abs_value >= ' + \
                            str(v)+' AND e2.highest_flag IS NOT NULL AND e2.bot = "editor" GROUP BY 2, 3'

                for row in cursor.execute(query):
                    m2_count = row[0]
                    year_month = row[1]
                    m2_value = row[2]

                    if year_month == '' or year_month == None:
                        continue

                    if v == 5:
                        parameters.append((languagecode, t, year_month, 'flags', 'monthly_edits', 'threshold', 5,
                                          'highest_flag', 'name', m2_value, active_editors_5_year_month[year_month], m2_count))
                    if v == 100:
                        parameters.append((languagecode, t, year_month, 'flags', 'monthly_edits', 'threshold', 5,
                                          'highest_flag', 'name', m2_value, active_editors_100_year_month[year_month], m2_count))

            cursor2.executemany(query_cm, parameters)
            conn2.commit()

    stability_balance_special_global_flags_functions()
    print('stability_balance_special_global_flags_functions')

    def administrators():

        parameters = []
        for metric_name in ['granted_flag', 'removed_flag', 'highest_flag']:

            if metric_name == 'highest_flag':
                query = 'SELECT count(distinct e1.user_id), e1.highest_flag, substr(e1.year_month_first_edit, 1, 4), e1.lustrum_first_edit FROM ' + \
                    languagecode+'wiki_editors e1 WHERE e1.highest_flag IS NOT NULL AND e1.bot = "editor" GROUP BY 2, 3 ORDER BY 2, 3;'

            else:
                query = 'SELECT count(distinct e1.user_id), e1.abs_value, substr(e1.year_month, 1, 4), e2.lustrum_first_edit FROM '+languagecode+'wiki_editor_metrics e1 INNER JOIN ' + \
                    languagecode+'wiki_editors e2 ON e1.user_id = e2.user_id WHERE e1.metric_name = "' + \
                        metric_name+'" AND e2.bot = "editor" GROUP BY 2, 3, 4;'

            for row in cursor.execute(query):

                m2_count = row[0]
                m1_value = row[1]
                year_month = row[2]
                m2_value = row[3]

                parameters.append((languagecode, 'y', year_month, 'flags', metric_name,
                                  'name', m1_value, 'lustrum_first_edit', 'bin', m2_value, None, m2_count))

        cursor2.executemany(query_cm, parameters)
        conn2.commit()

    administrators()
    print('administrators')
