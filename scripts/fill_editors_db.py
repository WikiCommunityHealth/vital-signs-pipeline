from scripts import utils
from scripts import config

import sqlite3
import datetime
import bz2
import calendar
import csv
import os

from dateutil import relativedelta

import logging


def process_editor_metrics_from_dump(languagecode):
    logger = logging.getLogger(languagecode + '' + __name__)

    d_paths, cym = utils.get_mediawiki_paths(languagecode)
    cym_timestamp_dt = datetime.datetime.today().replace(
        day=1)

    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()

    # added
    editor_edit_count = {}

    user_id_user_name_dict = {}
    user_id_bot_dict = {}
    user_id_user_groups_dict = {}

    editor_first_edit_timestamp = {}
    editor_registration_date = {}

    editor_last_edit_timestamp = {}

    editor_user_group_dict = {}
    editor_user_group_dict_timestamp = {}

    # for the survival part
    survived_dict = {}
    survival_measures = []
    user_id_edit_count = {}

    # for the monthly part
    editor_monthly_namespace_technical = {}  # 8, 10
    editor_monthly_namespace_coordination = {}  # 4, 12

    editor_monthly_edits = {}

    last_year_month = 0
    first_date = datetime.datetime.strptime(
        '2001-01-01 01:15:15', '%Y-%m-%d %H:%M:%S')

    for dump_path in d_paths:

        dump_in = bz2.open(dump_path, 'r')
        line = 'something'
        line = dump_in.readline()

        while line != '':

            line = dump_in.readline()
            line = line.rstrip().decode('utf-8')[:-1]
            values = line.split('\t')
            if len(values) == 1:
                continue

            event_entity = values[1]
            event_type = values[2]

            event_user_id = values[5]
            try:
                int(event_user_id)
            except:
                continue

            event_user_text = values[7]
            if event_user_text != '':
                user_id_user_name_dict[event_user_id] = event_user_text
            else:
                continue

            try:
                editor_last_edit = editor_last_edit_timestamp[event_user_id]
                last_edit_date_dt = datetime.datetime.strptime(
                    editor_last_edit[:len(editor_last_edit)-2], '%Y-%m-%d %H:%M:%S')
                last_edit_year_month_day = datetime.datetime.strptime(
                    last_edit_date_dt.strftime('%Y-%m-%d'), '%Y-%m-%d')
            except:
                last_edit_year_month_day = ''

            event_timestamp = values[3]
            event_timestamp_dt = datetime.datetime.strptime(
                event_timestamp[:len(event_timestamp)-2], '%Y-%m-%d %H:%M:%S')
            editor_last_edit_timestamp[event_user_id] = event_timestamp

            editor_edit_count[event_user_id] = values[21]

            event_user_groups = values[11]
            if event_user_groups != '':
                user_id_user_groups_dict[event_user_id] = event_user_groups

            page_namespace = values[28]

            if event_entity == 'user':

                user_text = str(values[38])  # this is target of the event

                if event_type == 'altergroups':

                    user_id = values[36]
                    cur_ug = values[41]

                    user_text = values[38]
                    if user_text != '':
                        user_id_user_name_dict[user_id] = user_text

                    if cur_ug != '' and cur_ug != None:

                        try:
                            old_ug = editor_user_group_dict[user_id]
                            if ',' in old_ug:
                                old_ug_list = old_ug.split(',')
                            else:
                                old_ug_list = [old_ug]

                        except:
                            old_ug_list = []

                        if ',' in cur_ug:
                            cur_ug_list = cur_ug.split(',')
                        else:
                            cur_ug_list = [cur_ug]

                        i = 0
                        for x in cur_ug_list:
                            if x not in old_ug_list:
                                event_ts = event_timestamp[:len(
                                    event_timestamp)-i]
                                editor_user_group_dict_timestamp[user_id, event_ts] = [
                                    'granted_flag', x]
                                i += 1

                        for x in old_ug_list:
                            if x not in cur_ug_list:
                                event_ts = event_timestamp[:len(
                                    event_timestamp)-i]
                                editor_user_group_dict_timestamp[user_id, event_ts] = [
                                    'removed_flag', x]
                                i += 1

                        editor_user_group_dict[user_id] = cur_ug

            event_is_bot_by = values[13]
            if event_is_bot_by != '':
                user_id_bot_dict[event_user_id] = event_is_bot_by

            event_user_is_anonymous = values[17]
            if event_user_is_anonymous == True or event_user_id == '':
                continue

            event_user_registration_date = values[20]
            event_user_creation_date = values[21]
            if event_user_id not in editor_registration_date:
                if event_user_registration_date != '':
                    editor_registration_date[event_user_id] = event_user_registration_date
                elif event_user_creation_date != '':
                    editor_registration_date[event_user_id] = event_user_creation_date

            # ---------

            # MONTHLY EDITS COUNTER
            try:
                editor_monthly_edits[event_user_id] = editor_monthly_edits[event_user_id]+1
            except:
                editor_monthly_edits[event_user_id] = 1

            # MONTHLY NAMESPACES EDIT COUNTER
            if page_namespace == '4' or page_namespace == '12':
                try:
                    editor_monthly_namespace_coordination[
                        event_user_id] = editor_monthly_namespace_coordination[event_user_id]+1
                except:
                    editor_monthly_namespace_coordination[event_user_id] = 1
            elif page_namespace == '8' or page_namespace == '10':
                try:
                    editor_monthly_namespace_technical[event_user_id] = editor_monthly_namespace_technical[event_user_id]+1
                except:
                    editor_monthly_namespace_technical[event_user_id] = 1

            # ---------    ---------    ---------    ---------    ---------    ---------

            # CHECK MONTH CHANGE AND INSERT MONTHLY EDITS/NAMESPACES EDITS/SECONDS
            current_year_month = datetime.datetime.strptime(
                event_timestamp_dt.strftime('%Y-%m'), '%Y-%m')

            if last_year_month != current_year_month and last_year_month != 0:
                lym = last_year_month.strftime('%Y-%m')
                print('change of month / new: ',
                      current_year_month, 'old: ', lym)

                lym_sp = lym.split('-')
                ly = lym_sp[0]
                lm = lym_sp[1]

                lym_days = calendar.monthrange(int(ly), int(lm))[1]

                monthly_edits = []
                namespaces = []

                for user_id, edits in editor_monthly_edits.items():
                    monthly_edits.append(
                        (user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits', lym, ''))

                for user_id, edits in editor_monthly_namespace_coordination.items():
                    try:
                        namespaces.append(
                            (user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_coordination', lym, ''))
                    except:
                        pass

                for user_id, edits in editor_monthly_namespace_technical.items():
                    try:
                        namespaces.append(
                            (user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_technical', lym, ''))
                    except:
                        pass

                for key, data in editor_user_group_dict_timestamp.items():
                    user_id = key[0]
                    timestamp = key[1]

                    metric_name = data[0]
                    flags = data[1]

                    try:
                        namespaces.append(
                            (user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))

                    except:
                        pass

                query = 'INSERT OR IGNORE INTO '+languagecode + \
                    'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
                cursor.executemany(query, monthly_edits)
                cursor.executemany(query, namespaces)

                conn.commit()

                monthly_edits = []
                namespaces = []

                editor_monthly_namespace_coordination = {}
                editor_monthly_namespace_technical = {}

                editor_monthly_edits = {}
                editor_user_group_dict_timestamp = {}

            last_year_month = current_year_month
            # month change

            # ---------

            # SURVIVAL MEASURES
            event_user_first_edit_timestamp = values[20]
            if event_user_id not in editor_first_edit_timestamp:
                editor_first_edit_timestamp[event_user_id] = event_user_first_edit_timestamp

            if event_user_first_edit_timestamp == '' or event_user_first_edit_timestamp == None:
                event_user_first_edit_timestamp = editor_first_edit_timestamp[event_user_id]

            if event_user_first_edit_timestamp != '' and event_user_id not in survived_dict:
                event_user_first_edit_timestamp_dt = datetime.datetime.strptime(
                    event_user_first_edit_timestamp[:len(event_user_first_edit_timestamp)-2], '%Y-%m-%d %H:%M:%S')

                # thresholds
                first_edit_timestamp_1day_dt = (
                    event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=1))
                first_edit_timestamp_7days_dt = (
                    event_user_first_edit_timestamp_dt + relativedelta.relativedelta(days=7))
                first_edit_timestamp_1months_dt = (
                    event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=1))
                first_edit_timestamp_2months_dt = (
                    event_user_first_edit_timestamp_dt + relativedelta.relativedelta(months=2))

                try:
                    ec = user_id_edit_count[event_user_id]
                except:
                    ec = 1

                # at 1 day
                if event_timestamp_dt >= first_edit_timestamp_1day_dt:

                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_24h', first_edit_timestamp_1day_dt.strftime(
                        '%Y-%m'), first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 7 days
                if event_timestamp_dt >= first_edit_timestamp_7days_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_7d', first_edit_timestamp_7days_dt.strftime(
                        '%Y-%m'), first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 1 month
                if event_timestamp_dt >= first_edit_timestamp_1months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_30d', first_edit_timestamp_1months_dt.strftime(
                        '%Y-%m'), first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')))

                # at 2 months
                if event_timestamp_dt >= first_edit_timestamp_2months_dt:
                    survival_measures.append((event_user_id, event_user_text, ec, None, 'edit_count_60d', first_edit_timestamp_2months_dt.strftime(
                        '%Y-%m'), first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')))
                    survived_dict[event_user_id] = event_user_text

                    try:
                        del user_id_edit_count[event_user_id]
                    except:
                        pass

            # USER PAGE EDIT COUNT, ADD ONE MORE EDIT.
            if event_user_id not in survived_dict:

                # EDIT COUNT, ADD ONE MORE EDIT.
                event_user_revision_count = values[21]
                if event_user_revision_count != '':
                    user_id_edit_count[event_user_id] = event_user_revision_count
                elif event_user_id in user_id_edit_count:
                    user_id_edit_count[event_user_id] = int(
                        user_id_edit_count[event_user_id]) + 1
                else:
                    user_id_edit_count[event_user_id] = 1

            # ---------

        # SURVIVAL MEASURES INSERT
        query = 'INSERT OR IGNORE INTO '+languagecode + \
            'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
        cursor.executemany(query, survival_measures)
        conn.commit()
        survival_measures = []

        # MONTHLY EDITS/NAMESPACES INSERT (LAST ROUND)

        try:
            lym = last_year_month.strftime('%Y-%m')
        except:
            lym = ''

        if lym != cym and lym != '':

            monthly_edits = []
            for event_user_id, edits in editor_monthly_edits.items():
                monthly_edits.append(
                    (event_user_id, user_id_user_name_dict[event_user_id], edits, None, 'monthly_edits', lym, ''))

            query = 'INSERT OR IGNORE INTO '+languagecode + \
                'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query, monthly_edits)
            conn.commit()

            namespaces = []
            for key, data in editor_user_group_dict_timestamp.items():
                user_id = key[0]
                timestamp = key[1]

                metric_name = data[0]
                flags = data[1]

                try:
                    namespaces.append(
                        (user_id, user_id_user_name_dict[user_id], flags, None, metric_name, lym, timestamp))
                except:
                    pass

            for user_id, edits in editor_monthly_namespace_coordination.items():
                try:
                    namespaces.append(
                        (user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_coordination', lym, ''))
                except:
                    pass

            for user_id, edits in editor_monthly_namespace_technical.items():
                try:
                    namespaces.append(
                        (user_id, user_id_user_name_dict[user_id], edits, None, 'monthly_edits_technical', lym, ''))
                except:
                    pass

            query = 'INSERT OR IGNORE INTO '+languagecode + \
                'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
            cursor.executemany(query, monthly_edits)
            cursor.executemany(query, namespaces)
            conn.commit()

            namespaces = []
            monthly_edits = []

            editor_monthly_edits = {}
            editor_monthly_namespace_coordination = {}
            editor_monthly_namespace_technical = {}
            editor_user_group_dict_timestamp = {}

        # USER CHARACTERISTICS INSERT
        user_characteristics1 = []
        user_characteristics2 = []
        for user_id, user_name in user_id_user_name_dict.items():

            try:
                user_flags = user_id_user_groups_dict[user_id]
            except:
                user_flags = None

            try:
                bot = user_id_bot_dict[user_id]
            except:
                bot = 'editor'

            if user_id in survived_dict:
                survived60d = '1'
            else:
                survived60d = '0'

            try:
                edit_count = editor_edit_count[user_id]
            except:
                edit_count = None

            try:
                registration_date = editor_registration_date[user_id]

            except:
                registration_date = None

            # THIS IS SOMETHING WE "ASSUME" BECAUSE THERE ARE MANY ACCOUNTS WITHOUT A REGISTRATION DATE.
            if registration_date == None:
                try:
                    registration_date = editor_first_edit_timestamp[user_id]
                except:
                    registration_date = None

            if registration_date != '' and registration_date != None:
                year_month_registration = datetime.datetime.strptime(registration_date[:len(
                    registration_date)-2], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            else:
                year_month_registration = None

            try:
                fe = editor_first_edit_timestamp[user_id]
            except:
                fe = None

            try:
                le = editor_last_edit_timestamp[user_id]
                year_last_edit = datetime.datetime.strptime(
                    le[:len(le)-2], '%Y-%m-%d %H:%M:%S').strftime('%Y')

            except:
                le = None
                year_last_edit = None

            if fe != None and fe != '':
                year_month = datetime.datetime.strptime(
                    fe[:len(fe)-2], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
                year_first_edit = datetime.datetime.strptime(
                    fe[:len(fe)-2], '%Y-%m-%d %H:%M:%S').strftime('%Y')

                if int(year_first_edit) >= 2001 < 2006:
                    lustrum_first_edit = '2001-2005'
                if int(year_first_edit) >= 2006 < 2011:
                    lustrum_first_edit = '2006-2010'
                if int(year_first_edit) >= 2011 < 2016:
                    lustrum_first_edit = '2011-2015'
                if int(year_first_edit) >= 2016 < 2021:
                    lustrum_first_edit = '2016-2020'
                if int(year_first_edit) >= 2021 < 2026:
                    lustrum_first_edit = '2021-2025'

                fe_d = datetime.datetime.strptime(
                    fe[:len(fe)-2], '%Y-%m-%d %H:%M:%S')
            else:
                year_month = None
                year_first_edit = None
                lustrum_first_edit = None
                fe_d = None

            if le != None:
                le_d = datetime.datetime.strptime(
                    le[:len(le)-2], '%Y-%m-%d %H:%M:%S')
                days_since_last_edit = (cym_timestamp_dt - le_d).days
            else:
                le_d = None
                days_since_last_edit = None

            if fe != None and fe != '' and le != None:
                lifetime_days = (le_d - fe_d).days
            else:
                lifetime_days = None

            user_characteristics1.append((user_id, user_name, registration_date,
                                         year_month_registration,  fe, year_month, year_first_edit, lustrum_first_edit))

            if le != None:
                user_characteristics2.append((bot, user_flags, le, year_last_edit, lifetime_days,
                                             days_since_last_edit, survived60d, edit_count, user_id, user_name))

        query = 'INSERT OR IGNORE INTO '+languagecode + \
            'wiki_editors (user_id, user_name, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit) VALUES (?,?,?,?,?,?,?,?);'
        cursor.executemany(query, user_characteristics1)
        conn.commit()

        query = 'INSERT OR IGNORE INTO '+languagecode + \
            'wiki_editors (bot, user_flags, last_edit_timestamp, year_last_edit, lifetime_days, days_since_last_edit, survived60d, edit_count, user_id, user_name) VALUES (?,?,?,?,?,?,?,?,?,?);'
        cursor.executemany(query, user_characteristics2)
        conn.commit()

        query = 'UPDATE '+languagecode+'wiki_editors SET bot = ?, user_flags = ?, last_edit_timestamp = ?, year_last_edit = ?, lifetime_days = ?, days_since_last_edit = ?, survived60d = ?, edit_count = ?, user_id = ? WHERE user_name = ?;'
        cursor.executemany(query, user_characteristics2)
        conn.commit()

        user_characteristics1 = []
        user_characteristics2 = []

        # insert or ignore + update
        user_id_bot_dict = {}
        user_id_user_groups_dict = {}
        editor_last_edit_timestamp = {}
        editor_seconds_since_last_edit = {}
        editor_edit_count = {}

        # insert or ignore
        editor_first_edit_timestamp = {}
        editor_registration_date = {}

        user_id_user_name_dict2 = {}
        for k in editor_monthly_edits.keys():
            user_id_user_name_dict2[k] = user_id_user_name_dict[k]

        for k in editor_monthly_namespace_coordination.keys():
            user_id_user_name_dict2[k] = user_id_user_name_dict[k]

        for k in editor_monthly_namespace_technical.keys():
            user_id_user_name_dict2[k] = user_id_user_name_dict[k]

        for k in editor_user_group_dict_timestamp.keys():
            user_id_user_name_dict2[k[0]] = user_id_user_name_dict[k[0]]

        user_id_user_name_dict = user_id_user_name_dict2
        user_id_user_name_dict2 = {}

    # Getting the highest flag
    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()
    query = 'SELECT user_flags, count(user_id) FROM '+languagecode + \
        'wiki_editors WHERE user_flags != "" GROUP BY 1;'
    flags_count_dict = {}
    for row in cursor.execute(query):
        flags = row[0]
        count = row[1]

        if ',' in flags:
            fs = flags.split(',')
            for x in fs:
                try:
                    flags_count_dict[x] += count
                except:
                    flags_count_dict[x] = 1
        else:
            try:
                flags_count_dict[flags] += count
            except:
                flags_count_dict[flags] = 1

    flag_ranks = {
        'confirmed': 1, 'ipblock-exempt': 1,
        'filemover': 2, 'accountcreator': 2, 'autopatrolled': 2, 'reviewer': 2, 'autoreviewer': 2, 'rollbacker': 2, 'abusefilter': 2, 'abusefilter-ehlper': 2, 'interface-admin': 2, 'eventcoordinator': 2, 'extendedconfirmed': 2, 'extendedmover': 2, 'filemover': 2, 'massmessage-sender': 2, 'patroller': 2, 'researcher': 2, 'templateeditor': 2,
        'sysop': 3, 'bureaucrat': 3.5,
        'checkuser': 4, 'oversight': 4.5,
        'steward': 5.5, 'import': 5,
        'founder': 6
    }

    query = 'SELECT user_id, user_flags, user_name FROM ' + \
        languagecode+'wiki_editors WHERE user_flags != "";'
    params = []
    user_id_flag = {}
    for row in cursor.execute(query):
        user_id = row[0]
        user_flags = row[1]
        user_name = row[2]

        highest_rank = {}
        highest_count = {}

        if ',' in user_flags:
            uf = user_flags.split(',')

            for x in uf:
                if x in flag_ranks and 'bot' not in x:
                    val = flag_ranks[x]
                    highest_rank[x] = val

            if len(highest_rank) > 1:
                maxval = max(highest_rank.values())
                # we are choosing the flag of highest rank.
                highest_rank = {key: val for key,
                                val in highest_rank.items() if val == maxval}

                if len(highest_rank) > 1:
                    for x in highest_rank.keys():
                        val = flags_count_dict[x]
                        highest_count[x] = val

                    maxval = max(highest_count.values())
                    # we are choosing the flag that exists more in the community.
                    highest_count = {
                        key: val for key, val in highest_count.items() if val == maxval}

                    f = list(highest_count.keys())[0]
                    params.append((f, user_name))
                    user_id_flag[user_id] = f
                else:
                    f = list(highest_rank.keys())[0]
                    params.append((f, user_name))
                    user_id_flag[user_id] = f

        else:
            if user_flags in flag_ranks and 'bot' not in user_flags:
                params.append((user_flags, user_name))
                user_id_flag[user_id] = user_flags

    query = 'UPDATE '+languagecode + \
        'wiki_editors SET highest_flag = ? WHERE user_name = ?;'
    cursor.executemany(query, params)
    conn.commit()

    # let's update the highest_flag_year_month
    query = 'SELECT year_month, user_id, user_name, abs_value FROM ' + \
        languagecode+'wiki_editor_metrics WHERE metric_name = "granted_flag";'
    params2 = []

    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()

    for row in cursor.execute(query):
        year_month = row[0]
        user_id = row[1]
        user_name = row[2]
        flag = row[3]

        try:
            ex_flag = user_id_flag[user_id]
        except:
            continue

        if ex_flag in flag:

            params2.append((year_month, user_name))

    query = 'UPDATE '+languagecode + \
        'wiki_editors SET highest_flag_year_month = ? WHERE user_name = ?;'
    cursor.executemany(query, params2)
    conn.commit()

    # If an editor has been granted the 'bot' flag, even if it has been taken away, it must be a flag.
    query = 'SELECT user_id, user_name FROM '+languagecode + \
        'wiki_editor_metrics WHERE metric_name = "granted_flag" AND abs_value LIKE "%bot";'
    params = []
    for row in cursor.execute(query):
        username = row[1]

        if 'bot' in username:
            bottype = 'name,group'
        else:
            bottype = 'group'
        params.append((bottype, username))

    query = 'UPDATE '+languagecode+'wiki_editors SET bot = ? WHERE user_name = ?;'
    cursor.executemany(query, params)
    conn.commit()
    logger.info("Processed all dump's data")


def calculate_editor_activity_streaks(languagecode):
    logger = logging.getLogger(languagecode + '' + __name__)

    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()

    # MONTHLY EDITS LOOP
    query = 'SELECT abs_value, year_month, user_id, user_name FROM '+languagecode + \
        'wiki_editor_metrics WHERE metric_name = "monthly_edits" ORDER BY user_name, year_month'

    old_user_id = ''
    expected_year_month_dt = ''
    active_months_row = 0

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass

    edfile2 = open(config.databases_path+'temporary_editor_metrics.txt', "w")
    for row in cursor.execute(query):
        current_year_month = row[1]
        cur_user_id = row[2]
        cur_user_name = row[3]

        if cur_user_id != old_user_id and old_user_id != '':
            active_months_row = 0

        current_year_month_dt = datetime.datetime.strptime(
            current_year_month, '%Y-%m')

        # here there is a change of month
        # if the month is not the expected one
        if expected_year_month_dt != current_year_month_dt and expected_year_month_dt != '' and old_user_id == cur_user_id:

            while expected_year_month_dt < current_year_month_dt:

                expected_year_month_dt = (
                    expected_year_month_dt + relativedelta.relativedelta(months=1))

            active_months_row = 1

        else:
            active_months_row = active_months_row + 1

            if active_months_row > 1:

                edfile2.write(str(cur_user_id)+'\t'+cur_user_name+'\t'+str(active_months_row) +
                              '\t'+" "+'\t'+"active_months_row"+'\t'+current_year_month+'\t'+" "+'\n')

        old_year_month = current_year_month
        expected_year_month_dt = (datetime.datetime.strptime(
            old_year_month, '%Y-%m') + relativedelta.relativedelta(months=1))

    conn = sqlite3.connect(config.databases_path +
                           config.vital_signs_editors_db)
    cursor = conn.cursor()

    a_file = open(config.databases_path+"temporary_editor_metrics.txt")
    editors_metrics_parameters = csv.reader(
        a_file, delimiter="\t", quotechar='|')
    query = 'INSERT OR IGNORE INTO '+languagecode + \
        'wiki_editor_metrics (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp) VALUES (?,?,?,?,?,?,?);'
    cursor.executemany(query, editors_metrics_parameters)
    conn.commit()
    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
    editors_metrics_parameters = []

    logger.info("Processed all activity streaks")
