from collections import defaultdict
from scripts import utils
from scripts import config
import datetime
import bz2
import calendar
from sqlalchemy import create_engine, text
from dateutil import relativedelta
import logging


def process_editor_metrics_from_dump(languagecode):
    logger = logging.getLogger(languagecode + '' + __name__)

    d_paths, cym = utils.get_mediawiki_paths(languagecode)
    cym_timestamp_dt = datetime.datetime.today().replace(
        day=1)

    engine_editors = create_engine(config.db_uri_editors)

    with engine_editors.begin() as conn:

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
       

        for dump_path in d_paths:
            logger.info(f"processing {dump_path}")
            dump_in = bz2.open(dump_path, 'r')

            while True:
                line = dump_in.readline()
                if line == b'':
                    break
                line = line.rstrip().decode('utf-8')
                if not line:
                    continue
                values = line.split('\t')
                if len(values) == 1:
                    continue

                event_entity = values[1]  # user, page, revision
                # diversi tipi di evento per ogni entità
                event_type = values[2]

                # id dell'utente che ha causato l'evento
                event_user_id = values[5]

                event_user_is_anonymous = values[17]
                if event_user_is_anonymous == True or event_user_id == '':
                    continue

                if event_user_id.isdigit():
                    event_user_id = int(event_user_id)
                else:
                    continue

                # username dell'utente che ha causato l'evento
                event_user_text = values[7]

                if event_user_text != '':
                    user_id_user_name_dict[event_user_id] = event_user_text
                else:
                    continue

                # try:
                #    editor_last_edit = editor_last_edit_timestamp[event_user_id]
                #    last_edit_date_dt = datetime.datetime.strptime(
                #        editor_last_edit[:len(editor_last_edit)-2], '%Y-%m-%d %H:%M:%S')
                #    last_edit_year_month_day = datetime.datetime.strptime(
                #        last_edit_date_dt.strftime('%Y-%m-%d'), '%Y-%m-%d')
                # except:
                #    last_edit_year_month_day = ''

                # stringa con la data e l'ora dell'evento
                event_timestamp = values[3]

                event_timestamp_dt = datetime.datetime.strptime(
                    # conversione in datetime
                    event_timestamp[:len(event_timestamp)-2], '%Y-%m-%d %H:%M:%S')

                editor_last_edit_timestamp[event_user_id] = event_timestamp

                # could be a problem
                editor_edit_count[event_user_id] = values[23]

                event_user_groups = values[11]

                if event_user_groups != '':
                    user_id_user_groups_dict[event_user_id] = event_user_groups

                page_namespace = values[30]

                if event_entity == 'user':  # vado a vedere se sono stati cambiati gli usergroup dell'utente

                    user_text = str(values[40])  # this is target of the event

                    if event_type == 'altergroups':

                        if values[38] == '':
                            continue

                        user_id = int(values[38])
                        cur_ug = values[44]

                        if user_text != '' and user_id != '':
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

                event_is_bot_by = values[13]  # aggiungo i dati dei bot
                if event_is_bot_by != '':
                    user_id_bot_dict[event_user_id] = event_is_bot_by

                # aggiungo data di registrazione dell'utente
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
                if page_namespace == '4' or page_namespace == '12':  # namespaces di coordinamento
                    try:
                        editor_monthly_namespace_coordination[
                            event_user_id] = editor_monthly_namespace_coordination[event_user_id]+1
                    except:
                        editor_monthly_namespace_coordination[event_user_id] = 1
                elif page_namespace == '8' or page_namespace == '10':  # namespaces tecnici
                    try:
                        editor_monthly_namespace_technical[event_user_id] = editor_monthly_namespace_technical[event_user_id]+1
                    except:
                        editor_monthly_namespace_technical[event_user_id] = 1

                # ---------    ---------    ---------    ---------    ---------    ---------

                # CHECK MONTH CHANGE AND INSERT MONTHLY EDITS/NAMESPACES EDITS/SECONDS
                current_year_month = datetime.datetime.strptime(
                    event_timestamp_dt.strftime('%Y-%m'), '%Y-%m')

                if last_year_month != current_year_month and last_year_month != 0:
                    # ad ogni cambio di mese viene aggiunto al db per ogni utente il numero di edit e il mese
                    lym = last_year_month.strftime('%Y-%m')

                    monthly_edits = []
                    namespaces = []

                    for user_id, edits in editor_monthly_edits.items():
                        if user_id in user_id_user_name_dict:
                            monthly_edits.append({
                                'user_id': user_id,
                                'user_name': user_id_user_name_dict[user_id],
                                'abs_value': edits,
                                'rel_value': None,
                                'metric_name': 'monthly_edits',
                                'year_month': lym,
                                'timestamp': ''
                            })

                    for user_id, edits in editor_monthly_namespace_coordination.items():
                        if user_id in user_id_user_name_dict:
                            namespaces.append({
                                'user_id': user_id,
                                'user_name': user_id_user_name_dict[user_id],
                                'abs_value': edits,
                                'rel_value': None,
                                'metric_name': 'monthly_edits_coordination',
                                'year_month': lym,
                                'timestamp': ''
                            })

                    for user_id, edits in editor_monthly_namespace_technical.items():
                        if user_id in user_id_user_name_dict:
                            namespaces.append({
                                'user_id': user_id,
                                'user_name': user_id_user_name_dict[user_id],
                                'abs_value': edits,
                                'rel_value': None,
                                'metric_name': 'monthly_edits_technical',
                                'year_month': lym,
                                'timestamp': ''
                            })

                    for key, data in editor_user_group_dict_timestamp.items():
                        user_id = key[0]
                        timestamp = key[1]

                        metric_name = data[0]
                        flags = data[1]

                        if user_id in user_id_user_name_dict:
                            namespaces.append({
                                'user_id': user_id,
                                'user_name': user_id_user_name_dict[user_id],
                                'abs_value': flags,
                                'rel_value': None,
                                'metric_name': metric_name,
                                'year_month': lym,
                                'timestamp': timestamp
                            })

                    logger.info(f"DEBUG: Processing month {lym}")
                    logger.info(
                        f"DEBUG: Monthly edits count: {len(monthly_edits)}")
                    logger.info(f"DEBUG: Namespaces count: {len(namespaces)}")
                    if monthly_edits:
                        logger.info(
                            f"DEBUG: Sample monthly_edits: {monthly_edits[:3]}")
                    if namespaces:
                        logger.info(
                            f"DEBUG: Sample namespaces: {namespaces[:3]}")

                    query = text(f"""
                        INSERT INTO {languagecode}wiki_editor_metrics
                        (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp)
                        VALUES (:user_id, :user_name, :abs_value, :rel_value, :metric_name, :year_month, :timestamp)
                        ON CONFLICT DO NOTHING
                    """)
                    conn.execute(query, monthly_edits)
                    conn.execute(query, namespaces)

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
                # Get first edit timestamp from dump
                event_user_first_edit_timestamp = values[22]

                # Store first edit timestamp if not already recorded
                if event_user_id not in editor_first_edit_timestamp and event_user_first_edit_timestamp:
                    editor_first_edit_timestamp[event_user_id] = event_user_first_edit_timestamp

                # Use stored timestamp if current one is empty
                if not event_user_first_edit_timestamp:
                    event_user_first_edit_timestamp = editor_first_edit_timestamp.get(
                        event_user_id)

                # Process survival measures if we have a valid timestamp and user hasn't survived yet
                if event_user_first_edit_timestamp and event_user_id not in survived_dict:
                    # Remove microseconds (.0) from timestamp string
                    timestamp_str = event_user_first_edit_timestamp.rsplit('.', 1)[
                        0]
                    event_user_first_edit_timestamp_dt = datetime.datetime.strptime(
                        timestamp_str, '%Y-%m-%d %H:%M:%S')

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

                        survival_measures.append({
                            'user_id': event_user_id,
                            'user_name': event_user_text,
                            'abs_value': ec,
                            'rel_value': None,
                            'metric_name': 'edit_count_24h',
                            'year_month': first_edit_timestamp_1day_dt.strftime('%Y-%m'),
                            'timestamp': first_edit_timestamp_1day_dt.strftime('%Y-%m-%d %H:%M:%S')
                        })

                    # at 7 days
                    if event_timestamp_dt >= first_edit_timestamp_7days_dt:
                        survival_measures.append({
                            'user_id': event_user_id,
                            'user_name': event_user_text,
                            'abs_value': ec,
                            'rel_value': None,
                            'metric_name': 'edit_count_7d',
                            'year_month': first_edit_timestamp_7days_dt.strftime('%Y-%m'),
                            'timestamp': first_edit_timestamp_7days_dt.strftime('%Y-%m-%d %H:%M:%S')
                        })

                    # at 1 month
                    if event_timestamp_dt >= first_edit_timestamp_1months_dt:
                        survival_measures.append({
                            'user_id': event_user_id,
                            'user_name': event_user_text,
                            'abs_value': ec,
                            'rel_value': None,
                            'metric_name': 'edit_count_30d',
                            'year_month': first_edit_timestamp_1months_dt.strftime('%Y-%m'),
                            'timestamp': first_edit_timestamp_1months_dt.strftime('%Y-%m-%d %H:%M:%S')
                        })

                    # at 2 months
                    if event_timestamp_dt >= first_edit_timestamp_2months_dt:
                        survival_measures.append({
                            'user_id': event_user_id,
                            'user_name': event_user_text,
                            'abs_value': ec,
                            'rel_value': None,
                            'metric_name': 'edit_count_60d',
                            'year_month': first_edit_timestamp_2months_dt.strftime('%Y-%m'),
                            'timestamp': first_edit_timestamp_2months_dt.strftime('%Y-%m-%d %H:%M:%S')
                        })
                        survived_dict[event_user_id] = event_user_text

                        try:
                            del user_id_edit_count[event_user_id]
                        except:
                            pass

                # USER PAGE EDIT COUNT, ADD ONE MORE EDIT.
                if event_user_id not in survived_dict:

                    # EDIT COUNT, ADD ONE MORE EDIT.
                    event_user_revision_count = values[23]
                    if event_user_revision_count != '':
                        user_id_edit_count[event_user_id] = event_user_revision_count
                    elif event_user_id in user_id_edit_count:
                        user_id_edit_count[event_user_id] = int(
                            user_id_edit_count[event_user_id]) + 1
                    else:
                        user_id_edit_count[event_user_id] = 1

                # ---------

            # SURVIVAL MEASURES INSERT
            query = text(f"""
                            INSERT INTO {languagecode}wiki_editor_metrics
                            (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp)
                            VALUES (:user_id, :user_name, :abs_value, :rel_value, :metric_name, :year_month, :timestamp)
                            ON CONFLICT DO NOTHING
                        """)
            
            conn.execute(query, survival_measures)

            survival_measures = []

            # MONTHLY EDITS/NAMESPACES INSERT (LAST ROUND)

            try:
                lym = last_year_month.strftime('%Y-%m')
            except:
                lym = ''

            if lym != cym and lym != '':

                monthly_edits = []
                for event_user_id, edits in editor_monthly_edits.items():

                    try:
                        monthly_edits.append({
                            'user_id': event_user_id,
                            'user_name': user_id_user_name_dict[event_user_id],
                            'abs_value': edits,
                            'rel_value': None,
                            'metric_name': 'monthly_edits',
                            'year_month': lym,
                            'timestamp': ''
                        })
                    except:
                        pass

                query = text(f"""
                    INSERT INTO {languagecode}wiki_editor_metrics
                    (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp)
                    VALUES (:user_id, :user_name, :abs_value, :rel_value, :metric_name, :year_month, :timestamp)
                    ON CONFLICT DO NOTHING
                """)
                conn.execute(query, monthly_edits)

                namespaces = []
                for key, data in editor_user_group_dict_timestamp.items():
                    user_id = key[0]
                    timestamp = key[1]

                    metric_name = data[0]
                    flags = data[1]

                    if user_id in user_id_user_name_dict:
                        namespaces.append({
                            'user_id': user_id,
                            'user_name': user_id_user_name_dict[user_id],
                            'abs_value': flags,
                            'rel_value': None,
                            'metric_name': metric_name,
                            'year_month': lym,
                            'timestamp': timestamp
                        })

                for user_id, edits in editor_monthly_namespace_coordination.items():
                    if user_id in user_id_user_name_dict:
                        namespaces.append({
                            'user_id': user_id,
                            'user_name': user_id_user_name_dict[user_id],
                            'abs_value': edits,
                            'rel_value': None,
                            'metric_name': 'monthly_edits_coordination',
                            'year_month': lym,
                            'timestamp': ''
                        })

                for user_id, edits in editor_monthly_namespace_technical.items():
                    if user_id in user_id_user_name_dict:
                        namespaces.append({
                            'user_id': user_id,
                            'user_name': user_id_user_name_dict[user_id],
                            'abs_value': edits,
                            'rel_value': None,
                            'metric_name': 'monthly_edits_technical',
                            'year_month': lym,
                            'timestamp': ''
                        })

                query = text(f"""
                        INSERT INTO {languagecode}wiki_editor_metrics
                        (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp)
                        VALUES (:user_id, :user_name, :abs_value, :rel_value, :metric_name, :year_month, :timestamp)
                        ON CONFLICT DO NOTHING
                    """)
                
                conn.execute(query, namespaces)

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

                user_characteristics1.append({
                    'user_id': user_id,
                    'user_name': user_name,
                    'registration_date': registration_date,
                    'year_month_registration': year_month_registration,
                    'first_edit_timestamp': fe,
                    'year_month_first_edit': year_month,
                    'year_first_edit': year_first_edit,
                    'lustrum_first_edit': lustrum_first_edit
                })

                if le != None:
                    user_characteristics2.append({
                        'user_id': user_id,
                        'user_name': user_name,
                        'registration_date': registration_date,
                        'year_month_registration': year_month_registration,
                        'first_edit_timestamp': fe,
                        'year_month_first_edit': year_month,
                        'year_first_edit': year_first_edit,
                        'lustrum_first_edit': lustrum_first_edit,
                        'bot': bot,
                        'user_flags': user_flags,
                        'last_edit_timestamp': le,
                        'year_last_edit': year_last_edit,
                        'lifetime_days': lifetime_days,
                        'days_since_last_edit': days_since_last_edit,
                        'survived60d': survived60d,
                        'edit_count': edit_count
                    })

            query = text(f"""
                INSERT INTO {languagecode}wiki_editors 
                (user_id, user_name, registration_date, year_month_registration, first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit) 
                VALUES (:user_id, :user_name, :registration_date, :year_month_registration, :first_edit_timestamp, :year_month_first_edit, :year_first_edit, :lustrum_first_edit)
                ON CONFLICT DO NOTHING
            """)
            conn.execute(query, user_characteristics1)

            # upsert
            query = text(f"""
    INSERT INTO {languagecode}wiki_editors (
        user_id, user_name, registration_date, year_month_registration, 
        first_edit_timestamp, year_month_first_edit, year_first_edit, lustrum_first_edit,
        bot, user_flags, last_edit_timestamp, year_last_edit, lifetime_days,
        days_since_last_edit, survived60d, edit_count
    ) VALUES (
        :user_id, :user_name, :registration_date, :year_month_registration, 
        :first_edit_timestamp, :year_month_first_edit, :year_first_edit, :lustrum_first_edit,
        :bot, :user_flags, :last_edit_timestamp, :year_last_edit, :lifetime_days,
        :days_since_last_edit, :survived60d, :edit_count
    )
    ON CONFLICT (user_name) DO UPDATE SET
        bot = EXCLUDED.bot,
        user_flags = EXCLUDED.user_flags,
        last_edit_timestamp = EXCLUDED.last_edit_timestamp,
        year_last_edit = EXCLUDED.year_last_edit,
        lifetime_days = EXCLUDED.lifetime_days,
        days_since_last_edit = EXCLUDED.days_since_last_edit,
        survived60d = EXCLUDED.survived60d,
        edit_count = EXCLUDED.edit_count,
        user_id = EXCLUDED.user_id,
        registration_date = EXCLUDED.registration_date,
        year_month_registration = EXCLUDED.year_month_registration,
        first_edit_timestamp = EXCLUDED.first_edit_timestamp,
        year_month_first_edit = EXCLUDED.year_month_first_edit,
        year_first_edit = EXCLUDED.year_first_edit,
        lustrum_first_edit = EXCLUDED.lustrum_first_edit
""")

            conn.execute(query, user_characteristics2)

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

    logger.info("Processed all dump's data")


def calculate_editors_flag(languagecode):
    engine = create_engine(config.db_uri_editors)
    with engine.begin() as conn:  # begin() -> transaction
        # 1. Conta le flags
        query = text(f'''
            SELECT user_flags, count(user_id)
            FROM {languagecode}wiki_editors
            WHERE user_flags != ''
            GROUP BY 1;
        ''')
        flags_count_dict = defaultdict(int)
        for row in conn.execute(query):
            flags, count = row
            if ',' in flags:
                for x in flags.split(','):
                    flags_count_dict[x] += count
            else:
                flags_count_dict[flags] += count

        flag_ranks = {
            'confirmed': 1, 'ipblock-exempt': 1,
            'filemover': 2, 'accountcreator': 2, 'autopatrolled': 2, 'reviewer': 2, 'autoreviewer': 2, 'rollbacker': 2, 'abusefilter': 2, 'abusefilter-ehlper': 2, 'interface-admin': 2, 'eventcoordinator': 2, 'extendedconfirmed': 2, 'extendedmover': 2, 'filemover': 2, 'massmessage-sender': 2, 'patroller': 2, 'researcher': 2, 'templateeditor': 2,
            'sysop': 3, 'bureaucrat': 3.5,
            'checkuser': 4, 'oversight': 4.5,
            'steward': 5.5, 'import': 5,
            'founder': 6
        }

        # 2. Assegna la highest_flag per ogni user_name
        query = text(f'''
            SELECT user_id, user_flags, user_name
            FROM {languagecode}wiki_editors
            WHERE user_flags != '';
        ''')
        params = []
        user_id_flag = {}
        for row in conn.execute(query):
            user_id, user_flags, user_name = row
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
                    highest_rank = {key: val for key,
                                    val in highest_rank.items() if val == maxval}

                    if len(highest_rank) > 1:
                        for x in highest_rank.keys():
                            val = flags_count_dict[x]
                            highest_count[x] = val

                        maxval = max(highest_count.values())
                        highest_count = {
                            key: val for key, val in highest_count.items() if val == maxval}

                        f = list(highest_count.keys())[0]
                        params.append({'f': f, 'user_name': user_name})
                        user_id_flag[user_id] = f
                    else:
                        f = list(highest_rank.keys())[0]
                        params.append({'f': f, 'user_name': user_name})
                        user_id_flag[user_id] = f

            else:
                if user_flags in flag_ranks and 'bot' not in user_flags:
                    params.append({'f': user_flags, 'user_name': user_name})
                    user_id_flag[user_id] = user_flags

        update_query = text(f'''
            UPDATE {languagecode}wiki_editors
            SET highest_flag = :f
            WHERE user_name = :user_name
        ''')
        if params:
            conn.execute(update_query, params)

        # 3. Aggiorna highest_flag_year_month
        query = text(f'''
            SELECT year_month, user_id, user_name, abs_value
            FROM {languagecode}wiki_editor_metrics
            WHERE metric_name = 'granted_flag';
        ''')
        params2 = []
        for row in conn.execute(query):
            year_month, user_id, user_name, flag = row
            ex_flag = user_id_flag.get(user_id)
            if ex_flag and ex_flag in flag:
                params2.append(
                    {'year_month': year_month, 'user_name': user_name})

        update_query2 = text(f'''
            UPDATE {languagecode}wiki_editors
            SET highest_flag_year_month = :year_month
            WHERE user_name = :user_name
        ''')
        if params2:
            conn.execute(update_query2, params2)

        # 4. Se un editor ha avuto il flag 'bot'
        query = text(f'''
            SELECT user_id, user_name
            FROM {languagecode}wiki_editor_metrics
            WHERE metric_name = 'granted_flag' AND abs_value LIKE '%bot';
        ''')
        params3 = []
        for row in conn.execute(query):
            username = row[1]
            bottype = 'name,group' if 'bot' in username else 'group'
            params3.append({'bottype': bottype, 'user_name': username})

        update_query3 = text(f'''
            UPDATE {languagecode}wiki_editors
            SET bot = :bottype
            WHERE user_name = :user_name
        ''')
        if params3:
            conn.execute(update_query3, params3)


def calculate_editor_activity_streaks(languagecode):
    logger = logging.getLogger(__name__)
    engine = create_engine(config.db_uri_editors)
    with engine.begin() as conn:
        # Ottieni tutti i record ordinati
        query = text(f'''
            SELECT abs_value, year_month, user_id, user_name
            FROM {languagecode}wiki_editor_metrics
            WHERE metric_name = 'monthly_edits'
            ORDER BY user_name, year_month
        ''')

        results = list(conn.execute(query))

        to_insert = []

        old_user_id = None
        expected_year_month_dt = None
        active_months_row = 0

        for row in results:
            edits, current_year_month, cur_user_id, cur_user_name = row

            if cur_user_id != old_user_id and old_user_id is not None:
                active_months_row = 0
                expected_year_month_dt = None

            current_year_month_dt = datetime.datetime.strptime(
                current_year_month, '%Y-%m'
            )

            if (
                expected_year_month_dt is not None and
                expected_year_month_dt != current_year_month_dt and
                old_user_id == cur_user_id
            ):
                # Skip i mesi mancanti (reset della streak)
                while expected_year_month_dt < current_year_month_dt:
                    expected_year_month_dt = (
                        expected_year_month_dt +
                        relativedelta.relativedelta(months=1)
                    )
                active_months_row = 1
            else:
                active_months_row += 1
                if active_months_row > 1:
                    # Prepara la riga per l'inserimento
                    to_insert.append({
                        'user_id': cur_user_id,
                        'user_name': cur_user_name,
                        'abs_value': active_months_row,
                        'rel_value': None,
                        'metric_name': 'active_months_row',
                        'year_month': current_year_month,
                        'timestamp': None
                    })

            expected_year_month_dt = current_year_month_dt + \
                relativedelta.relativedelta(months=1)
            old_user_id = cur_user_id

        # Inserisci direttamente nel database
        if to_insert:
            insert_query = text(f'''
                INSERT INTO {languagecode}wiki_editor_metrics
                    (user_id, user_name, abs_value, rel_value, metric_name, year_month, timestamp)
                VALUES
                    (:user_id, :user_name, :abs_value, :rel_value, :metric_name, :year_month, :timestamp)
                ON CONFLICT DO NOTHING
            ''')
            conn.execute(insert_query, to_insert)

        logger.info("Processed all activity streaks")
