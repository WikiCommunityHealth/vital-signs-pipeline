from scripts import config
import sqlite3

def create_db(wikilanguagecodes):
    conn = sqlite3.connect(config.databases_path + config.vital_signs_editors_db)
    cursor = conn.cursor()
    conn2 = sqlite3.connect(config.databases_path + config.vital_signs_web_db)
    cursor2 = conn2.cursor()
    
    
    # EDITORS DB
    for languagecode in wikilanguagecodes:
        table_name = languagecode+'wiki_editors'
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (" +
    
                 "user_id integer, user_name text, bot text, user_flags text, highest_flag text, highest_flag_year_month text, gender text, " +
    
                 "primarybinary integer, primarylang text, edit_count integer, primary_ecount integer, totallangs_ecount integer, primary_year_month_first_edit text, primary_lustrum_first_edit text, numberlangs integer, " +
    
                 "registration_date, year_month_registration, first_edit_timestamp text, year_month_first_edit text, year_first_edit text, lustrum_first_edit text, " +
    
                 "survived60d text, last_edit_timestamp text, year_last_edit text, lifetime_days integer, days_since_last_edit integer, PRIMARY KEY (user_name))")
        cursor.execute(query)
    
        table_name = languagecode+'wiki_editor_metrics'
    
        try:
            cursor.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name +
                 " (user_id integer, user_name text, abs_value real, rel_value real, metric_name text, year_month text, timestamp text, PRIMARY KEY (user_id, metric_name, year_month, timestamp))")
        cursor.execute(query)
    
        # VITAL SIGNS DB
        table_name = 'vital_signs_metrics'
        try:
            cursor2.execute("DROP TABLE "+table_name+";")
        except:
            pass
        query = ("CREATE TABLE IF NOT EXISTS "+table_name+" (langcode text, year_year_month text, year_month text, topic text, m1 text, m1_calculation text, m1_value text, m2 text, m2_calculation text, m2_value text, m1_count float, m2_count float, PRIMARY KEY (langcode, year_year_month, year_month, topic, m1, m1_calculation, m1_value, m2, m2_calculation, m2_value))")
        cursor2.execute(query)
    
        conn2.commit()



