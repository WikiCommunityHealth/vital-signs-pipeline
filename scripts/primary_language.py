import config
import os
import sqlite3
import csv


def cross_wiki_editor_metrics(wikilanguagecodes):

    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    query = ("CREATE TABLE IF NOT EXISTS allwiki_editors (lang text, user_name text, edit_count integer, year_month_first_edit text, lustrum_first_edit text, PRIMARY KEY (lang, user_name));")
    cursor.execute(query)

    for languagecode in wikilanguagecodes:
        query = 'INSERT INTO allwiki_editors SELECT "'+languagecode + \
            '", user_name, edit_count, year_month_first_edit, lustrum_first_edit FROM ' + \
                languagecode+'wiki_editors WHERE user_name != "";'
        cursor.execute(query)
        conn.commit()
        
    print('Allwiki editors table filled')

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
    edfile2 = open(config.databases_path+'temporary_editor_metrics.txt', "w")

    query = 'SELECT user_name, lang, edit_count, year_month_first_edit, lustrum_first_edit FROM allwiki_editors ORDER BY user_name, edit_count DESC;'

    numbereditors = 0
    totallangs_ecount = 0
    numberlangs = 0
    primarylang = ''
    primary_ecount = 0
    primary_year_month_first_edit = ''
    primary_lustrum_first_edit = ''

    old_user_name = ''

    for row in cursor.execute(query):
        user_name = row[0]
        lang = row[1]
        try:
            edit_count = int(row[2])
        except:
            edit_count = 0

        try:
            year_month_first_edit = str(row[3])
        except:
            year_month_first_edit = ''

        try:
            lustrum_first_edit = str(row[4])
        except:
            lustrum_first_edit = ''

        if user_name != old_user_name and old_user_name != '':
            numbereditors += 1

            try:
                edfile2.write(primarylang+'\t'+str(primary_ecount)+'\t'+str(totallangs_ecount)+'\t'+str(numberlangs) +
                              '\t' + primary_year_month_first_edit+'\t'+primary_lustrum_first_edit+'\t'+old_user_name+'\n')
            except:
                pass

            # clean
            totallangs_ecount = 0
            numberlangs = 0
            primarylang = ''
            primary_ecount = 0
            primary_year_month_first_edit = ''
            primary_lustrum_first_edit = ''

        if edit_count > 4 and lang != "meta":
            numberlangs += 1

        # by definition we do not consider that meta can be a primary language. it is not a wikipedia.
        if (edit_count > primary_ecount and lang != "meta"):
            primarylang = lang
            primary_ecount = edit_count

            primary_year_month_first_edit = year_month_first_edit
            primary_lustrum_first_edit = lustrum_first_edit

        if primarylang == '' and lang != "meta":
            primarylang = lang

        totallangs_ecount += edit_count

        old_user_name = user_name
        old_lang = lang

    edfile2.write(primarylang+'\t'+str(primary_ecount)+'\t'+str(totallangs_ecount)+'\t'+str(numberlangs) +
                  '\t' + primary_year_month_first_edit+'\t'+primary_lustrum_first_edit+'\t'+user_name+'\n')

    print('All in the txt file')

    query = "DROP TABLE allwiki_editors;"
    cursor.execute(query)
    conn.commit()
    print('Allwiki editors table deleted')

    ###
    conn = sqlite3.connect(config.databases_path + 'vital_signs_editors.db')
    cursor = conn.cursor()

    for languagecode in wikilanguagecodes:
        print(languagecode)
        a_file = open(config.databases_path+"temporary_editor_metrics.txt")
        parameters = csv.reader(a_file, delimiter="\t", quotechar='|')

        query = 'UPDATE '+languagecode + \
            'wiki_editors SET (primarylang, primary_ecount, totallangs_ecount, numberlangs, primary_year_month_first_edit, primary_lustrum_first_edit) = (?,?,?,?,?,?) WHERE user_name = ?;'

        cursor.executemany(query, parameters)
        conn.commit()

    print("All the original tables updated with the editors' primary language")

    try:
        os.remove(config.databases_path + 'temporary_editor_metrics.txt')
    except:
        pass
