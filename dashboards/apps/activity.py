# -*- coding: utf-8 -*-
from urllib.parse import urlparse, parse_qsl, urlencode
import os
import sys
from time import time
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__,path="/activity")


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# activity_app = Dash(__name__, server = server, url_base_pathname= webtype + "/activity/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# activity_app.config['suppress_callback_exceptions']=True

title = "Activity"+title_addenda
layout = html.Div([

    dcc.Location(id='activity_url', refresh=False),
    html.H2(html.B('Activity'), style={
            'textAlign': 'center', 'font-weight': 'bold'}),

    html.Div(
        html.P('Languages'),
        style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        html.P('Editors'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Div(
        html.P('Time Aggregation'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Br(),

    html.Div(
        dcc.Dropdown(
            id='langcode',
            options=[{'label': k, 'value': k}
                     for k in language_names_list],
            multi=True,
            value='English (en)',
            style={'width': '490px'}
        ), style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        dcc.RadioItems(id='active_veryactive',
                       options=[{'label': 'Active', 'value': '5'}, {
                           'label': 'Very Active', 'value': '100'}],
                       value='5',
                       labelStyle={'display': 'inline-block',
                                   "margin": "0px 5px 0px 0px"},
                       style={'width': '200px'}
                       ), style={'display': 'inline-block', 'width': '200px'}),

    html.Div(
        dcc.RadioItems(id='year_yearmonth',
                       options=[{'label': 'Yearly', 'value': 'y'},
                                {'label': 'Monthly', 'value': 'ym'}],
                       value='ym',
                       labelStyle={'display': 'inline-block',
                                   "margin": "0px 5px 0px 0px"},
                       style={'width': '200px'}
                       ), style={'display': 'inline-block', 'width': '200px'}),

    # html.Hr(),

    html.Div(id='activity_output_container', children=[]),
    dcc.Graph(id='activity_my_graph', figure={}),

    html.Div(id='activity_output_container1', children=[]),
    dcc.Markdown(id="temp", children=[], style={
                 "font-size": "18px"}, className='container'),

    html.Div(id='activity_highlights_container', children=[]),
    dcc.Markdown(id='activity_highlights', children=[], style={
                 "font-size": "18px"}, className='container')

], className="container")

# FUNCTIONS


def timeconversion(rawdate, time_type):

    if time_type == 'ym':

        year = ''
        month = ''

        print("Raw date is: "+rawdate)

        lista = rawdate.split('-')

        print(lista[0])
        print(lista[1])

        year = lista[0]

        if lista[1] == '01':
            month = 'January'
        elif lista[1] == '02':
            month = 'February'
        elif lista[1] == '03':
            month = 'March'
        elif lista[1] == '04':
            month = 'April'
        elif lista[1] == '05':
            month = 'May'
        elif lista[1] == '06':
            month = 'June'
        elif lista[1] == '07':
            month = 'July'
        elif lista[1] == '08':
            month = 'August'
        elif lista[1] == '09':
            month = 'September'
        elif lista[1] == '10':
            month = 'October'
        elif lista[1] == '11':
            month = 'November'
        elif lista[1] == '12':
            month = 'December'
        else:
            month = 'invalid'

        date = month+' '+year
        print("Date is:"+date)
    elif time_type == 'y':
        date = rawdate
    return date


def get_count(df):
    # il valore della media nell'ultimo tempo
    temp_c = df["m1_count"].tolist()
    count = temp_c[0]

    return count


def get_media(df):
    # la media negli ultimi 5 anni/mesi
    temp_m = df["Media"].tolist()
    media = temp_m[0]

    return media


def get_time(df):
    # l'ultimo tempo disponibile
    temp = df["year_month"].tolist()
    temp1 = temp[0]

    return temp1


def generate1(lingua, attivi, tempo, last5, conn):

    query0 = "SELECT * FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value=" + \
        attivi+" AND year_year_month = '"+tempo+"'"
    query1 = " AND langcode IN (%s) ORDER BY year_month DESC LIMIT 1" % lingua

    query2 = query0+query1

    df1 = pd.read_sql_query(query2, conn)

    df1.reset_index(inplace=True)

    print("QUERY FOR DATAFRAME (HIGHLIGHTS)="+query1)
    print("---------------------------")

    return df1


def generate2(lingua, attivi, tempo, last5, conn):

    query0 = "SELECT AVG(m1_count) AS Media FROM vital_signs_metrics WHERE year_month IN "+last5 + \
        " AND topic = 'active_editors' AND year_year_month = '" + \
        tempo+"'  AND m1_value='"+attivi+"'"
    query1 = " AND langcode IN (%s)" % lingua

    query2 = query0+query1

    df2 = pd.read_sql_query(query2, conn)

    df2.reset_index(inplace=True)

    print("QUERY FOR AVERAGE ="+query2)
    print("---------------------------")

    return df2


def generatetail(count, media, active, time):

    if (count > media):
        tail = ", which is **above** the average number (**"+str(
            media)+"**) of "+active+" editors over the previous last **"+time+"**. \n"
    elif (count < media):
        tail = ", which is **below** the average number (**"+str(
            media)+"**) of "+active+" editors over the previous last **"+time+"**. \n"
    else:
        tail = ", which is **inline** the average number (**"+str(
            media)+"**) of "+active+" editors over the previous last **"+time+"**. \n"

    return tail


def findMax(language, active, yearmonth, time, conn):

    query0 = "SELECT MAX(m1_count) as max,langcode, year_month FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value = '" + \
        active+"' AND year_year_month = '"+yearmonth+"' AND year_month='"+time+"'"
    query1 = " AND langcode IN (%s)" % language

    query = query0 + query1
    print("FIND MAX="+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)
    return df


def findMax2(language, active, yearmonth, conn):

    query0 = "SELECT MAX(m1_count) as max,langcode, year_month FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value = '" + \
        active+"' AND year_year_month = '"+yearmonth+"'"
    query1 = " AND langcode IN (%s)" % language

    query = query0 + query1
    print("FIND MAX="+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)
    return df


def findMin(language, active, yearmonth, time, conn):

    query0 = "SELECT MIN(m1_count) as min,langcode FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value = '" + \
        active+"' AND year_year_month = '"+yearmonth+"' AND year_month='"+time+"'"
    query1 = " AND langcode IN (%s)" % language

    query = query0 + query1
    print("FIND MIN="+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)
    return df


# URL CALLBACK: DCC -> URL
component_ids = ['langcode', 'active_veryactive', 'year_yearmonth']


@dash.callback(Output('activity_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2]

    state = urlencode(dict(zip(component_ids, values)))
    print("UPDATE URL STATE UPDATE URL STATE UPDATE URL STATE UPDATE URL STATE")
    print(state)
    print("---------------------------")
    return '?'+state

# URL CALLBACK: URL -> DCC
# PARSE STATE FUNCTION (URL)


def parse_state(url):
    print("PARSE STATE PARSE STATE PARSE STATE PARSE STATE")
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    print(state)
    print("---------------------------")
    return state

# @activity_app.callback([Output(component_id='langcode', component_property='value'),
#                Output(component_id='active_veryactive', component_property='value'),
#                Output(component_id='year_yearmonth', component_property='value')],
#               inputs=[Input('url', 'href')])


def page_load(href):
    print("PAGE LOAD PAGE LOAD PAGE LOAD PAGE LOAD PAGE LOAD PAGE LOAD ")

    if not href:
        return []
    state = parse_state(href)
    print("LANGCODE="+state["langcode"])
    print("ACTIVE="+state["active_veryactive"])
    print("YEAR MONTH="+state["year_yearmonth"])
    print("---------------------------")
    return state["langcode"], state["active_veryactive"], state["year_yearmonth"]


@dash.callback([Output(component_id='activity_output_container1', component_property='children')],
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = parse_state(href)
    print(state["langcode"])
    print(state["active_veryactive"])
    print(state["year_yearmonth"])
    print("---------------------------")
    return state["langcode"]

# HIGHLIGHTS CALLBACK


@dash.callback([Output(component_id='activity_highlights_container', component_property='children'),
               Output(component_id='activity_highlights', component_property='children')],
              [Input(component_id='langcode', component_property='value'),
               Input(component_id='active_veryactive',
                     component_property='value'),
               Input(component_id='year_yearmonth', component_property='value')])
def highlights(language, user_type, time_type):

    print("HIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTS")

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    container = ""

    if user_type == '5':
        active = 'active'
    elif user_type == '100':
        active = 'very active'

    print("RICEVO LINGUE...")
    print(language)

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

    params = ""
    for x in langs:
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    if time_type == 'y':
        last5 = "('2021','2020','2019','2018','2017')"
        time = "years"
    elif time_type == 'ym':
        last5 = "('2021-12','2021-11','2021-10','2021-09','2021-08')"
        time = "months"

    h1 = ""
    h2 = ""
    h3 = ""

    if len(langs) == 0:
        h1 = ""
        h2 = ""
    elif len(langs) != 1:

        for x in langs:

            df1 = generate1("'"+x+"'", user_type, time_type, last5, conn)
            df2 = generate2("'"+x+"'", user_type, time_type, last5, conn)

            count = get_count(df1)
            media = get_media(df2)

            tail = generatetail(count, media, active, time)

            timespan = get_time(df1)
            date = timeconversion(timespan, time_type)

            h1 = h1+"* In **"+date+"**, in **" + \
                language_names_inv[x]+"** Wikipedia, the number of " + \
                    active+" editors was **"+str(count)+"**"+tail

            df3 = findMax2("'"+x+"'", user_type, time_type, conn)

            timemax = get_time(df3)
            datemax = timeconversion(timemax, time_type)

            temp = df3["max"].tolist()
            max_value = temp[0]

            h3 = h3+"* The **"+language_names_inv[x]+"** Wikipedia reached its maximum number of " + \
                active+" editors in **"+datemax+"** with **" + \
                    str(max_value)+"** "+active+" editors. \n"

        dfmax = findMax(params, user_type, time_type, timespan, conn)
        dfmin = findMin(params, user_type, time_type, timespan, conn)

        max0 = dfmax["langcode"].tolist()
        maxlang = max0[0]
        max1 = dfmax["max"].tolist()
        max = max1[0]

        min0 = dfmin["langcode"].tolist()
        minlang = min0[0]
        min1 = dfmin["min"].tolist()
        min = min1[0]

        h2 = "* **"+language_names_inv[str(maxlang)]+"** Wikipedia language edition has the largest number of "+active+" editors (**"+str(
            max)+"**), **"+language_names_inv[str(minlang)]+"** Wikipedia has the smallest (**"+str(min)+"**)."
    else:

        df1 = generate1(params, user_type, time_type, last5, conn)
        df2 = generate2(params, user_type, time_type, last5, conn)

        count = get_count(df1)
        media = get_media(df2)

        tail = generatetail(count, media, active, time)

        timespan = get_time(df1)
        date = timeconversion(timespan, time_type)

        h1 = "* In **"+date+"**, in **" + \
            str(language)+"** Wikipedia, the number of " + \
            active+" editors was **"+str(count)+"**"+tail

        df3 = findMax2(params, user_type, time_type, conn)

        timemax = get_time(df3)
        datemax = timeconversion(timemax, time_type)

        temp = df3["max"].tolist()
        max_value = temp[0]

        h3 = "* The **"+str(language)+"** Wikipedia reached its maximum number of "+active + \
            " editors in **"+datemax+"** with **" + \
            str(max_value)+"** "+active+" editors."

    print("---------------------------")

    return container, h1+"\n"+h2+"\n"+h3


@dash.callback(
    [Output(component_id='activity_output_container', component_property='children'),
     Output(component_id='activity_my_graph', component_property='figure')],
    [Input(component_id='langcode', component_property='value'),
     Input(component_id='active_veryactive', component_property='value'),
     Input(component_id='year_yearmonth', component_property='value')]
)
def update_graph(language, user_type, time_type):

    print("RICEVO LINGUE...")
    print(language)

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

    container = ""  # The langcode chosen was: {}".format(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    params = ""
    for x in langs:
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    query0 = "select * from vital_signs_metrics where topic = 'active_editors' and year_year_month = '" + \
        time_type+"' and m1_calculation = 'threshold' and m1_value = '"+user_type+"'"

    query1 = " and langcode IN (%s)" % params

    query = query0 + query1

    print("ACTIVITY QUERY = "+query)

    df = pd.read_sql_query(query, conn)

    df.reset_index(inplace=True)
    # print(df[:100])

    incipit = ''

    if user_type == '5':
        incipit = 'Active'
    else:
        incipit = 'Very Active'

    if time_type == 'y':
        time_text = 'Period of Time (Yearly)'
    else:
        time_text = 'Period of Time (Monthly)'

    fig = px.line(
        df,
        x='year_month',
        y='m1_count',
        color='langcode',
        title=incipit+' Users',
        labels={
            "m1_count": "Number of Monthly Active Editors (log)",
            "year_month": time_text,
            "langcode": "Projects",
        },
    )

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=6,
                         label="<b>6M</b>",
                         step="month",
                         stepmode="backward"),
                    dict(count=1,
                         label="<b>1Y</b>",
                         step="year",
                         stepmode="backward"),
                    dict(count=5,
                         label="<b>5Y</b>",
                         step="year",
                         stepmode="backward"),
                    dict(count=10,
                         label="<b>10Y</b>",
                         step="year",
                         stepmode="backward"),
                    dict(label="<b>ALL</b>",
                         step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    )

    # print("-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-")

    return container, fig
