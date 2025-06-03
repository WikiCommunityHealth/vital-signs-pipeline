# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__, path="/stability")


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# stability_app = Dash(__name__, server = server, url_base_pathname= webtype + "/stability/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# stability_app.config['suppress_callback_exceptions']=True

title = "Stability" + title_addenda
layout = html.Div([
    dcc.Location(id='stability_url', refresh=False),
    html.H2(html.B('Stability'), style={
            'textAlign': 'center', 'font-weight': 'bold'}),

    html.Div(
        html.P('Languages'),
        style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        html.P('Editors'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Div(
        html.P('Percentages or absolute numbers'),
        style={'display': 'inline-block', 'width': '250px'}),

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
            value='piedmontese (pms)',
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
        dcc.RadioItems(id='percentage_number',
                       options=[{'label': 'Percentage', 'value': 'perc'}, {
                           'label': 'Number', 'value': 'm2_count'}],
                       value='perc',
                       labelStyle={'display': 'inline-block',
                                   "margin": "0px 5px 0px 0px"},
                       style={'width': '250px'}
                       ), style={'display': 'inline-block', 'width': '250px'}),

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

    html.Div(id='stability_output_container', children=[]),
    html.Br(),

    dcc.Graph(id='stability_my_graph', figure={}),

    html.Div(id='stability_output_container1', children=[]),
    dcc.Markdown(id="temp", children=[], style={
                 "font-size": "18px"}, className='container'),

    html.Div(id='stability_highlights_container', children=[]),
    dcc.Markdown(id='stability_highlights', children=[], style={
                 "font-size": "18px"}, className='container')

], className="container")

# Callbacks


def get_avg_fresh(df):

    temp = df["fresh"].tolist()
    temp1 = temp[0]

    return temp1


def get_avg_long(df):

    temp = df["long"].tolist()
    temp1 = temp[0]

    return temp1


def calc_trend(num):

    res = ""

    if num < 33:
        res = "below"
    elif num > 33:
        res = "above"
    elif num == 33:
        res = "inline"

    return res


def generate1(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = '(m2_count/m1_count)*100'
    else:
        toreturn = 'm2_count'

    query0 = "select *, AVG("+toreturn+") AS fresh from vital_signs_metrics where topic = 'stability' AND year_year_month = '" + \
        tempo+"' AND year_month LIKE '20%' AND m1_value='"+attivi+"' and m2_value = '1'"
    query1 = " AND langcode IN (%s) " % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    # print("QUERY FOR DATAFRAME (HIGHLIGHTS)="+query)
    # print("---------------------------")

    return df1


def generate2(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = '(m2_count/m1_count)*100'
    else:
        toreturn = 'm2_count'

    query0 = "select *, AVG("+toreturn+") AS long from vital_signs_metrics where topic = 'stability' AND year_year_month = '" + \
        tempo+"' AND year_month LIKE '20%' AND m1_value='" + \
        attivi+"' and (m2_value = '13-24' OR m2_value='+24')"
    query1 = " AND langcode IN (%s) " % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    print("QUERY FOR DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df1


@dash.callback([Output(component_id='stability_highlights_container', component_property='children'),
               Output(component_id='stability_highlights', component_property='children')],
              [Input(component_id='langcode', component_property='value'),
               Input(component_id='active_veryactive',
                     component_property='value'),
               Input(component_id='percentage_number',
                     component_property='value'),
               Input(component_id='year_yearmonth', component_property='value')])
def highlights(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

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
    print("PARAMS")
    print(params)

    df1 = generate1(params, user_type, value_type, time_type, conn)
    avg_fresh = get_avg_fresh(df1)

    df2 = generate2(params, user_type, value_type, time_type, conn)
    avg_long = get_avg_long(df2)

    if value_type == 'perc':
        perc_or_num = '%'
        avg_fresh = round(avg_fresh, 2)
        avg_long = round(avg_long, 2)
    else:
        perc_or_num = ''
        avg_fresh = round(avg_fresh, 0)
        avg_long = round(avg_long, 0)

    trend = calc_trend(avg_long)

    h1 = "* On average, in the **"+str(language)+"** Wikipedia, the **fresh** editors are **"+str(avg_fresh)+""+perc_or_num+"**. A target of 30"+"%"+"-40"+"%" + \
        " of **fresh** editors may be desirable in order to have an influx of new energy and ideas. \nIf higher than this, and especially over 60%, it may be an indicator of a lack of capacity to engage and stabilize the community. High percentages of fresh editors are only desirable when the number of active editors is growing."

    h2 = "* On average, in the **"+str(language)+"** Wikipedia, the share of **long-term engaged** editors (including the bins of editing **13-24 months** in a row and **> 24 months** in a row) is **"+str(
        avg_long)+""+perc_or_num+"**. This is **"+trend+"** the target of a **33%** that we would recommend. This value is indicative of a solid community able to carry on with long-term Wikiprojects and activities."

    return "", h1+"\n\n"+h2


component_ids = ['langcode', 'active_veryactive',
                 'percentage_number', 'year_yearmonth']


@dash.callback(Output('stability_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2], values[3]

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


@dash.callback(
    [Output(component_id='stability_output_container', component_property='children'),
     Output(component_id='stability_my_graph', component_property='figure')],
    [Input(component_id='langcode', component_property='value'),
     Input(component_id='active_veryactive', component_property='value'),
     Input(component_id='percentage_number', component_property='value'),
     Input(component_id='year_yearmonth', component_property='value')]
)
def update_graph(language, user_type, value_type, time_type):

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

    container = ""  # "The langcode chosen was: {}".format(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    params = ""
    for x in langs:
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    query0 = "select * from vital_signs_metrics where topic = 'stability' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"'"

    query = query0 + " and langcode IN (%s)" % params

    print("STABILITY QUERY = "+query)

    df = pd.read_sql_query(query, conn)

    df.reset_index(inplace=True)
    # print(df[:100])

    df['perc'] = ((df['m2_count']/df['m1_count'])*100).round(2)

    if time_type == 'y':
        time_text = 'Period of Time (Yearly)'
    else:
        time_text = 'Period of Time (Monthly)'

    if value_type == 'perc':
        text_type = '%{y:.2f}%'
    else:
        text_type = ''

    fig = px.bar(
        df,
        x='year_month',
        y=value_type,
        color='m2_value',
        text=value_type,
        color_discrete_map={
            "1": "gray",
                 "2": "#00CC96",
            "3-6": "#FECB52",
            "7-12": "red",
            "13-24": "#E377C2",
            "24+": "#636EFA"},
        labels={
            "year_month": time_text,
            "perc": "Active Editors (%)",
            "m2_value": "Active Months in a row",
            "m2_count": "Active Editors (num)"
        }
    )

    fig.update_layout(font_size=12)

    fig.update_traces(texttemplate=text_type)
    # it also causes the text to float
    fig.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')

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

    return container, fig
