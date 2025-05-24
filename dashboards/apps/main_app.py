# -*- coding: utf-8 -*-
from plotly.subplots import make_subplots
from urllib.parse import urlparse, parse_qsl, urlencode
import urllib
from dash import Dash, dcc, html, Input, Output
import dash
import plotly.graph_objects as go
import plotly.express as px  # (version 4.7.0 or higher)
import pandas as pd
import time
import sqlite3
import re
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__,path="/main")

##### METHODS #####
# parse
def parse_state(url):
    parse_result = urlparse(url)
    params = parse_qsl(parse_result.query)
    state = dict(params)
    print(state)
    return state

# layout


def apply_default_value(params):
    def wrapper(func):
        def apply_value(*args, **kwargs):
            if 'id' in kwargs and kwargs['id'] in params:
                kwargs['value'] = params[kwargs['id']]
            return func(*args, **kwargs)
        return apply_value
    return wrapper


def save_dict_to_file(dic):
    f = open('databases/'+'dict.txt', 'w')
    f.write(str(dic))
    f.close()


def load_dict_from_file():
    f = open('databases/'+'dict.txt', 'r')
    data = f.read()
    f.close()
    return eval(data)


def enrich_dataframe(dataframe):
    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['m2_value', 'language_name'])

    res = pd.merge(dataframe, df_created, how='inner', on='m2_value')

    return res

### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# main_app = Dash("main_app", server = server, url_base_pathname= webtype + "/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# main_app.config['suppress_callback_exceptions']=True


title = "Vital Signs"+title_addenda

layout = html.Div([
    dcc.Location(id='main_url', refresh=False),
    html.Div(id='page-content'),
])

text_default = '''Vital Signs are 6 indicators related to the community capacity and function to grow or renew itself. In this page you can select a Wikipedia language edition and check its vital signs.'''

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

interface_row1 = html.Div([

    html.Div(
        [
            html.P(
                [
                    "Vital Sign",

                ]
            )],
        style={'display': 'inline-block', 'width': '300px'},
    ),


    html.Div(
        [
            html.P(
                [
                    "",
                    html.Span(
                        "Language",
                        id="tooltip-languages",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
            dbc.Tooltip(
                html.P("Select the language editions you want to see the results for the selected Vital Sign. You can select a single language edition if you want. Some highlights are generated for the first selected language edition.",
                       style={"width": "42rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                              'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
                       ),
                target="tooltip-languages",
                placement="bottom",
                style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
            )],
        style={'display': 'inline-block', 'width': '200px'},
    ),

])

interface_row2 = html.Div([

    html.Div(
        [
            html.P(
                [
                    html.Span(
                        "Editors",
                        id="tooltip-editors",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
        ],
        style={'display': 'inline-block', 'width': '300px'},
    ),

    dbc.Tooltip(
        html.P('Select “active editors” or “very active editors” to see editors who have done a minimum of 5 or 100 edits per month.',
               style={"width": "47rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                      'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
               ),
        target="tooltip-editors",
        placement="bottom",
        style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
    ),


    html.Div(
        [
            html.P(
                [
                    "",
                    html.Span(
                        "Flag",
                        id="tooltip-admin-flag",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
            dbc.Tooltip(
                html.P(
                    "Select the admin flag you want to see in the graphs.",
                    style={"width": "auto", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                           'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
                ),
                target="tooltip-admin-flag",
                placement="bottom",
                style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
            )],
        style={'display': 'inline-block', 'width': '290px'},
    ),


    html.Div(
        [
            html.P(
                [
                    "",
                    html.Span(
                        "Retention rate",
                        id="tooltip-retention-rate",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
            dbc.Tooltip(
                html.P(
                    "Select the survival period to compute the editor retention. The survival period is the time between the first edit and the second edit to consider that an editor has survived.",
                    style={"width": "42rem", 'font-size': 12, 'color': 'black',  'text-align': 'left',
                           'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
                ),
                target="tooltip-retention-rate",
                placement="bottom",
                style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
            )],
        style={'display': 'inline-block', 'width': '200px'},
    )
])

interface_row3 = html.Div([

    html.Div(
        [
            html.P(
                [
                    "Time ",
                    html.Span(
                        "aggregation",
                        id="tooltip-time-aggregation",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
        ],
        style={'display': 'inline-block', 'width': '300px'},
    ),

    dbc.Tooltip(
        html.P(' Select the time aggregation period: months or years.',
               style={"width": "47rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                      'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
               ),
        target="tooltip-time-aggregation",
        placement="bottom",
        style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
    ),


    html.Div(
        [
            html.P(
                [
                    "",
                    html.Span(
                        "Percentage or number (y-axis)",
                        id="tooltip-perc-num",
                        style={"textDecoration": "underline"},
                    ),
                ]
            ),
            dbc.Tooltip(
                html.P(
                    "Select whether you want to see absolute or percentage values in the y-axis.",
                    style={"width": "auto", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                           'backgroundColor': '#F7FBFE', 'padding': '12px 12px 12px 12px'}
                ),
                target="tooltip-perc-num",
                placement="bottom",
                style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
            )],
        style={'display': 'inline-block', 'width': '290px'},
    ),

])

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

# callback to update URL
component_ids = ['metric', 'langcode', 'active_veryactive',
                 'year_yearmonth', 'retention_rate', 'percentage_number', 'admin']


@dash.callback(Output('main_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):
    #    print (values)

    if not isinstance(values[1], str):
        values = values[0], ','.join(
            values[1]), values[2], values[3], values[4], values[5], values[6]

    state = urlencode(dict(zip(component_ids, values)))
    return '?'+state

# callback to update page layout


@dash.callback(Output('page-content', 'children'),
              inputs=[Input('url', 'href')])
def page_load(href):
    if not href:
        return []
    state = parse_state(href)
    return main_app_build_layout(state)

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################


def main_app_build_layout(params):

    functionstartTime = time.time()

    # 'source_lang','target_langs','topic','order_by','show_gaps','limit'
    if len(params) != 0:

        print(params)
        # RESULTS

        main_app.title = "Vital Signs"+title_addenda

        # LAYOUT
        layout = html.Div([
            # html.Div(id='title_container', children=[]),
            # html.H3(id='title', children=[], style={'textAlign':'center',"fontSize":"18px",'fontStyle':'bold'}, className = 'container'),
            # html.Br(),

            html.H3('Vital Signs', style={'textAlign': 'center'}),
            html.Br(),
            dcc.Markdown(
                text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE
            html.Br(),
            html.H5('Select the parameters'),

            interface_row1,

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='metric',
                    options=[{'label': k, 'value': k} for k in metrics],
                    multi=False,
                    value=params['metric'],
                    style={'width': '290px'}
                ), style={'display': 'inline-block', 'width': '290'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='langcode',
                    options=[{'label': k, 'value': k}
                             for k in language_names_list],
                    multi=True,
                    value='English (en)',
                    style={'width': '570px'}
                ), style={'display': 'inline-block', 'width': '570px', 'padding': '0px 0px 0px 10px'}),

            html.Br(),

            interface_row2,

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='active_veryactive',
                    options=[{'label': 'Active', 'value': '5'}, {
                        'label': 'Very Active', 'value': '100'}],
                    value='5',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'},
                ), style={'display': 'inline-block', 'width': '290px'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='admin',
                    options=[{'label': k, 'value': k} for k in admin_type],
                    multi=False,
                    value='sysop',
                    # style={'width': '390px'},
                    disabled=False,
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='retention_rate',
                    options=[{'label': '24 hours', 'value': '24h'}, {'label': '30 days', 'value': '30d'}, {
                        'label': '60 days', 'value': '60d'}, {'label': '365 days', 'value': '365d'}, {'label': '730 days', 'value': '730d'}],
                    value='60d',
                    # style={'width': '490px'},
                    disabled=False,
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Br(),

            interface_row3,

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='year_yearmonth',
                    options=[{'label': 'Yearly', 'value': 'y'},
                             {'label': 'Monthly', 'value': 'ym'}],
                    value='ym',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'},
                ), style={'display': 'inline-block', 'width': '290px'}),

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='percentage_number',
                    options=[{'label': 'Percentage', 'value': 'perc'}, {
                        'label': 'Number', 'value': 'm2_count'}],
                    value='perc',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'}
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Div(id='chosen-metric', children=[]),

            # here is the graph
            html.Div(id='output_container', children=[]),
            html.Br(),

            # dcc.Graph(id='my_graph', figure={}),
            html.Div(id='graph_container', children=[]),

            html.Div(id='page-content', children=[]),

            html.Hr(),

            html.H5('Highlights'),

            html.Div(id='highlights_container', children=[]),

            html.Div(id='highlights_container_additional',
                     children=[], style={'visibility': 'hidden'}),

        ], className="container")

    else:

        # PAGE 1: FIRST PAGE. NOTHING STARTED YET.
        layout = html.Div([
            
            html.H3('Vital Signs', style={'textAlign': 'center'}),
            # html.Div(id='title_container', children=[]),
            # html.H3(id='title', children=[], title='Activity', style={'textAlign':'center',"fontSize":"18px",'fontStyle':'bold'}, className = 'container'),
            html.Br(),
            dcc.Markdown(text_default.replace('  ', '')),

            # HERE GOES THE INTERFACE
            # LINE
            html.Br(),
            html.H5('Select the parameters'),

            interface_row1,

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='metric',
                    options=[{'label': k, 'value': k} for k in metrics],
                    multi=False,
                    value='Activity',
                    style={'width': '290px'}
                ), style={'display': 'inline-block', 'width': '290px'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='langcode',
                    options=[{'label': k, 'value': k}
                             for k in language_names_list],
                    multi=True,
                    value='English (en)',

                    style={'width': '570px'}
                ), style={'display': 'inline-block', 'width': '570px', 'padding': '0px 0px 0px 10px'}),

            html.Br(),

            interface_row2,

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='active_veryactive',
                    options=[{'label': 'Active', 'value': '5'}, {
                        'label': 'Very Active', 'value': '100'}],
                    value='5',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'}
                ), style={'display': 'inline-block', 'width': '290px'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='admin',
                    options=[{'label': k, 'value': k} for k in admin_type],
                    multi=False,
                    value='sysop',
                    # style={'width': '390px'},
                    disabled=True,
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),


            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='retention_rate',
                    options=[{'label': '24h', 'value': '24h'}, {'label': '30d', 'value': '30d'}, {
                        'label': '60d', 'value': '60d'}, {'label': '365d', 'value': '365d'}, {'label': '730d', 'value': '730d'}],
                    value='60d',
                    # style={'width': '490px'},
                    disabled=True,
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Br(),

            interface_row3,

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='year_yearmonth',
                    options=[{'label': 'Yearly', 'value': 'y'},
                             {'label': 'Monthly', 'value': 'ym'}],
                    value='ym',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'}
                ), style={'display': 'inline-block', 'width': '290px'}),

            html.Div(
                apply_default_value(params)(dcc.RadioItems)(
                    id='percentage_number',
                    options=[{'label': 'Percentage', 'value': 'perc'}, {
                        'label': 'Number', 'value': 'm2_count'}],
                    value='perc',
                    labelStyle={'display': 'inline-block',
                                "margin": "0px 5px 0px 0px"},
                    # style={'width': '200px'}
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Div(id='chosen-metric', children=[]),


            # here is the graph
            # html.Br(),

            # dcc.Graph(id='my_graph', figure={}),
            html.Div(id='graph_container', children=[]),

            html.Div(id='page-content', children=[]),

            html.Hr(),

            html.H5('Highlights'),

            html.Div(id='highlights_container', children=[]),

            html.Div(id='highlights_container_additional',
                     children=[], style={'visibility': 'hidden'}),


        ], className="container")

    return layout

##########################################################################################################################################################################################################
##########################################################################################################################################################################################################

# ACTIVITY GRAPH
##########################################################################################################################################################################################################


def activity_graph(language, user_type, time_type):

    if isinstance(language, str):
        language = language.split(',')

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

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

    print(df[:10])

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df = pd.merge(df, df_created, how='inner', on='langcode')

    print(df[:10])

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
        color=df['language_name'],
        height=500,
        width=1200,
        title=incipit+' Users',
        labels={
            "m1_count": incipit+" Editors ",
            "year_month": time_text,
            "language_name": "Projects",
        },
        # labels=language_names_inv
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

    res = html.Div(
        dcc.Graph(id='my_graph', figure=fig)
    )

    return res

# RETENTION GRAPH
##########################################################################################################################################################################################################


def retention_graph(lang, retention_rate, length):

    container = "The langcode chosen was: {}".format(lang)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # users that register theirselves
    query01 = "select * from vital_signs_metrics where topic = 'retention' and year_year_month = 'ym' and m1 = 'register' and m2_value='"+retention_rate+"'"
    # retention
    query02 = "select * from vital_signs_metrics where topic = 'retention' and year_year_month = 'ym' and m1 = 'first_edit' and m2_value='"+retention_rate+"'"

    query1 = query01+' and langcode IN (\'%s\')' % language_names[lang]
    query2 = query02+' and langcode IN (\'%s\')' % language_names[lang]

    # print("\033[0;31m FIRST RETENTION QUERY="+query1+"\033[0m")
    # print("\033[0;32m FIRST RETENTION QUERY="+query1+"\033[0m")

    df1 = pd.read_sql_query(query1, conn)

    df1.reset_index(inplace=True)
    # print(df[:100])

    df2 = pd.read_sql_query(query2, conn)

    df2.reset_index(inplace=True)

    # Create figure with secondary y-axis
    fig = make_subplots(
        specs=[[{"secondary_y": True}]])

    if length == 1:
        height_value = 600
    else:
        height_value = 350

    # Add bar
    fig.add_bar(
        x=df1['year_month'],
        y=df1['m1_count'],
        name="Registered Editors",
        marker_color='gray')

    fig.update_layout(
        autosize=False,
        width=1200,
        height=height_value
    )

    # Add trace
    df2['retention'] = (df2['m2_count']/df2['m1_count'])*100

    fig.add_trace(
        go.Scatter(
            x=df2['year_month'],
            y=df2['retention'],
            name="Retention Rate",
            hovertemplate='%{y:.2f}%',
            marker_color='orange'),
        secondary_y=True)

    # Add figure title
    if retention_rate == '24h':
        fig.update_layout(title="Retention in "+lang +
                          " Wikipedia. Editors who edited again after 24 hours of their first edit")
    elif retention_rate == '30d':
        fig.update_layout(title="Retention in "+lang +
                          " Wikipedia. Editors who edited again after 30 days of their first edit")
    elif retention_rate == '60d':
        fig.update_layout(title="Retention in "+lang +
                          " Wikipedia. Editors who edited again after 60 days of their first edit")
    elif retention_rate == '365d':
        fig.update_layout(title="Retention in "+lang +
                          " Wikipedia. Editors who edited again after 365 days of their first edit")
    elif retention_rate == '730d':
        fig.update_layout(title="Retention in "+lang +
                          " Wikipedia. Editors who edited again after 730 days of their first edit")

    # Set x-axis title
    fig.update_xaxes(title_text="Period of Time (Yearly)")

    # Set y-axes titles
    fig.update_yaxes(title_text="Registered Editors", secondary_y=False)
    fig.update_yaxes(title_text="Retention Rate", secondary_y=True)

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
                visible=False
            ),
            type="date"
        )
    )

    # res = html.Div(
    #     dcc.Graph(id='my_graph',figure=fig)
    # )

    res = dcc.Graph(id='my_graph', figure=fig)

    return res

# STABILITY GRAPH
##########################################################################################################################################################################################################


def stability_graph(language, user_type, value_type, time_type):

    if isinstance(language, str):
        language = language.split(',')

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

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

    if user_type == '5':
        incipit = 'Active'
    else:
        incipit = 'Very Active'

    if value_type == 'perc':
        text_type = '%{y:.2f}%'
    else:
        text_type = ''

    if len(language) == 1:
        height_value = 400
    else:
        height_value = 230*len(language)

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df = pd.merge(df, df_created, how='inner', on='langcode')

    fig = px.bar(
        df,
        x='year_month',
        y=value_type,
        color='m2_value',
        text=value_type,
        facet_row=df['language_name'],
        width=1200,
        height=height_value,
        color_discrete_map={
            "1": "gray",
                 "2": "#00CC96",
            "3-6": "#FECB52",
            "7-12": "red",
            "13-24": "#E377C2",
            "24+": "#636EFA"},
        labels={
            "year_month": time_text,
            "perc":  incipit+" Editors (%)",
            "m2_value": "Active Months in a row",
            "m2_count":  incipit+" Editors",
            "language_name": "Language"
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
                visible=False
            ),
            type="date"
        )
    )

    res = html.Div(
        dcc.Graph(id='my_graph', figure=fig)
    )

    return res

# BALANCE GRAPH
##########################################################################################################################################################################################################


def balance_graph(language, user_type, value_type, time_type):

    if isinstance(language, str):
        language = language.split(',')

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

    query0 = "select * from vital_signs_metrics where topic = 'balance' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"'"

    query = query0 + " and langcode IN (%s)" % params

    print("BALANCE QUERY = "+query)

    df = pd.read_sql_query(query, conn)

    df.reset_index(inplace=True)
    # print(df[:100])

    df['perc'] = ((df['m2_count']/df['m1_count'])*100).round(2)

    if value_type == 'perc':
        value_text = 'perc'
        hover = '%{y:.2f}%'
    else:
        value_text = 'm2_count'
        hover = ''

    if time_type == 'y':
        time_text = 'Period of Time (Yearly)'
    else:
        time_text = 'Period of Time (Monthly)'

    if user_type == '5':
        incipit = 'Active'
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'
        incipit = 'Very active'

    if value_type == 'perc':
        text_type = '%{y:.2f}%'
    else:
        text_type = ''

    if len(language) == 1:
        height_value = 400
    else:
        height_value = 230*len(language)

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df = pd.merge(df, df_created, how='inner', on='langcode')

    fig = px.bar(
        df,
        x='year_month',
        y=value_text,
        color='m2_value',
        text=value_type,
        facet_row=df['language_name'],
        width=1200,
        height=height_value,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "year_month": time_text,
            "perc":  incipit+" Editors (%)",
            "m2_value": "Lustrum First Edit",
            "m2_count": user_text,
            "language_name": "Language"
        },
    )

    fig.update_layout(uniformtext_minsize=12)
    fig.update_traces(texttemplate=text_type)  # , hovertemplate=hover)
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
                visible=False
            ),
            type="date"
        )
    )
    res = html.Div(
        dcc.Graph(id='my_graph', figure=fig)
    )

    return res

# SPECIALISTS GRAPH
##########################################################################################################################################################################################################


def special_graph(language, user_type, value_type, time_type):

    if isinstance(language, str):
        language = language.split(',')

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

    query01 = "select * from vital_signs_metrics where topic = 'technical_editors' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"'"

    query02 = "select * from vital_signs_metrics where topic = 'coordinators' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"'"

    query1 = query01 + " and langcode IN (%s)" % params

    query2 = query02 + " and langcode IN (%s)" % params

    print("SPECIAL FUNCTIONS QUERY1 = "+query1)
    print("SPECIAL FUNCTIONS QUERY2 = "+query2)

    df = pd.read_sql_query(query1, conn)
    df2 = pd.read_sql_query(query2, conn)

    df.reset_index(inplace=True)
    df2.reset_index(inplace=True)
    # print(df[:100])

    df['perc'] = ((df['m2_count']/df['m1_count'])*100).round(2)
    df2['perc'] = ((df2['m2_count']/df2['m1_count'])*100).round(2)

    if value_type == 'perc':
        value_text = 'perc'
        hover = '%{y:.2f}%'
        text = '%{y:.2f}%'
    else:
        value_text = 'm2_count'
        hover = ''
        text = ''

    if time_type == 'y':
        time_text = 'Period of Time(Yearly)'
    elif time_type == 'ym':
        time_text = 'Period of Time(Monthly)'

    if user_type == '5':
        user_text = 'Active'
    else:
        user_text = 'Very Active'

    if len(language) == 1:
        height_value = 400
    else:
        height_value = 230*len(language)

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df = pd.merge(df, df_created, how='inner', on='langcode')

    fig1 = px.bar(
        df,
        x='year_month',
        y=value_text,
        color='m2_value',
        text=value_text,
        facet_row=df['language_name'],
        width=1200,
        height=height_value,
        title=user_text+' Technical Contributors',
        color_discrete_map={
            "2001-2005": "#636EFA",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "language_name": "Language",
            "year_month": time_text,
            "perc": user_text+" editors (%)",
            "m2_count": user_text+" editors",
            "m2_value": "Lustrum First Edit"

        },
    )

    fig1.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig1.update_traces(texttemplate=text)  # , hovertemplate=hover)

    fig1.update_layout(
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
                visible=False
            ),
            type="date"
        )
    )

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df2 = pd.merge(df2, df_created, how='inner', on='langcode')

    fig2 = px.bar(
        df2,
        x='year_month',
        y=value_text,
        color='m2_value',
        text=value_text,
        facet_row=df2['language_name'],
        height=height_value,
        title=user_text+' Project Coordinators',
        color_discrete_map={
            "2001-2005": "#636EFA",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "language_name": "Language",
            "m1_count": "Editors",
            "year_month": time_text,
            "perc": user_text+" editors (%)",
            "m2_count": user_text+" editors",
            "m2_value": "Lustrum First Edit"
        },
    )

    fig2.update_layout(uniformtext_minsize=12)
    fig2.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')
    fig2.update_traces(texttemplate=text)  # , hovertemplate=hover)

    fig2.update_layout(
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
                visible=False
            ),
            type="date"
        )
    )

    res = html.Div(children=[
        dcc.Graph(id='my_graph1', figure=fig1),

        html.H5('Highlights'),

        html.Div(id='highlights_container_additional', children=[]),

        # html.Br(),
        dcc.Graph(id='my_graph2', figure=fig2),]
    )

    return res

# ADMIN FLAG GRAPH
##########################################################################################################################################################################################################


def admin_graph(language, admin_type, time_type):

    if isinstance(language, str):
        language = language.split(',')

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

    # container = "" #"The langcode chosen was: {}".format(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    params = ""
    for x in langs:
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    # HOW MUCH [flag] ADMIN IN A YEAR
    query10 = "select langcode, sum(m2_count) as count, m2_value,year_year_month,year_month from vital_signs_metrics where topic = 'flags' and langcode In (%s)" % params
    query11 = " and m1 = 'granted_flag' and m1_value = '"+admin_type + \
        "' and m2_value not null group by year_month, m2_value"

    query1 = query10+query11

    query20 = "select langcode, sum(m2_count) as count, m2_value from vital_signs_metrics where topic = 'flags' and langcode In (%s)" % params
    query21 = " and m1 = 'granted_flag' and m1_value = '" + \
        admin_type+"' and m2_value not null group by m2_value"

    query2 = query20 + query21

    print("ADMIN QUERY 1 = "+query1)
    print("ADMIN QUERY 2 = "+query2)

    df1 = pd.read_sql_query(query1, conn)
    df2 = pd.read_sql_query(query2, conn)

    df1.reset_index(inplace=True)
    df2.reset_index(inplace=True)
    # print(df[:100])

    # df['perc']=((df['m2_count']/df['m1_count'])*100).round(2)
    # hover='%{y:.2f}%'

    if time_type == 'y':
        time_text = 'Yearly'
    else:
        time_text = 'Monthly'

    df2['x'] = ''

    if len(language) == 1:
        height_value = 400
    else:
        height_value = 230*len(language)

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df1 = pd.merge(df1, df_created, how='inner', on='langcode')

    fig1 = px.bar(
        df1,
        x='year_month',
        y='count',
        color='m2_value',
        title='['+admin_type +
        '] flags granted over the years by lustrum of first edit',
        text='count',
        facet_row=df1['language_name'],
        height=height_value,
        width=850,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "language_name": "Language",
            "count": "Number of Admins",
                     "year_month": "Period of Time("+time_text+")",
                     # "perc": user_text,
                     "m2_value": "Lustrum First Edit"

        },
    )

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df2 = pd.merge(df2, df_created, how='inner', on='langcode')

    fig2 = px.bar(
        df2,
        x='x',
        y='count',
        color='m2_value',
        title='Total Num. of ['+admin_type+']',
        text='count',
        facet_row=df2['language_name'],
        height=height_value,
        width=300,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "count": "",
            "x": "",
                     "language_name": "Language",
        },

    )

    fig1.update_layout(
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
                visible=False
            ),
            type="date"
        )
    )

    fig1.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

    fig2.layout.update(showlegend=False)

    ###################################

    query30 = "select *, ROUND((m2_count/m1_count)*100,2) as perc from vital_signs_metrics where topic = 'flags' and year_year_month='" + \
        time_type+"' and m2_value='"+admin_type+"'"

    query31 = " and langcode in (%s)" % params

    query3 = query30+query31

    print("ADMIN QUERY3 = "+query3)
    df3 = pd.read_sql_query(query3, conn)
    df3.reset_index(inplace=True)

    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['langcode', 'language_name'])

    df3 = pd.merge(df3, df_created, how='inner', on='langcode')

    fig3 = px.bar(
        df3,
        x='year_month',
        y='perc',
        text='perc',
        facet_row=df3['language_name'],
        height=height_value,
        width=1000,
        title="Percentage of ["+admin_type +
        "] flags among active editors on a "+time_text+" basis",
        labels={
            "language_name": "Language",
            'm2_count': "Number of Admins per Active Editors",
            'perc': "Percentage",
            "year_month": "Period of Time("+time_text+")"
        }
    )

    # fig3.update_traces(texttemplate = "(%{y})")
    fig3.update_traces(marker_color='indigo')
    fig3.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')

    fig3.update_layout(
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
                visible=False
            ),
            type="date"
        )
    )

    fig3.update_traces(texttemplate="%{y}%")

    res = html.Div(
        children=[
            dcc.Graph(id="my_graph", figure=fig1, style={
                      'display': 'inline-block'}),
            dcc.Graph(id="my_subgraph", figure=fig2,
                      style={'display': 'inline-block'}),

            html.Hr(),

            html.H5('Highlights'),

            html.Div(id='highlights_container_additional', children=[]),

            dcc.Graph(id="my_graph2", figure=fig3),


        ])

    return res


# GLOBAL COMMUNITY GRAPH
##########################################################################################################################################################################################################

def global_graph(language, user_type, value_type, time_type, year, month):

    # container = "The langcode chosen was: {}".format(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    query10 = "select * from vital_signs_metrics where topic = 'primary_editors' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"' "
    query11 = "and langcode IN (%s) " % params
    query1 = query10 + query11

    print("[GLOBAL] FIRST QUERY = "+query1)

    df1 = pd.read_sql_query(query1, conn)
    df1.reset_index(inplace=True)

    df1['perc'] = ((df1['m2_count']/df1['m1_count'])*100).round(2)

    if time_type == 'y':
        time_text = '(Yearly)'
    else:
        time_text = '(Monthy)'

    if user_type == '5':
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'

    if len(language) == 1:
        height_value = 400
    else:
        height_value = 270*len(language)

    df1 = enrich_dataframe(df1)

    fig1 = px.bar(
        df1,
        y=value_type,
        x='year_month',
        color='language_name',
        text=value_type,
        title=user_text+" by primary language",
        facet_row=df1['langcode'],
        height=height_value,
        labels={
            "language_name": "Language",
            "m1_count": "Active Editors",
            "year_month": "Period of Time "+time_text,
            "perc": user_text,
            "m2_value": "Primary language",
            "m2_count": user_text,
            "langcode": "Language",
        },
        color_discrete_map={
            "de": "#FEAF16",
            "en": "#3366CC",
            "it": "#3283FE",
            "ru": "#FD3216",
            "pl": "#1C8356"},

    )

    fig1.update_layout(width=1300)

    query20 = "select * from vital_signs_metrics where topic = 'primary_editors' and year_year_month='ym' and year_month = '2022-03' and m1_value='"+user_type+"' "
    query21 = "and langcode = 'meta'"
    query2 = query20 + query21

    print("[GLOBAL] SECOND QUERY = "+query2)

    df2 = pd.read_sql_query(query2, conn)
    df2.reset_index(inplace=True)

    df2['perc'] = ((df2['m2_count']/df2['m1_count'])*100).round(2)

    df2 = enrich_dataframe(df2)

    fig2 = go.Figure()

    fig2.add_trace(go.Treemap(
        parents=df2["langcode"],
        labels=df2['language_name'],
        values=df2['m2_count'],
        customdata=df2['perc'],
        text=df2['m2_value'],
        texttemplate="<b>%{label} </b><br>Percentage: %{customdata}%<br>Editors: %{value}<br>",
        hovertemplate='<b>%{label} </b><br>Percentage: %{customdata}%<br>Editors: %{value}<br>%{text}<br><extra></extra>',
    ))

    fig2.update_layout(width=1200, title_text="Meta-wiki " +
                       user_text+" by primary language")

    res = html.Div(
        children=[
            dcc.Graph(id="my_graph", figure=fig1, style={
                      'display': 'inline-block'}),

            html.Hr(),

            html.H5('Highlights'),

            html.Div(id='highlights_container_additional', children=[]),

            dcc.Graph(id="my_graph2", figure=fig2),


        ])

    return res


@dash.callback([Output(component_id='graph_container', component_property='children'),
               Output(component_id='retention_rate',
                      component_property='disabled'),
               Output(component_id='admin', component_property='disabled'),
               Output(component_id='percentage_number',
                      component_property='options'),
               Output(component_id='year_yearmonth',
                      component_property='options'),
               Output(component_id='active_veryactive', component_property='options')],
              [Input(component_id='metric', component_property='value'),
               Input(component_id='langcode', component_property='value'),
               Input(component_id='active_veryactive',
                     component_property='value'),
               Input(component_id='year_yearmonth',
                     component_property='value'),
               Input(component_id='retention_rate',
                     component_property='value'),
               Input(component_id='percentage_number',
                     component_property='value'),
               Input(component_id='admin', component_property='value')])
def change_graph(metric, language, user_type, time_type, retention_rate, value_type, admin_type):

    if isinstance(language, str):
        language = language.split(',')

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
        # count+=1
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    fig = ""

    if metric == 'Activity':

        fig = activity_graph(language, user_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'Retention':

        array = []
        res = html.Div(children=array)

        for x in language:
            # print(x)
            fig = retention_graph(x, retention_rate, len(params))
            array.append(fig)

        return res, False, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y', 'disabled': True}, {'label': 'Monthly', 'value': 'ym', 'disabled': True}], [{'label': 'Active', 'value': '5', 'disabled': True}, {'label': 'Very Active', 'value': '100', 'disabled': True}]
    elif metric == 'Stability':

        fig = stability_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y', 'disabled': True}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'Balance':

        fig = balance_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'Specialists':
        fig = special_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'Administrators':
        fig = admin_graph(language, admin_type, time_type)

        return fig, True, False, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5', 'disabled': True}, {'label': 'Very Active', 'value': '100', 'disabled': True}]
    elif metric == 'Global':

        fig = global_graph(language, user_type, value_type,
                           time_type, '2021', '12')

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]

####################################################################################################################################################################################
####################################################################################################################################################################################
####################################################################################################################################################################################


def timeconversion(rawdate, time_type):

    if time_type == 'ym':

        year = ''
        month = ''

        # print("Raw date is: "+rawdate)

        lista = rawdate.split('-')

        # print(lista[0])
        # print(lista[1])

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
        # print("Date is:"+date)
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


def activity_generate1(lingua, attivi, tempo, last5, conn):

    query0 = "SELECT * FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value=" + \
        attivi+" AND year_year_month = '"+tempo+"'"
    query1 = " AND langcode IN (%s) ORDER BY year_month DESC LIMIT 1" % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    print("[ACTIVITY] QUERY FOR FIRST DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df1


def activity_generate2(lingua, attivi, tempo, last5, conn):

    query0 = "SELECT AVG(m1_count) AS Media FROM vital_signs_metrics WHERE year_month IN "+last5 + \
        " AND topic = 'active_editors' AND year_year_month = '" + \
        tempo+"'  AND m1_value='"+attivi+"'"
    query1 = " AND langcode IN (%s)" % lingua

    query2 = query0+query1

    df2 = pd.read_sql_query(query2, conn)

    df2.reset_index(inplace=True)

    print("[ACTIVITY] QUERY FOR AVERAGE ="+query2)
    print("---------------------------")

    return df2


def activity_generatetail(count, media, active, time):

    if (count > media):
        tail = ", which is **above** the median number (**"+str(
            media)+"**) of "+active+" editors of the five last **"+time+"**. \n"
    else:
        tail = ", which is **below** the median number (**"+str(
            media)+"**) of "+active+" editors of the five last **"+time+"**. \n"

    return tail


def activity_findMax(language, active, yearmonth, time, conn):

    query0 = "SELECT MAX(m1_count) as max,langcode FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value = '" + \
        active+"' AND year_year_month = '"+yearmonth+"' AND year_month='"+time+"'"
    query1 = " AND langcode IN (%s)" % language

    query = query0 + query1
    # print("FIND MAX="+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)
    return df


def activity_findMin(language, active, yearmonth, time, conn):

    query0 = "SELECT MIN(m1_count) as min,langcode FROM vital_signs_metrics WHERE topic = 'active_editors' AND m1_value = '" + \
        active+"' AND year_year_month = '"+yearmonth+"' AND year_month='"+time+"'"
    query1 = " AND langcode IN (%s)" % language

    query = query0 + query1
    # print("FIND MIN="+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)
    return df


def activity_highlights(language, user_type, time_type):

    print("HIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTSHIGHLIGHTS")

    print(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    if user_type == '5':
        active = 'active'
    elif user_type == '100':
        active = 'very active'

    if time_type == 'y':
        last5 = "('2021','2020','2019','2018','2017')"
        time = "years"
    elif time_type == 'ym':
        last5 = "('2021-12','2021-11','2021-10','2021-09','2021-08')"
        time = "months"

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:

        for x in langs:

            df1 = activity_generate1(
                "'"+x+"'", user_type, time_type, last5, conn)
            df2 = activity_generate2(
                "'"+x+"'", user_type, time_type, last5, conn)

            count = get_count(df1)
            media = get_media(df2)

            tail = activity_generatetail(count, media, active, time)

            timespan = get_time(df1)
            date = timeconversion(timespan, time_type)

            h1 = h1+"* In **"+date+"**, in **" + \
                language_names_full[x]+"** Wikipedia, the number of " + \
                    active+" editors was **"+str(count)+"**"+tail

        dfmax = activity_findMax(params, user_type, time_type, timespan, conn)
        dfmin = activity_findMin(params, user_type, time_type, timespan, conn)

        max0 = dfmax["langcode"].tolist()
        maxlang = max0[0]
        max1 = dfmax["max"].tolist()
        max = max1[0]

        min0 = dfmin["langcode"].tolist()
        minlang = min0[0]
        min1 = dfmin["min"].tolist()
        min = min1[0]

        h2 = "* **"+language_names_full[str(maxlang)]+"** Wikipedia language edition has the higher number of "+active+" editors (**"+str(
            max)+"**), **"+language_names_full[str(minlang)]+"** Wikipedia has the lower (**"+str(min)+"**)."
    else:

        df1 = activity_generate1(params, user_type, time_type, last5, conn)
        df2 = activity_generate2(params, user_type, time_type, last5, conn)

        count = get_count(df1)
        media = get_media(df2)

        tail = activity_generatetail(count, media, active, time)

        timespan = get_time(df1)
        date = timeconversion(timespan, time_type)

        h1 = "* In **"+date+"**, in **"+language_names_full[str(
            langs[0])]+"** Wikipedia, the number of "+active+" editors was **"+str(count)+"**"+tail
        h2 = ""

    # print("---------------------------")

    res = dcc.Markdown(id='highlights', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


def retention_timeconversion(rawdate):

    # print("Received "+rawdate)

    year = ''
    lista = rawdate.split('-')
    year = lista[0]

    return year


def retention_generate1(lingua, retention_rate, conn):

    query0 = "SELECT * FROM vital_signs_metrics WHERE topic = 'retention' AND m2_value='"+retention_rate+"'"
    query1 = " AND langcode IN (%s) ORDER BY year_month DESC LIMIT 1" % lingua

    query2 = query0+query1

    df1 = pd.read_sql_query(query2, conn)
    df1.reset_index(inplace=True)

    print("[RETENTION] QUERY FOR DATAFRAME (HIGHLIGHTS)="+query2)
    print("---------------------------")

    return df1


def get_avg_retention(lingua, retention_rate, year, conn):

    query0 = "SELECT *, AVG(m2_count / m1_count)*100 AS retention_rate FROM vital_signs_metrics WHERE topic = 'retention' AND m1='first_edit' AND m2_value = '" + \
        retention_rate+"' AND year_month LIKE '"+str(year)+"-%'"
    query1 = " AND langcode IN (%s)" % lingua

    query_retention = query0 + query1

    df2 = pd.read_sql_query(query_retention, conn)
    df2.reset_index(inplace=True)

    print("[RETENTION] QUERY FOR DATAFRAME RETENTION (HIGHLIGHTS)="+query_retention)

    temp = df2["retention_rate"]
    res = round(temp[0], 2)

    return str(res)


def retention_highlights(language, retention_rate):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:
        count = 0
        for x in langs:
            count += 1
            df1 = retention_generate1("'"+x+"'", retention_rate, conn)
            df1.reset_index(inplace=True)

            last_time = get_time(df1)
            date = retention_timeconversion(last_time)

            old_date = int(date)-10

            old_retention_value = get_avg_retention(
                "'"+x+"'", retention_rate, old_date, conn)
            current_retention_value = get_avg_retention(
                "'"+x+"'", retention_rate, date, conn)

            if count > 1:
                i = '\n \n * To'
                text_eventual = 'in the same periods'
            else:
                i = '* In'
                text_eventual = ''

            h1 = h1+"* In "+str(old_date)+" **"+language_names_full[x]+"** Wikipedia, the average retention rate was **"+old_retention_value+"%**, in **"+str(
                date)+"** it is **"+current_retention_value+"%**, "+text_eventual+" this is **"+str(round(float(current_retention_value)-float(old_retention_value), 2))+"** difference. \n \n"

        h1 = h1+"\n \n We argue that a reasonable target would be a 3"+"%" + \
            " retention rate to ensure there is renewal among editors, while it could be desirable to reach 5-7%. In general, communities should aim at **reversing** the declining trend in the retention rate."

    else:

        df1 = retention_generate1(params, retention_rate, conn)
        df1.reset_index(inplace=True)

        last_time = get_time(df1)
        date = retention_timeconversion(last_time)

        old_date = int(date)-10

        old_retention_value = get_avg_retention(
            params, retention_rate, old_date, conn)
        current_retention_value = get_avg_retention(
            params, retention_rate, date, conn)

        h1 = "* In **"+str(old_date)+"**, to **"+language_names_full[str(langs[0])]+"** Wikipedia, the average retention rate was **"+old_retention_value+"%**, in **"+str(date)+"** it is **"+current_retention_value+"%**, this is **"+str(round(float(current_retention_value)-float(
            old_retention_value), 2))+"** difference. \nWe argue that a reasonable target would be a 3"+"%"+" retention rate to ensure there is renewal among editors, while it could be desirable to reach 5-7%. In general, communities should aim at reversing the declining trend in the retention rate."

    res = dcc.Markdown(id='highlights', children=[h1], style={
                       "font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


def stability_get_avg_fresh(df):

    temp = df["fresh"].tolist()
    temp1 = temp[0]

    return temp1


def stability_get_avg_long(df):

    temp = df["long"].tolist()
    temp1 = temp[0]

    return temp1


def stability_calc_trend(num):

    res = ""

    if num < 33:
        res = "below"
    elif num > 33:
        res = "above"
    elif num == 33:
        res = "inline"

    return res


def stability_generate1(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = '(m2_count/m1_count)*100'
    else:
        toreturn = 'm2_count'

    query0 = "select *, AVG("+toreturn+") AS fresh from vital_signs_metrics where topic = 'stability' AND year_year_month = '" + \
        tempo+"' AND year_month LIKE '20%' AND m1_value='"+attivi+"' and m2_value = '1'"
    query1 = " AND langcode = %s " % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    print("[STABILITY] QUERY FOR DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df1


def stability_generate2(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = '(m2_count/m1_count)*100'
    else:
        toreturn = 'm2_count'

    query0 = "select *, AVG("+toreturn+") AS long from vital_signs_metrics where topic = 'stability' AND year_year_month = '" + \
        tempo+"' AND year_month LIKE '20%' AND m1_value='" + \
        attivi+"' and (m2_value = '13-24' OR m2_value='+24')"
    query1 = " AND langcode = %s " % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    print("[STABILITY] QUERY FOR DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df1


def stability_highlights(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:

        for x in langs:

            df1 = stability_generate1(
                "'"+x+"'", user_type, value_type, time_type, conn)
            avg_fresh = stability_get_avg_fresh(df1)

            df2 = stability_generate2(
                "'"+x+"'", user_type, value_type, time_type, conn)
            avg_long = stability_get_avg_long(df2)

            if value_type == 'perc':
                perc_or_num = '%'
                avg_fresh = round(avg_fresh, 2)
                avg_long = round(avg_long, 2)
            else:
                perc_or_num = ''
                avg_fresh = round(avg_fresh, 0)
                avg_long = round(avg_long, 0)

            trend = stability_calc_trend(avg_long)

            h1 = h1+"\n* On average, in the **" + \
                language_names_full[x]+"** Wikipedia, the **fresh** editors are **"+str(
                    avg_fresh)+""+perc_or_num+"**. "

            h2 = h2+"\n* On average, in the **"+language_names_full[x]+"** Wikipedia, the share of **long-term engaged** editors (including the bins of editing **13-24 months** in a row and **> 24 months** in a row) is **"+str(
                avg_long)+""+perc_or_num+"**. This is **"+trend+"** the target of a **33%** that we would recommend."

        h1 = h1+"\n \n A target of 30"+"%"+"-40"+"%"+" of **fresh** editors may be desirable in order to have an influx of new energy and ideas. \nIf higher than this, and especially over 60%, it may be an indicator of a lack of capacity to engage and stabilize the community. High percentages of fresh editors are only desirable when the number of active editors is growing."
        h2 = h2+"\n \n The target value is indicative of a solid community able to carry on with long-term Wikiprojects and activities."
    else:

        df1 = stability_generate1(
            params, user_type, value_type, time_type, conn)
        avg_fresh = stability_get_avg_fresh(df1)

        df2 = stability_generate2(
            params, user_type, value_type, time_type, conn)
        avg_long = stability_get_avg_long(df2)

        if value_type == 'perc':
            perc_or_num = '%'
            avg_fresh = round(avg_fresh, 2)
            avg_long = round(avg_long, 2)
        else:
            perc_or_num = ''
            avg_fresh = round(avg_fresh, 0)
            avg_long = round(avg_long, 0)

        trend = stability_calc_trend(avg_long)

        h1 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, the **fresh** editors are **"+str(avg_fresh)+""+perc_or_num+"**. A target of 30"+"%"+"-40"+"%" + \
            " of **fresh** editors may be desirable in order to have an influx of new energy and ideas. \nIf higher than this, and especially over 60%, it may be an indicator of a lack of capacity to engage and stabilize the community. High percentages of fresh editors are only desirable when the number of active editors is growing."

        h2 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, the share of **long-term engaged** editors (including the bins of editing **13-24 months** in a row and **> 24 months** in a row) is **"+str(
            avg_long)+""+perc_or_num+"**. This is **"+trend+"** the target of a **33%** that we would recommend. This value is indicative of a solid community able to carry on with long-term Wikiprojects and activities."

    res = dcc.Markdown(id='highlights', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res

################################################################################################################################################################


def balance_get_last_gen_val(df):

    temp = df["last_gen"].tolist()
    temp1 = temp[0]

    return temp1


def balance_get_first_gen_val(df):

    temp = df["first_gen"].tolist()
    temp1 = temp[0]

    return temp1


def balance_calc_trend(num):

    res = ""

    if num < 5:
        res = "below"
    elif num > 15:
        res = "above"
    elif num > 5 or num < 15:
        res = "inline with"

    return res


def balance_get_gen(df):
    temp = df["m2_value"].tolist()
    temp1 = temp[0]

    return temp1


def balance_generate1(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = 'ROUND((m2_count/m1_count)*100,2)'
    else:
        toreturn = 'm2_count'

    inner_query0 = "select distinct m2_value from vital_signs_metrics where topic = 'balance' and year_year_month='" + \
        tempo+"'  and m1_value='"+attivi+"'"
    inner_query1 = " and langcode = %s order by m2_value desc limit 1" % lingua

    inner_query = inner_query0 + inner_query1

    query0 = "select *, "+toreturn+" as last_gen from vital_signs_metrics where topic = 'balance' and year_year_month='" + \
        tempo+"' and m2_value= ( "+inner_query+" ) and m1_value='"+attivi+"'"
    query1 = " and langcode = %s order by year_month desc" % lingua

    query = query0 + query1

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    print("[BALANCE] QUERY FOR FIRST DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df


def balance_generate2(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = 'ROUND((m2_count/m1_count)*100,2)'
    else:
        toreturn = 'm2_count'

    inner_query0 = "select distinct m2_value from vital_signs_metrics where topic = 'balance' and year_year_month='" + \
        tempo+"'  and m1_value='"+attivi+"'"
    inner_query1 = " and langcode = %s order by m2_value desc limit 1" % lingua

    inner_query = inner_query0 + inner_query1

    query0 = "select *, "+toreturn+" as first_gen from vital_signs_metrics where topic = 'balance' and year_year_month='" + \
        tempo+"' and m2_value= '2001-2005' and m1_value='"+attivi+"'"
    query1 = " and langcode = %s order by year_month desc" % lingua

    query = query0 + query1

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    print("[BALANCE] QUERY FOR SECOND DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df


def balance_highlights(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    if user_type == '5':
        active_type = 'active'
    else:
        active_type = 'very active'

    if value_type == 'perc':
        perc_or_num = '%'
    else:
        perc_or_num = ''

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:
        for x in langs:
            df1 = balance_generate1(
                "'"+x+"'", user_type, value_type, time_type, conn)

            recent_year1 = get_time(df1)
            recent_year1 = timeconversion(recent_year1, time_type)
            last_gen_value = balance_get_last_gen_val(df1)
            last_gen = balance_get_gen(df1)

            h1 = h1+"\n* In **"+recent_year1+"**, in the **"+language_names_full[x]+"** Wikipedia, the **last generation ["+last_gen+"]** had a share of **"+str(
                last_gen_value)+""+perc_or_num+"** of the **"+active_type+"** editors. "

            df2 = balance_generate2(
                "'"+x+"'", user_type, value_type, time_type, conn)

            recent_year2 = get_time(df2)
            recent_year2 = timeconversion(recent_year2, time_type)
            first_gen_value = balance_get_first_gen_val(df2)

            trend = balance_calc_trend(first_gen_value)

            h2 = h2+"\n* In **"+recent_year2+"**, in **the "+language_names_full[x]+"** Wikipedia, the share of the **first generation [2001-2005]** takes **"+str(
                first_gen_value)+""+perc_or_num+"** of the **"+active_type+"** editors. This is **"+trend+"** a desirable target of **5-15%**."

        h1 = h1 + \
            "\n \n We believe a growing share of the last generation until occupying between **30-40%** may be reasonable for a language edition that is not in a growth phase. Larger when it is. We considered every generation to be 5 years (a lustrum), so, as a rule of thumb, we suggest that the last generation occupies from 10 to 40" + \
            "%" + \
            " depending on the years which have passed since its beginning (0-5)."
        h2 = h2 + \
            "\n \n Although this generation might be at the end of their lifecycle and the growth may have occurred with the following generation (2006-2010). The share of every previous generation will inevitably **decrease** over time."
    else:

        df1 = balance_generate1(params, user_type, value_type, time_type, conn)

        recent_year1 = get_time(df1)
        recent_year1 = timeconversion(recent_year1, time_type)
        last_gen_value = balance_get_last_gen_val(df1)
        last_gen = balance_get_gen(df1)

        h1 = "* In **"+recent_year1+"**, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, the **last generation ["+last_gen+"]** had a share of **"+str(last_gen_value)+""+perc_or_num+"** of the **"+active_type + \
            "** editors. We believe a growing share of the last generation until occupying between **30-40%** may be reasonable for a language edition that is not in a growth phase. Larger when it is. We considered every generation to be 5 years (a lustrum), so, as a rule of thumb, we suggest that the last generation occupies from 10 to 40" + \
            "%" + \
            " depending on the years which have passed since its beginning (0-5)."

        df2 = balance_generate2(params, user_type, value_type, time_type, conn)

        recent_year2 = get_time(df2)
        recent_year2 = timeconversion(recent_year2, time_type)
        first_gen_value = balance_get_first_gen_val(df2)

        trend = balance_calc_trend(first_gen_value)

        h2 = "* In **"+recent_year2+"**, in **the "+language_names_full[str(langs[0])]+"** Wikipedia, the share of the **first generation [2001-2005]** takes **"+str(first_gen_value)+""+perc_or_num+"** of the **"+active_type+"** editors. This is **"+trend + \
            "** a desirable target of **5-15%**. Although this generation might be at the end of their lifecycle and the growth may have occurred with the following generation (2006-2010). The share of every previous generation will inevitably **decrease** over time."

    res = dcc.Markdown(id='highlights', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


def special_calc_trend(num):

    res = ""

    if num < 20:
        res = "below"
    elif num > 20:
        res = "above"
    elif num == 20:
        res = "inline with"

    return res


def special_get_tech_editors(df):
    # il valore della media nell'ultimo tempo
    temp = df["m1_count"].tolist()
    value = temp[0]

    return value


def special_get_fresh_tech_editors(df):
    # il valore della media nell'ultimo tempo
    temp = df["percentage"].tolist()
    value = temp[0]

    return value


def special_generate1(topic, lingua, attivi, conn):

    query0 = "SELECT * FROM vital_signs_metrics WHERE topic = '" + \
        topic+"' AND m1_value="+attivi+" AND year_year_month = 'y'"
    query1 = " AND langcode IN (%s) ORDER BY year_month DESC LIMIT 1" % lingua

    query = query0+query1

    df1 = pd.read_sql_query(query, conn)

    df1.reset_index(inplace=True)

    print("[SPECIALISTS] QUERY FOR FIRST " +
          topic+" DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df1


def special_generate2(topic, lingua, attivi, tempo, conn):

    inner_query0 = "select distinct m2_value from vital_signs_metrics where topic = '" + \
        topic+"' and m1_value='"+attivi+"'"
    inner_query1 = " and langcode = %s order by m2_value desc limit 1" % lingua

    inner_query = inner_query0 + inner_query1

    query0 = "SELECT ROUND(AVG((m2_count/m1_count)*100),2) as percentage FROM vital_signs_metrics WHERE topic = '" + \
        topic+"' AND m1_value="+attivi + \
        " AND m2_value = ("+inner_query+") AND year_year_month = '"+tempo+"'"
    query1 = " AND langcode IN (%s)" % lingua

    query = query0+query1

    df2 = pd.read_sql_query(query, conn)

    df2.reset_index(inplace=True)

    print("[SPECIALISTS] QUERY FOR SECOND " +
          topic+" DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df2


def special_highlights1(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    if user_type == '5':
        active = 'active'
    elif user_type == '100':
        active = 'very active'

    if time_type == 'y':
        periodicity = 'yearly'
    elif time_type == 'ym':
        periodicity = 'monthly'

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:
        for x in langs:
            df1 = special_generate1(
                "technical_editors", "'"+x+"'", user_type, conn)
            tech_editors = special_get_tech_editors(df1)

            h1 = h1+"\n* In **"+get_time(df1)+"**, in the **"+language_names_full[x]+"** Wikipedia, there were **"+str(tech_editors)+"** "+active+" **technical** editors. This is **"+str(
                special_calc_trend(tech_editors))+"** the target of **20** that would be recommended to ensure the sustainability of the technical tasks."

            df2 = special_generate2(
                "technical_editors", "'"+x+"'", user_type, time_type, conn)
            fresh_tech_editors = special_get_fresh_tech_editors(df2)

            h2 = h2+"\n* On average, in the **"+language_names_full[x]+"** Wikipedia, the **last generation**  had a **"+periodicity+"** share of an **"+str(
                fresh_tech_editors)+""+"%"+"** of the **"+active+"** technical editors. This is **"+special_calc_trend(fresh_tech_editors)+"** the target of a **10-15% yearly** from the last generation that would be recommended."
    else:
        df1 = special_generate1("technical_editors", params, user_type, conn)
        tech_editors = special_get_tech_editors(df1)

        h1 = "* In **"+get_time(df1)+"**, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, there were **"+str(tech_editors)+"** "+active+" **technical** editors. This is **"+str(
            special_calc_trend(tech_editors))+"** the target of **20** that would be recommended to ensure the sustainability of the technical tasks."

        df2 = special_generate2("technical_editors",
                                params, user_type, time_type, conn)
        fresh_tech_editors = special_get_fresh_tech_editors(df2)

        h2 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, the **last generation**  had a **"+periodicity+"** share of an **"+str(
            fresh_tech_editors)+""+"%"+"** of the **"+active+"** technical editors. This is **"+special_calc_trend(fresh_tech_editors)+"** the target of a **10-15% yearly** from the last generation that would be recommended."

    res = dcc.Markdown(id='highlights1', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res


def special_highlights2(language, user_type, value_type, time_type):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    if user_type == '5':
        active = 'active'
    elif user_type == '100':
        active = 'very active'

    if time_type == 'y':
        periodicity = 'yearly'
    elif time_type == 'ym':
        periodicity = 'monthly'

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:
        for x in langs:
            df1 = special_generate1("coordinators", "'"+x+"'", user_type, conn)
            tech_editors = special_get_tech_editors(df1)

            h1 = h1+"\n* In **"+get_time(df1)+"**, in the **"+language_names_full[x]+"** Wikipedia, there were **"+str(tech_editors)+"** "+active+" **coordinators**. This is **"+str(
                special_calc_trend(tech_editors))+"** the target of **20** that would be recommended to ensure the sustainability of the technical tasks."

            df2 = special_generate2(
                "coordinators", "'"+x+"'", user_type, time_type, conn)
            fresh_tech_editors = special_get_fresh_tech_editors(df2)

            h2 = h2+"\n* On average, in the **"+language_names_full[x]+"** Wikipedia, the **last generation**  had a **"+periodicity+"** share of an **"+str(
                fresh_tech_editors)+""+"%"+"** of the **"+active+"** coordinators. This is **"+special_calc_trend(fresh_tech_editors)+"** the target of a **10-15% yearly** from the last generation that would be recommended."
    else:
        df1 = special_generate1("coordinators", params, user_type, conn)
        tech_editors = special_get_tech_editors(df1)

        h1 = "* In **"+get_time(df1)+"**, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, there were **"+str(tech_editors)+" "+active+"** coordinators. This is **"+str(
            special_calc_trend(tech_editors))+"** the target of **20** that would be recommended to ensure the sustainability of the technical tasks."

        df2 = special_generate2("coordinators", params,
                                user_type, time_type, conn)
        fresh_tech_editors = special_get_fresh_tech_editors(df2)

        h2 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, the **last generation**  had a **"+periodicity+"** share of an **"+str(
            fresh_tech_editors)+""+"%"+"** of the **"+active+"** coordinators. This is **"+special_calc_trend(fresh_tech_editors)+"** the target of a **10-15% yearly** from the last generation that would be recommended."

    res = dcc.Markdown(id='highlights2', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


def admin_calc_trend1(num):

    res = ""

    if num < 5:
        res = "below"
    elif num > 5:
        res = "above"
    else:
        res = "inline with"

    return res


def admin_calc_trend2(num):

    res = ""

    if num < 10:
        res = "below"
    elif num > 15:
        res = "above"
    else:
        res = "inline with"

    return res


def admin_calc_trend3(num):

    res = ""

    if num < 1:
        res = "below"
    elif num > 5:
        res = "above"
    else:
        res = "inline with"

    return res


def admin_get_value(df):
    temp = df["perc"].tolist()
    temp1 = temp[0]

    return temp1


def admin_get_avg(df):
    temp = df["avg"].tolist()
    temp1 = temp[0]

    return temp1


def admin_generate1(lingua, admin, conn):

    inner_query0 = "select sum(m2_count) as count from vital_signs_metrics where topic = 'flags' and m1 = 'granted_flag' and m1_value = '"+admin+"'"
    inner_query1 = " and langcode = %s group by year_month" % lingua

    inner_query = inner_query0 + inner_query1

    query = "select ROUND(AVG(count),1) as avg from ("+inner_query+")"

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    # print("QUERY FOR FIRST DATAFRAME (HIGHLIGHTS)="+query)
    # print("---------------------------")

    return df


def admin_generate2(lingua, tempo, admin, conn):

    query1 = "select *, ROUND((m2_count/m1_count)*100,2) as perc from vital_signs_metrics where topic = 'flags' and year_year_month='" + \
        tempo+"' and m2_value='"+admin+"' "
    query2 = "and langcode = %s order by year_month desc limit 1" % lingua

    query = query1+query2

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    print("QUERY FOR SECOND DATAFRAME (HIGHLIGHTS)="+query)
    # print("---------------------------")

    return df


def admin_find_last_flag(lingua, admin, conn):

    query = "select year_month from vital_signs_metrics where topic = 'flags' and m1 = 'granted_flag' and m1_value = '"+admin+"'"
    query1 = " and langcode IN  (%s) order by year_month desc limit 1" % lingua

    query = query + query1

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    temp = df["year_month"].tolist()
    temp1 = temp[0]

    # print("LOOK HERE")
    # print(temp1)

    return temp1


def admin_total_num_admin(lingua, conn):

    query20 = "select sum(m2_count) as tot from vital_signs_metrics where topic = 'flags' and m1 = 'granted_flag' "
    query21 = " and langcode In (%s)" % lingua  # and year_month = '2021'

    query2 = query20 + query21

    df2 = pd.read_sql_query(query2, conn)
    df2.reset_index(inplace=True)

    temp = df2["tot"].tolist()
    tot_admin = temp[0]

    # print("TOT ADMIN")
    # print(query2)

    return tot_admin


def admin_find_last_percentage(lingua, conn):

    query10 = "select sum(m2_count) as count, m2_value from vital_signs_metrics where topic = 'flags' and m1 = 'granted_flag'"
    query11 = " and langcode In (%s) group by m2_value order by m2_value desc limit 1" % lingua

    query1 = query10 + query11

    df1 = pd.read_sql_query(query1, conn)
    df1.reset_index(inplace=True)

    temp = df1["count"].tolist()
    num_admin = temp[0]

    # print("NUM ADMIN")
    # print(query1)

    tot_admin = admin_total_num_admin(lingua, conn)

    res = round((num_admin/tot_admin)*100, 2)

    # print("PERCENTAGE")
    # print(res)

    return res


def admin_highlights1(language, admin_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""
    h2 = ""

    if len(language) == 0:
        h1 = ""
        h2 = ""
    elif len(language) != 1:
        for x in langs:
            df1 = admin_generate1("'"+x+"'", admin_type, conn)
            avg = admin_get_avg(df1)

            last_flag_year = admin_find_last_flag("'"+x+"'", admin_type, conn)

            last_per = admin_find_last_percentage("'"+x+"'", conn)

            tot = admin_total_num_admin("'"+x+"'", conn)

            flag_percentage = (avg/tot)*100

            h1 = h1+"\n* On average, in the **"+language_names_full[x]+"** Wikipedia, there are **"+str(avg)+" ("+str(round(flag_percentage, 2))+")%** new **"+admin_type+"** flags given every year; the last **"+admin_type + \
                "** flag was granted in **"+last_flag_year+"** . This is **"+admin_calc_trend1(
                    flag_percentage)+"** the target of a renewal of **5"+"%"+"** of the total admins every year that would be recommended."

            h2 = h2+"\n* Among the **"+language_names_full[x]+"** Wikipedia’s admins, the last generation has a share of **"+str(
                last_per)+"%**. This is **"+admin_calc_trend2(last_per)+"** the target of **10-15%** yearly from the last generation that would be recommended. "

        h2 = h2+"\n \n Ideally, the group of admins should include members from all generations."
    else:
        df1 = admin_generate1(params, admin_type, conn)
        avg = admin_get_avg(df1)

        last_flag_year = admin_find_last_flag(params, admin_type, conn)

        last_per = admin_find_last_percentage(params, conn)

        tot = admin_total_num_admin(params, conn)

        flag_percentage = (avg/tot)*100

        h1 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, there are **"+str(avg)+" ("+str(round(flag_percentage, 2))+")%** new **"+admin_type+"** flags given every year; the last **" + \
            admin_type+"** flag was granted in **"+last_flag_year+"** . This is **"+admin_calc_trend1(
                flag_percentage)+"** the target of a renewal of **5"+"%"+"** of the total admins every year that would be recommended."

        h2 = "* Among the **"+language_names_full[str(langs[0])]+"** Wikipedia’s admins, the last generation has a share of **"+str(last_per)+"%**. This is **"+admin_calc_trend2(
            last_per)+"** the target of **10-15%** yearly from the last generation that would be recommended. Ideally, the group of admins should include members from all generations."

    res = dcc.Markdown(id='highlights2', children=[
                       h1+"\n"+h2], style={"font-size": "18px"}, className='container'),

    return res


def admin_highlights2(language, admin_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""

    if len(language) == 0:
        h1 = ""
    elif len(language) != 1:
        for x in langs:
            df = admin_generate2("'"+x+"'", time_type, admin_type, conn)

            last_time = get_time(df)
            last_date = timeconversion(last_time, time_type)

            proportion_value = admin_get_value(df)

            h1 = h1+"\n* In **"+str(last_date)+"**, the proportion of ["+admin_type+"] admins in the **"+language_names_full[x]+"** Wikipedia is **"+str(proportion_value)+"%**. This is **"+admin_calc_trend3(
                proportion_value)+"** the target of **1-5%** to guarantee that admins do not carry an excessive workload, since, in the end, they revise other editors’ edits."
    else:
        df = admin_generate2(params, time_type, admin_type, conn)

        last_time = get_time(df)
        last_date = timeconversion(last_time, time_type)

        proportion_value = admin_get_value(df)
        h1 = "* In **"+str(last_date)+"**, the proportion of ["+admin_type+"] admins in the **"+language_names_full[str(langs[0])]+"** Wikipedia is **"+str(proportion_value)+"%**. This is **"+admin_calc_trend3(
            proportion_value)+"** the target of **1-5%** to guarantee that admins do not carry an excessive workload, since, in the end, they revise other editors’ edits."

    res = dcc.Markdown(id='highlights2', children=[h1], style={
                       "font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


def get_value(df, toreturn):

    temp = df[toreturn].tolist()
    res = temp[0]

    return res


def global_calc_trend(num):

    if num < 55:
        res = "below"
    elif num == 55:
        res = "inline"
    elif num > 55:
        res = "above"

    return res


def global_generate1(language, user_type, value_type, time_type, conn):

    query0 = "select round(avg(m2_count),0) as nums, round(avg(m2_count/m1_count)*100,4) as percentage from vital_signs_metrics where topic = 'primary_editors' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"' and m2_value != langcode "
    query1 = "and langcode = %s" % language

    query = query0 + query1
    print("[GLOBAL] Highlights query = "+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    return df


def global_generate2(language, user_type, value_type, time_type, conn):

    query0 = "select avg(m2_count) as avg1, avg(m1_count) as avg2 from vital_signs_metrics where topic = 'primary_editors' and year_year_month='ym' and m1_value='" + \
        user_type+"' and langcode = 'meta' "
    query1 = "and m2_value = %s" % language

    query = query0 + query1
    print("[GLOBAL] Highlights second query = "+query)

    df = pd.read_sql_query(query, conn)
    df.reset_index(inplace=True)

    return df


def global_highlights1(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if user_type == '5':
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""

    if len(language) == 0:
        h1 = ""
    elif len(language) != 1:
        for x in langs:
            df = global_generate1("'"+x+"'", user_type,
                                  value_type, time_type, conn)

            num = get_value(df, "nums")

            perc = get_value(df, "percentage")

            h1 = h1+"\n* On average, in the **"+language_names_full[x]+"** Wikipedia, there are **"+str(num)+"** "+user_text+" editors **("+str(perc)+"%)** whose primary language is a different language edition. This is **"+global_calc_trend(
                perc)+"** the target of 55% to guarantee that there are dedicated editors whose main project is that Wikipedia language edition."

        h1 = h1+"\n\n  A percentage higher than 95% might imply that the community is not attracting collaborators from other communities. The rationale for both percentages is that we see both 50" + \
            "%"+" and 100"+"%"+" as extremes, and we suggest some margin around these values."
    else:
        df = global_generate1(params, user_type, value_type, time_type, conn)

        num = get_value(df, "nums")

        perc = get_value(df, "percentage")

        h1 = "* On average, in the **"+language_names_full[str(langs[0])]+"** Wikipedia, there are **"+str(num)+"** "+user_text+" editors **("+str(perc)+"%)** whose primary language is a different language edition. This is **"+global_calc_trend(
            perc)+"** the target of 55% to guarantee that there are dedicated editors whose main project is that Wikipedia language edition. A percentage higher than 95% might imply that the community is not attracting collaborators from other communities. The rationale for both percentages is that we see both 50"+"%"+" and 100"+"%"+" as extremes, and we suggest some margin around these values."

    res = dcc.Markdown(id='highlights1', children=[h1], style={
                       "font-size": "18px"}, className='container'),

    return res


def global_highlights2(language, user_type, value_type, time_type):

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    if user_type == '5':
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'

    if isinstance(language, str):
        language = language.split(',')

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

    h1 = ""

    if len(language) == 0:
        h1 = ""
    elif len(language) != 1:
        for x in langs:
            df = global_generate2("'"+x+"'", user_type,
                                  value_type, time_type, conn)

            avg1 = get_value(df, "avg1")
            avg2 = get_value(df, "avg2")

            res = round((avg1/avg2)*100, 2)
            print(res)

            h1 = h1+"\n* On average, the proportion between the number of "+user_text+" editors in **Meta-wiki** whose primary language belongs to **" + \
                language_names_full[x]+"** Wikipedia to the total number of active editors in " + \
                    language_names_full[x]+" Wikipedia is **"+str(res)+"% **"
    else:

        df = global_generate2(params, user_type, value_type, time_type, conn)

        avg1 = get_value(df, "avg1")
        avg2 = get_value(df, "avg2")

        res = round((avg1/avg2)*100, 2)
        print(res)

        h1 = "* On average, the proportion between the number of "+user_text+" editors in **Meta-wiki** whose primary language belongs to **" + \
            language_names_full[str(langs[0])]+"** Wikipedia to the total number of active editors in " + \
            language_names_full[str(langs[0])] + \
            " Wikipedia is **"+str(res)+"% **"

    res = dcc.Markdown(id='highlights2', children=[h1], style={
                       "font-size": "18px"}, className='container'),

    return res

##########################################################################################################################################################################


@dash.callback([Output(component_id='highlights_container', component_property='children'),
               Output(component_id='highlights_container_additional', component_property='children')],
              [Input(component_id='metric', component_property='value'),
               Input(component_id='langcode', component_property='value'),
               Input(component_id='active_veryactive',
                     component_property='value'),
               Input(component_id='year_yearmonth',
                     component_property='value'),
               Input(component_id='retention_rate',
                     component_property='value'),
               Input(component_id='percentage_number',
                     component_property='value'),
               Input(component_id='admin', component_property='value')])
def change_highlight(metric, language, user_type, time_type, retention_rate, value_type, admin_type):

    if metric == 'Activity':

        res = activity_highlights(
            language, user_type, time_type), dcc.Markdown("")

        return res
    elif metric == 'Retention':

        res = retention_highlights(language, retention_rate), dcc.Markdown("")
        return res
    elif metric == 'Stability':

        res = stability_highlights(
            language, user_type, value_type, time_type), dcc.Markdown("")

        return res
    elif metric == 'Balance':

        res = balance_highlights(
            language, user_type, value_type, time_type), dcc.Markdown("")

        return res
    elif metric == 'Specialists':

        res = special_highlights2(language, user_type, value_type, time_type), special_highlights1(
            language, user_type, value_type, time_type)

        return res
    elif metric == 'Administrators':

        res = admin_highlights2(language, admin_type, time_type), admin_highlights1(
            language, admin_type, time_type)

        return res
    else:

        res = global_highlights2(language, user_type, value_type, time_type), global_highlights1(
            language, user_type, value_type, time_type)

        return res
