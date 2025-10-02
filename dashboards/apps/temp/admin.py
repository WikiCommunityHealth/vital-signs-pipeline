# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__, path="/admin")


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# admin_app = Dash(__name__, server = server, url_base_pathname= webtype + "/admin/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# admin_app.config['suppress_callback_exceptions']=True

title = "Administrators"+title_addenda
layout = html.Div([

    dcc.Location(id='admin_url', refresh=False),


    html.H2(html.B('Administrators flag'), style={
            'textAlign': 'center', 'font-weight': 'bold'}),

    html.Div(
        html.P('Select Languages'),
        style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        html.P('Select Admin Type'),
        style={'display': 'inline-block', 'width': '400px'}),

    html.Div(
        html.P('Show Percentages or Numbers'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Div(
        html.P('Select Yearly or Monthly Time Aggregation'),
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
        dcc.Dropdown(
            id='admin',
            options=[{'label': k, 'value': k} for k in admin_type],
            multi=False,
            value='sysop',
            style={'width': '390px'}
        ), style={'display': 'inline-block', 'width': '400px'}),

    html.Div(
        dcc.RadioItems(id='percentage_number',
                       options=[{'label': 'Percentage', 'value': 'perc'}, {
                           'label': 'Number', 'value': 'num'}],
                       value='perc',
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

    html.Div(id='admin_output_container', children=[]),
    html.Br(),


    html.Div(children=[
        dcc.Graph(id="admin_my_graph", style={'display': 'inline-block'}),
        dcc.Graph(id="admin_my_subgraph", style={'display': 'inline-block'})
    ]),

    html.Br(),

    dcc.Graph(id="admin_my_graph2", style={'display': 'inline-block'}),



])


# Callback
component_ids = ['langcode', 'admin', 'percentage_number', 'year_yearmonth']


@dash.callback(Output('admin_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2], values[3]

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


@dash.callback(
    [Output(component_id='admin_output_container', component_property='children'),
     Output(component_id='admin_my_graph', component_property='figure'),
     Output(component_id='admin_my_subgraph', component_property='figure'),
     Output(component_id='admin_my_graph2', component_property='figure'),],
    [Input(component_id='langcode', component_property='value'),
     Input(component_id='admin', component_property='value'),
     Input(component_id='percentage_number', component_property='value'),
     Input(component_id='year_yearmonth', component_property='value')]
)
def update_graph(language, admin_type, value_type, time_type):

    if language == None:
        languages = []

    langs = []
    if type(language) != str:
        for x in language:
            langs.append(language_names[x])
    else:
        langs.append(language_names[language])

    container = ""  # "The langcode chosen was: {}".format(language)

    engine = create_engine(database)

    params = ""
    for x in langs:
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    # HOW MUCH ADMIN IN A YEAR
    query = "select count(m2_count) as count, m2_value,year_year_month,year_month from vital_signs_metrics where topic = 'flags'  and langcode In (%s) and m2_calculation = 'bin' and m2_value not null group by year_month, m2_value" % params

    query1 = "select count(m2_count) as count, m2_value from vital_signs_metrics where topic = 'flags'  and langcode In (%s) and m2_calculation = 'bin' and m2_value not null group by m2_value" % params

    print("ADMIN QUERY = "+query)
    print("ADMIN QUERY1 = "+query1)

    df = pd.read_sql_query(query, engine)
    df1 = pd.read_sql_query(query1, engine)

    df.reset_index(inplace=True)
    df1.reset_index(inplace=True)
    # print(df[:100])

    # df['perc']=((df['m2_count']/df['m1_count'])*100).round(2)
    # hover='%{y:.2f}%'

    if time_type == 'y':
        time_text = 'Year'
    else:
        time_text = 'Month of the Year'

    df1['x'] = ''

    fig1 = px.bar(
        df,
        x='year_month',
        y='count',
        color='m2_value',
        title='Admin Flags Granted (Years)',
        text='count',
        width=850,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "count": "Number of Admins",
            "year_month": "Years",
                     # "perc": user_text,
                     "m2_value": "Lustrum First Edit"

        },
    )

    fig2 = px.bar(
        df1,
        x='x',
        y='count',
        color='m2_value',
        title='Total Num. of Admins',
        text='count',
        width=400,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "count": "",
            "x": "",
                     # "perc": user_text,
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
                visible=True
            ),
            type="date"
        )
    )

    fig1.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig1.update_layout(legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01
    ))
    fig2.layout.update(showlegend=False)

    ###################################

    query03 = "select * from vital_signs_metrics where topic = 'flags' and year_year_month='" + \
        time_type+"' and m1='monthly_edits' and m2_value='"+admin_type+"'"

    query04 = " and langcode in (%s)" % params

    query3 = query03+query04

    print("SPECIAL FUNCTIONS QUERY3 = "+query3)
    df3 = pd.read_sql_query(query3, engine)
    df3.reset_index(inplace=True)

    df3['perc'] = ((df3['m2_count']/df3['m1_count'])*100).round(2)

    fig3 = px.bar(
        df3,
        x='year_month',
        y='m2_count',
        text='m2_count',
        width=1000,
        title="Admins Role "+admin_type,
        labels={
            'm2_count': "Number of Admins per Active Editors",
            'perc': "Number of Admins per Active Editors",
            "year_month": time_text
        }
    )

    # fig3.update_traces(texttemplate = "(%{y})")

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
                visible=True
            ),
            type="date"
        )
    )

    return container, fig1, fig2, fig3
