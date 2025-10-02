# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__, path="/special")

### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# special_app = Dash(__name__, server = server, url_base_pathname= webtype + "/special/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# special_app.config['suppress_callback_exceptions']=True

title = "Special Functions" + title_addenda
layout = html.Div([

    dcc.Location(id='special_url', refresh=False),
    html.H2(html.B('Special Functions'), style={
            'textAlign': 'center', 'font-weight': 'bold'}),

    html.Div(
        html.P('Select Languages'),
        style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        html.P('Show number of Active or Very Active users'),
        style={'display': 'inline-block', 'width': '200px'}),

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




    html.Div(id='special_output_container', children=[]),
    html.Br(),

    dcc.Graph(id='special_my_graph1', figure={}),
    html.Br(),
    dcc.Graph(id='special_my_graph2', figure={})


])

# Callbacks


component_ids = ['langcode', 'active_veryactive',
                 'percentage_number', 'year_yearmonth']


@dash.callback(Output('special_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2], values[3]

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


@dash.callback(
    [Output(component_id='special_output_container', component_property='children'),
     Output(component_id='special_my_graph1', component_property='figure'),
     Output(component_id='special_my_graph2', component_property='figure')],
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

    engine = create_engine(database)

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

    df = pd.read_sql_query(query1, engine)
    df2 = pd.read_sql_query(query2, engine)

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
        time_text = 'Year'
    else:
        time_text = 'Month of the Year'

    if user_type == '5':
        user_text = 'Active'
    else:
        user_text = 'Very Active'

    fig1 = px.bar(
        df,
        x='year_month',
        y=value_text,
        color='m2_value',
        text=value_text,
        title=user_text+' Technical Contributors',
        color_discrete_map={
            "2001-2005": "#636EFA",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={

            "year_month": time_text,
            "perc": "Editors",
            "m2_count": "Editors",
            "m2_value": "Lustrum First Edit"

        },
    )

    fig1.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig1.update_traces(texttemplate=text, hovertemplate=hover)

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

    fig2 = px.bar(
        df2,
        x='year_month',
        y=value_text,
        color='m2_value',
        text=value_text,
        title=user_text+' Project Coordinators',
        color_discrete_map={
            "2001-2005": "#636EFA",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356"},
        labels={
            "m1_count": "Editors",
            "year_month": time_text,
            "perc": "Editors",
            "m2_count": "Editors",
            "m2_value": "Lustrum First Edit"
        },
    )

    fig2.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig2.update_traces(texttemplate=text, hovertemplate=hover)

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
                visible=True
            ),
            type="date"
        )
    )

    return container, fig1, fig2
