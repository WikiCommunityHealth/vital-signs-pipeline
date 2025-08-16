# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__, path="/global")


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# global_app = Dash(__name__, server = server, url_base_pathname= webtype + "/global/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# global_app.config['suppress_callback_exceptions']=True

title = "Global Participation"+title_addenda
layout = html.Div([

    dcc.Location(id='global_url', refresh=False),


    html.H2(html.B('Global Participation'), style={
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

    html.Div(
        html.P('Select a Year'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Div(
        html.P('Select a Month'),
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

    html.Div(
        dcc.Dropdown(
            id='year',
            options=[{'label': k, 'value': k} for k in year_list],
            multi=False,
            value='2021',
            style={'width': '490px'}
        ), style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        dcc.Dropdown(
            id='month',
            options=[{'label': k, 'value': k} for k in month_list],
            multi=False,
            value='12',
            style={'width': '490px'}
        ), style={'display': 'inline-block', 'width': '500px'}),


    html.Div(id='global_output_container', children=[]),
    html.Br(),

    dcc.Graph(id='global_my_graph1', figure={}),
    html.Br(),
    dcc.Graph(id='global_my_graph2', figure={}),



])

# Callbacks

component_ids = ['langcode', 'active_veryactive',
                 'percentage_number', 'year_yearmonth', 'year', 'month']


@dash.callback(Output('global_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2], values[3], values[4], values[5],

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


@dash.callback(
    [Output(component_id='global_output_container', component_property='children'),
     Output(component_id='global_my_graph1', component_property='figure'),
     Output(component_id='global_my_graph2', component_property='figure')],
    [Input(component_id='langcode', component_property='value'),
     Input(component_id='active_veryactive', component_property='value'),
     Input(component_id='percentage_number', component_property='value'),
     Input(component_id='year_yearmonth', component_property='value'),
     Input(component_id='year', component_property='value'),
     Input(component_id='month', component_property='value'),]
)
def update_graph(language, user_type, value_type, time_type, year, month):

    container = ""  # The langcode chosen was: {}".format(language)

    engine = create_engine(database)

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
    temp = params[1]+params[2]

    query20 = "select * from vital_signs_metrics where topic = 'primary_editors' and year_year_month='" + \
        time_type+"' and m1_value='"+user_type+"' and langcode = m2_value "
    query21 = "and langcode IN ('%s')" % temp

    query2 = query20 + query21

    print("GLOBAL QUERY2 = "+query2)

    df2 = pd.read_sql_query(query2, engine)

    df2.reset_index(inplace=True)

    df2['perc'] = ((df2['m2_count']/df2['m1_count'])*100).round(2)

    if time_type == 'y':
        time_text = 'Year'
    else:
        time_text = 'Month of the Year'

    if user_type == '5':
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_bar(
        x=df2['year_month'],
        y=df2['m1_count'],
        name="Active Editors",
        marker_color='blue')

    fig.add_trace(
        go.Scatter(
            x=df2['year_month'],
            y=df2['perc'],
            name="Ratio",
            # mode='lines+markers',
            hovertemplate='%{y:.2f}%',
            marker_color='orange'),
        secondary_y=True)

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
        ),

        uniformtext_minsize=12
    )

    query03 = "SELECT * FROM vital_signs_metrics WHERE topic='primary_editors' AND year_year_month='" + \
        time_type+"' AND m1_value='"+user_type+"' "

    if time_type == 'ym':
        query13 = " AND year_month='"+year+"-"+month+"'"
    elif time_type == 'y':
        query13 = "AND year_month='"+year+"'"

    query23 = "AND langcode IN (%s)" % params
    query3 = query03 + query13 + query23

    df3 = pd.read_sql_query(query3, engine)

    df3.reset_index(inplace=True)

    print("GLOBAL QUERY3 = "+query3)

    df3['perc'] = ((df3['m2_count']/df3['m1_count'])*100).round(2)

    fig1 = px.bar(
        df3,
        orientation='h',
        y='langcode',
        x=value_type,
        text='m2_value',
        color='m2_value',
        labels={
            'm2_value': 'Languages',
            'm2_count': 'Number of Monthly Active Editors',
            'langcode': 'Languages',
        },
    )

    fig1.update_layout(uniformtext_minsize=8)
    # fig1.update_traces(texttemplate = 'Primary') #, hovertemplate=hover)
    fig1.update_yaxes(categoryorder="total ascending")

    return container, fig, fig1

###########################################################################
