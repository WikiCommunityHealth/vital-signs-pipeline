# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *

dash.register_page(__name__, path="/balance")


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# balance_app = Dash(__name__, server = server, url_base_pathname= webtype + "/balance/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# balance_app.config['suppress_callback_exceptions']=True

title = "Balance"+title_addenda
layout = html.Div([

    dcc.Location(id='balance_url', refresh=False),


    html.H2(html.B('Balance'), style={
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
            value='italian (it)',
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


    # html.Hr(),

    html.Div(id='balance_output_container', children=[]),
    html.Br(),

    dcc.Graph(id='balance_my_graph', figure={}),

    html.Div(id='balance_output_container1', children=[]),
    dcc.Markdown(id="temp", children=[], style={
                 "font-size": "18px"}, className='container'),

    html.Div(id='balance_highlights_container', children=[]),
    dcc.Markdown(id='balance_highlights', children=[], style={
                 "font-size": "18px"}, className='container')



], className="container")

# Callbacks


def get_last_gen_val(df):

    temp = df["last_gen"].tolist()
    temp1 = temp[0]

    return temp1


def get_time(df, time_type):

    temp = df["year_month"].tolist()
    temp1 = temp[0]

    if time_type == 'ym':
        list2 = temp1.split()
        print(list2)
        list2 = str(list2)
        list2 = list2[:-1]
        list2 = list2[:-1]
        list2 = list2[:-1]
        list2 = list2[:-1]
        list2 = list2[:-1]
        list2 = list2[1:]
        list2 = list2[1:]
        temp1 = str(list2)

    return temp1


def get_gen(df):
    temp = df["m2_value"].tolist()
    temp1 = temp[0]

    return temp1


def generate1(lingua, attivi, valore, tempo, conn):

    if valore == 'perc':
        toreturn = 'ROUND(AVG((m2_count/m1_count)*100),2)'
    else:
        toreturn = 'AVG(m2_count)'

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

    print("QUERY FOR DATAFRAME (HIGHLIGHTS)="+query)
    print("---------------------------")

    return df


@dash.callback([Output(component_id='balance_highlights_container', component_property='children'),
               Output(component_id='balance_highlights', component_property='children')],
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

    if user_type == '5':
        active_type = 'active'
    else:
        active_type = 'very active'

    if value_type == 'perc':
        perc_or_num = '%'
    else:
        perc_or_num = ''

    df1 = generate1(params, user_type, value_type, time_type, conn)

    recent_year1 = get_time(df1, time_type)
    last_gen_value = get_last_gen_val(df1)

    last_gen = get_gen(df1)

    h1 = "* In **"+recent_year1+"**, in the **"+str(language)+"** Wikipedia, the **last generation ["+last_gen+"]** had a share of **"+str(
        last_gen_value)+""+perc_or_num+"** of the "+active_type+" editors. We believe a growing share of the last generation until occupying between **30-40%** may be reasonable for a language edition that is not in a growth phase. Larger when it is. We considered every generation to be 5 years (a lustrum), so, as a rule of thumb, we suggest that the last generation occupies from 10 to 40"+"%"+" depending on the years which have passed since its beginning (0-5)."

    # h2="In "+recent_year2+", in the "+str(language)+" Wikipedia, the share of the first generation [2001-2005] takes "+last_gen_value+""+perc_or_num+" of the "+active_type+" editors. This is "+trend+" a desirable target of **5-15%**. Although this generation might be at the end of their lifecycle and the growth may have occurred with the following generation (2006-2010). The share of every previous generation will inevitably **decrease** over time."

    return "", h1


component_ids = ['langcode', 'active_veryactive',
                 'percentage_number', 'year_yearmonth']


@dash.callback(Output('balance_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1], values[2], values[3]

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


@dash.callback(
    [Output(component_id='balance_output_container', component_property='children'),
     Output(component_id='balance_my_graph', component_property='figure')],
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
        time_text = 'Year'
    else:
        time_text = 'Month of the Year'

    if user_type == '5':
        user_text = 'Active Editors'
    else:
        user_text = 'Very Active Editors'

    if value_type == 'perc':
        text_type = '%{y:.2f}%'
    else:
        text_type = ''

    fig = px.bar(
        df,
        x='year_month',
        y=value_text,
        color='m2_value',
        text='perc',
        # facet_row=df['langcode'],
             color_discrete_map={
                 "2001-2005": "#3366CC",
                 "2006-2010": "#F58518",
                 "2011-2015": "#E45756",
                 "2016-2020": "#FC0080",
                 "2021-2025": "#1C8356"},
             labels={
                 "m1_count": "Active Editors",
                 "year_month": time_text,
                 "perc": user_text,
                 "m2_value": "Lustrum First Edit",
                 "m2_count": user_text,
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
                visible=True
            ),
            type="date"
        )
    )

    return container, fig
