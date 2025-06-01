# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from app import *


### DASH APP TEST IN LOCAL ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
# retention_app = Dash(__name__, server = server, url_base_pathname= webtype + "/retention/", external_stylesheets=external_stylesheets, external_scripts=external_scripts)
# retention_app.config['suppress_callback_exceptions']=True

dash.register_page(__name__,path="/retention")

title = "Retention" + title_addenda
layout = html.Div([
    dcc.Location(id='retention_url', refresh=False),

    html.H2(html.B('Retention'), style={
            'textAlign': 'center', 'font-weight': 'bold'}),

    html.Div(
        html.P('Languages'),
        style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        html.P('Retention Rate'),
        style={'display': 'inline-block', 'width': '200px'}),

    html.Br(),

    html.Div(
        dcc.Dropdown(
            id='langcode',
            options=[{'label': k, 'value': k}
                     for k in language_names_list],
            multi=False,
            value='italian (it)',
            style={'width': '490px'}
        ), style={'display': 'inline-block', 'width': '500px'}),

    html.Div(
        dcc.Dropdown(id='retention_rate',
                     options=[{'label': '24h', 'value': '24h'}, {'label': '30d', 'value': '30d'}, {
                         'label': '60d', 'value': '60d'}, {'label': '365d', 'value': '365d'}, {'label': '730d', 'value': '730d'}],
                     value='60d',
                     style={'width': '490px'}
                     ), style={'display': 'inline-block', 'width': '500px'}),



    # html.Hr(),

    html.Div(id='retention_output_container', children=[]),
    html.Br(),

    dcc.Graph(id='retention_my_graph', figure={}),

    html.Div(id='retention_highlights_container', children=[]),
    dcc.Markdown(id='retention_highlights', children=[], style={
                 "font-size": "18px"}, className='container')

], className="container")

# Callbacks

component_ids = ['langcode', 'retention_rate']


@dash.callback(Output('retention_url', 'search'),
              inputs=[Input(i, 'value') for i in component_ids])
def update_url_state(*values):

    values = values[0], values[1]

    state = urlencode(dict(zip(component_ids, values)))
    print("**********+")
    print(state)
    return '?'+state


def get_time(df):
    # l'ultimo tempo disponibile
    temp = df["year_month"].tolist()

    print("*********")
    print(temp)

    temp1 = temp[0]

    return temp1


def timeconversion(rawdate):

    # print("Received "+rawdate)

    year = ''
    lista = rawdate.split('-')
    year = lista[0]

    return year


def generate1(lingua, retention_rate, conn):

    query0 = "SELECT * FROM vital_signs_metrics WHERE topic = 'retention' AND m2_value='"+retention_rate+"'"
    query1 = " AND langcode IN (%s) ORDER BY year_month DESC LIMIT 1" % lingua

    query2 = query0+query1

    df1 = pd.read_sql_query(query2, conn)
    df1.reset_index(inplace=True)

    print("QUERY FOR DATAFRAME (HIGHLIGHTS)="+query2)
    print("---------------------------")

    return df1


def get_avg_retention(lingua, retention_rate, year, conn):

    query0 = "SELECT *, AVG(m2_count / m1_count)*100 AS retention_rate FROM vital_signs_metrics WHERE topic = 'retention' AND m1='first_edit' AND m2_value = '" + \
        retention_rate+"' AND year_month LIKE '"+str(year)+"-%'"
    query1 = " AND langcode IN (%s)" % lingua

    query_retention = query0 + query1

    df2 = pd.read_sql_query(query_retention, conn)
    df2.reset_index(inplace=True)

    print("QUERY FOR DATAFRAME RETENTION (HIGHLIGHTS)="+query_retention)

    temp = df2["retention_rate"]
    res = round(temp[0], 2)

    return str(res)


@dash.callback([Output(component_id='retention_highlights_container', component_property='children'),
               Output(component_id='retention_highlights', component_property='children')],
              [Input(component_id='langcode', component_property='value'),
               Input(component_id='retention_rate', component_property='value')])
def highlights(language, retention_rate):

    container = ""

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # params=""
    # for x in language:
    #     params+="'"
    #     params+=x
    #     params+="',"

    # params = params[:-1]

    params = "'"+language_names[language]+"'"

    df1 = generate1(params, retention_rate, conn)
    df1.reset_index(inplace=True)

    last_time = get_time(df1)
    date = timeconversion(last_time)

    old_date = int(date)-10

    old_retention_value = get_avg_retention(
        params, retention_rate, old_date, conn)
    current_retention_value = get_avg_retention(
        params, retention_rate, date, conn)

    return "", "* In **"+str(old_date)+"**, to **"+language+"** Wikipedia, the average retention rate was **"+old_retention_value+"%**, in **"+str(date)+"** it is **"+current_retention_value+"%**, this is **"+str(round(float(current_retention_value)-float(old_retention_value), 2))+"** difference."+"\n"+"We argue that a reasonable target would be a 3"+"%"+" retention rate to ensure there is renewal among editors, while it could be desirable to reach 5-7%. In general, communities should aim at reversing the declining trend in the retention rate."


@dash.callback(
    [Output(component_id='retention_output_container', component_property='children'),
     Output(component_id='retention_my_graph', component_property='figure')],
    [Input(component_id='langcode', component_property='value'),
     Input(component_id='retention_rate', component_property='value')]
)
def update_graph(language, retention_rate):

    # if language == None:
    #     languages = []

    # langs = []
    # if type(language) != str:
    #     for x in language: langs.append(language_names[x])
    # else:
    #     langs.append(language_names[language])

    container = ""  # "The langcode chosen was: {}".format(language)

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # params=""
    # for x in langs:
    #     params+="'"
    #     params+=x
    #     params+="',"

    # params = params[:-1]

    params = "'"+language_names[language]+"'"

    query01 = "select * from vital_signs_metrics where topic = 'retention' and year_year_month = 'ym' and m1 = 'register' and m2_value='"+retention_rate+"'"
    query02 = "select * from vital_signs_metrics where topic = 'retention' and year_year_month = 'ym' and m1 = 'first_edit' and m2_value='"+retention_rate+"'"

    query1 = query01+' and langcode IN (%s)' % params
    query2 = query02+' and langcode IN (%s)' % params

    print("RETENION QUERY 1 ="+query1)
    print("RETENION QUERY 2 = "+query2)

    df1 = pd.read_sql_query(query1, conn)

    df1.reset_index(inplace=True)
    # print(df[:100])

    df2 = pd.read_sql_query(query2, conn)

    df2.reset_index(inplace=True)

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add bar
    fig.add_bar(x=df1['year_month'], y=df1['m1_count'],
                name="Registered Editors",  marker_color='gray')

    # Add trace
    df2['retention'] = (df2['m2_count']/df2['m1_count'])*100
    fig.add_trace(go.Scatter(x=df2['year_month'], y=df2['retention'], name="Retention Rate",
                  hovertemplate='%{y:.2f}%', marker_color='orange'), secondary_y=True)

    # Add figure title
    if retention_rate != 24:
        fig.update_layout(
            title="Retention of Users who had their first edit after "+str(retention_rate)+" days")
    else:
        fig.update_layout(
            title="Retention of Users who had their first edit after 24 hours")

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

    return container, fig
