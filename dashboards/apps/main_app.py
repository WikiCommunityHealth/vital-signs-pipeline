# -*- coding: utf-8 -*-
import sys
import os
from datetime import datetime, timedelta
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))
from config import *


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


def enrich_dataframe(dataframe):
    datas = []
    for x in wikilanguagecodes:
        datas.append([x, language_names_inv[x]])

    df_created = pd.DataFrame(datas, columns=['m2_value', 'language_name'])

    res = pd.merge(dataframe, df_created, how='inner', on='m2_value')

    return res



title = "Vital Signs"+title_addenda

layout = html.Div([
    dcc.Location(id='url', refresh=False),
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


@dash.callback(Output('url', 'search'),
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

        
        # LAYOUT
        layout = html.Div([
            # html.Div(id='title_container', children=[]),
            # html.H3(id='title', children=[], style={'textAlign':'center',"fontSize":"18px",'fontStyle':'bold'}, className = 'container'),
            # html.Br(),
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
                    value='activity',
                    style={'width': '290px'}
                ), style={'display': 'inline-block', 'width': '290'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='langcode',
                    options=[{'label': k, 'value': k}
                             for k in language_names_list],
                    multi=True,
                    value='piedmontese (pms)',
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
                    multi=True,
                    value='sysop',
                    # style={'width': '390px'},
                    disabled=False,
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),

            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='retention_rate',
                    options=[{'label': '24 hours', 'value': '24h'}, {'label': '30 days', 'value': '30d'}, {
                        'label': '60 days', 'value': '60d'}, {'label': '365 days', 'value': '365d'}, {'label': '730 days', 'value': '730d'}],
                    multi=True,
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
                    value='piedmontese (pms)',

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
                ), style={'display': 'inline-block', 'width': '290px', 'padding': '0px 0px 0px 10px'}),


            html.Div(
                apply_default_value(params)(dcc.Dropdown)(
                    id='retention_rate',
                    options=[{'label': '24h', 'value': '24h'}, {'label': '30d', 'value': '30d'}, {
                        'label': '60d', 'value': '60d'}, {'label': '365d', 'value': '365d'}, {'label': '730d', 'value': '730d'}],
                    value='60d',
                    # style={'width': '490px'},
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
    # 1) Normalizza l'input lingua → lista di etichette UI
    if language is None or language == "":
        language_list = []
    elif isinstance(language, str):
        language_list = [s.strip() for s in language.split(",")]
    else:
        language_list = list(language)

    # 2) Mappa etichette UI → codici (es. "piedmontese (pms)" → "pms")
    langcodes = [language_names[x]
                 for x in language_list] if language_list else []

    # 3) Costruisci la query parametrica
    base_sql = """
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'active_editors'
          AND year_year_month = :time_type
          AND m1_calculation = 'threshold'
          AND m1_value = :user_type
          {lang_filter}
        ORDER BY year_month
    """
    lang_filter = "AND langcode IN :langcodes" if langcodes else ""
    stmt = text(base_sql.format(lang_filter=lang_filter))
    if langcodes:
        stmt = stmt.bindparams(bindparam("langcodes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if langcodes:
        params["langcodes"] = langcodes

    # 4) Esegui con pandas + SQLAlchemy (parametrico)
    df = pd.read_sql_query(stmt, engine, params=params)

    # 5) Aggiungi il nome leggibile della lingua senza merge
    #    (language_names_inv: "pms" -> "piedmontese (pms)")
    df["language_name"] = df["langcode"].map(language_names_inv)

    # 6) Testi per etichette
    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Period of Time (Yearly)" if time_type == "y" else "Period of Time (Monthly)"

    # 7) Grafico
    fig = px.line(
        df,
        x="year_month",
        y="m1_count",
        color="language_name",
        height=500,
        width=1200,
        title=f"{incipit} Users",
        labels={"m1_count": f"{incipit} Editors",
                "year_month": time_text, "language_name": "Projects"},
    )
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(buttons=[
                dict(count=6,  label="<b>6M</b>",
                     step="month", stepmode="backward"),
                dict(count=1,  label="<b>1Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=5,  label="<b>5Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=10, label="<b>10Y</b>",
                     step="year",  stepmode="backward"),
                dict(label="<b>ALL</b>", step="all")
            ]),
            rangeslider=dict(visible=True),
            type="date"
        )
    )

    return html.Div(dcc.Graph(id="my_graph", figure=fig))


# RETENTION GRAPH
##########################################################################################################################################################################################################


def retention_graph(lang: str, retention_rate: str, length: int):

    # 1) Normalizza lang in 'langcode'
    #    Se 'lang' è un'etichetta presente in language_names -> mappa a codice; altrimenti usa lang come già-codice.
    try:
        langcode = language_names[lang]
    except Exception:
        langcode = lang

    # 2) SQL parametrico
    base_sql = text("""
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'retention'
          AND year_year_month = 'ym'
          AND m1 = :m1
          AND m2_value = :retention_rate
          AND langcode = :langcode
        ORDER BY year_month
    """)

    params_common = {"retention_rate": retention_rate, "langcode": langcode}

    # 3) Query: registered editors (m1='register')
    df1 = pd.read_sql_query(base_sql, engine, params={
                            **params_common, "m1": "register"}).copy()
    df1.reset_index(inplace=True, drop=True)

    # 4) Query: first_edit (per la linea di retention) (m1='first_edit')
    df2 = pd.read_sql_query(base_sql, engine, params={
                            **params_common, "m1": "first_edit"}).copy()
    df2.reset_index(inplace=True, drop=True)

    # 5) Costruzione figura
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    height_value = 600 if length == 1 else 350

    # Bar: registered editors
    if not df1.empty:
        fig.add_bar(
            x=df1['year_month'],
            y=df1['m1_count'],
            name="Registered Editors",
            marker_color='gray'
        )

    # Line: retention rate = 100 * m2_count / m1_count (evita divisioni per zero)
    if not df2.empty:
        denom = df2['m1_count'].replace(0, pd.NA)
        df2['retention'] = (df2['m2_count'] / denom) * 100
        df2['retention'] = df2['retention'].fillna(0)

        fig.add_trace(
            go.Scatter(
                x=df2['year_month'],
                y=df2['retention'],
                name="Retention Rate",
                hovertemplate='%{y:.2f}%',
                marker_color='orange'
            ),
            secondary_y=True
        )

    # Titolo in base al retention_rate (come in originale)
    titles = {
        '24h': "24 hours",
        '30d': "30 days",
        '60d': "60 days",
        '365d': "365 days",
        '730d': "730 days",
    }
    rate_label = titles.get(retention_rate, retention_rate)
    fig.update_layout(
        title=f"Retention in {lang} Wikipedia. Editors who edited again after {rate_label} of their first edit")

    # Assi e layout
    # rimasto come originale
    fig.update_xaxes(title_text="Period of Time (Yearly)")
    fig.update_yaxes(title_text="Registered Editors", secondary_y=False)
    fig.update_yaxes(title_text="Retention Rate", secondary_y=True)

    fig.update_layout(
        autosize=False,
        width=1200,
        height=height_value,
        xaxis=dict(
            rangeselector=dict(buttons=list([
                dict(count=6,  label="<b>6M</b>",
                     step="month", stepmode="backward"),
                dict(count=1,  label="<b>1Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=5,  label="<b>5Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=10, label="<b>10Y</b>",
                     step="year",  stepmode="backward"),
                dict(label="<b>ALL</b>", step="all")
            ])),
            rangeslider=dict(visible=False),
            type="date"
        )
    )

    return dcc.Graph(id='my_graph', figure=fig)

# STABILITY GRAPH
##########################################################################################################################################################################################################


def stability_graph(language, user_type: str, value_type: str, time_type: str):
    # --- 1) Normalizza lingue in lista di langcode ---
    if language is None or language == "":
        ui_langs = []
    elif isinstance(language, str):
        ui_langs = [s.strip() for s in language.split(",")]
    else:
        ui_langs = list(language)

    # mappa etichette UI -> codici; se già codice, lascialo com'è
    langcodes = []
    for x in ui_langs:
        try:
            langcodes.append(language_names[x])
        except Exception:
            langcodes.append(x)
    # se nulla selezionato, NON filtriamo per lingua (mostra tutto)
    filter_lang = bool(langcodes)

    # --- 2) Query parametrica (con IN expanding quando serve) ---
    base_sql = f"""
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'stability'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :langcodes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("langcodes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params["langcodes"] = langcodes

    df = pd.read_sql_query(stmt, engine)

    if df.empty:
        # Grafico vuoto “gentile”
        return html.Div(dcc.Graph(
            id='my_graph',
            figure=px.bar(title="No data for selected filters")
        ))

    # --- 3) Colonne derivate & testi ---
    # perc = 100 * m2_count / m1_count (evita divisioni per zero)
    denom = df["m1_count"].replace(0, pd.NA)
    df["perc"] = ((df["m2_count"] / denom) * 100).round(2).fillna(0)

    # Mappa langcode -> nome leggibile (es. "pms" -> "piedmontese (pms)")
    df["language_name"] = df["langcode"].map(language_names_inv)

    time_text = "Period of Time (Yearly)" if time_type == "y" else "Period of Time (Monthly)"
    incipit = "Active" if user_type == "5" else "Very Active"
    text_template = "%{y:.2f}%" if value_type == "perc" else ""

    # Altezza: proporzionale al numero di lingue selezionate (se nessuna selezione, 1)
    n_langs = max(1, len(langcodes) if filter_lang else 1)
    height_value = 400 if n_langs == 1 else 230 * n_langs

    # --- 4) Grafico ---
    fig = px.bar(
        df,
        x="year_month",
        y=value_type,                 # 'perc' oppure 'm2_count'
        color="m2_value",             # bucket "1","2","3-6",...
        text=value_type,
        facet_row="language_name",    # una riga per lingua
        width=1200,
        height=height_value,
        color_discrete_map={
            "1": "gray",
            "2": "#00CC96",
            "3-6": "#FECB52",
            "7-12": "red",
            "13-24": "#E377C2",
            "24+": "#636EFA",
        },
        labels={
            "year_month": time_text,
            "perc": f"{incipit} Editors (%)",
            "m2_value": "Active Months in a row",
            "m2_count": f"{incipit} Editors",
            "language_name": "Language",
        },
        title=f"{incipit} users stability",
    )

    fig.update_layout(font_size=12, uniformtext_minsize=12,
                      uniformtext_mode="hide")
    fig.update_traces(texttemplate=text_template)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(buttons=[
                dict(count=6,  label="<b>6M</b>",
                     step="month", stepmode="backward"),
                dict(count=1,  label="<b>1Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=5,  label="<b>5Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=10, label="<b>10Y</b>",
                     step="year",  stepmode="backward"),
                dict(label="<b>ALL</b>", step="all"),
            ]),
            rangeslider=dict(visible=False),
            type="date",
        )
    )

    return html.Div(dcc.Graph(id="my_graph", figure=fig))

# BALANCE GRAPH
##########################################################################################################################################################################################################


def balance_graph(language, user_type: str, value_type: str, time_type: str):

    # --- 1) Normalizza lingue (accetta stringa CSV o lista) -> langcodes ---
    if language is None or language == "":
        ui_langs = []
    elif isinstance(language, str):
        ui_langs = [s.strip() for s in language.split(",")]
    else:
        ui_langs = list(language)

    langcodes = []
    for x in ui_langs:
        # se è un'etichetta UI, mappa a codice; se è già codice, lascialo
        try:
            langcodes.append(language_names[x])
        except Exception:
            langcodes.append(x)

    filter_lang = bool(langcodes)

    # --- 2) Query parametrica (IN expanding se servono lingue filtrate) ---
    base_sql = f"""
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'balance'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :langcodes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("langcodes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params["langcodes"] = langcodes

    df = pd.read_sql_query(stmt, engine)

    if df.empty:
        return html.Div(dcc.Graph(
            id="my_graph",
            figure=px.bar(title="No data for selected filters")
        ))

    # --- 3) Colonne derivate & testi ---
    # perc = 100 * m2_count / m1_count (evita divisioni per zero)
    denom = df["m1_count"].replace(0, pd.NA)
    df["perc"] = ((df["m2_count"] / denom) * 100).round(2).fillna(0)

    # lingua leggibile (es. "pms" -> "piedmontese (pms)")
    df["language_name"] = df["langcode"].map(language_names_inv)

    if value_type == "perc":
        value_y = "perc"
        text_template = "%{y:.2f}%"
    else:
        value_y = "m2_count"
        text_template = ""

    time_text = "Period of Time (Yearly)" if time_type == "y" else "Period of Time (Monthly)"
    if user_type == "5":
        incipit = "Active"
        user_text = "Active Editors"
    else:
        incipit = "Very active"
        user_text = "Very Active Editors"

    # altezza grafico in base al numero di lingue selezionate (almeno 1)
    n_langs = max(1, len(langcodes) if filter_lang else 1)
    height_value = 400 if n_langs == 1 else 230 * n_langs

    # --- 4) Grafico ---
    fig = px.bar(
        df,
        x="year_month",
        y=value_y,
        color="m2_value",
        text=value_type,               # coerente con il comportamento originale
        facet_row="language_name",
        width=1200,
        height=height_value,
        color_discrete_map={
            "2001-2005": "#3366CC",
            "2006-2010": "#F58518",
            "2011-2015": "#E45756",
            "2016-2020": "#FC0080",
            "2021-2025": "#1C8356",
        },
        labels={
            "year_month": time_text,
            "perc": f"{incipit} Editors (%)",
            "m2_value": "Lustrum First Edit",
            "m2_count": user_text,
            "language_name": "Language",
        },
        title=f"{incipit} users balance",
    )

    fig.update_layout(uniformtext_minsize=12, uniformtext_mode="hide")
    fig.update_traces(texttemplate=text_template)

    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(buttons=[
                dict(count=6,  label="<b>6M</b>",
                     step="month", stepmode="backward"),
                dict(count=1,  label="<b>1Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=5,  label="<b>5Y</b>",
                     step="year",  stepmode="backward"),
                dict(count=10, label="<b>10Y</b>",
                     step="year",  stepmode="backward"),
                dict(label="<b>ALL</b>", step="all"),
            ]),
            rangeslider=dict(visible=False),
            type="date",
        )
    )

    return html.Div(dcc.Graph(id="my_graph", figure=fig))

# SPECIALISTS GRAPH
##########################################################################################################################################################################################################


def special_graph(language, user_type: str, value_type: str, time_type: str):
    # --- 1) Normalizza lingue -> lista di langcode ---
    if language is None or language == "":
        ui_langs = []
    elif isinstance(language, str):
        ui_langs = [s.strip() for s in language.split(",")]
    else:
        ui_langs = list(language)

    langcodes = []
    for x in ui_langs:
        try:
            # etichetta UI -> codice
            langcodes.append(language_names[x])
        except Exception:
            # già codice
            langcodes.append(x)

    filter_lang = bool(langcodes)

    # --- 2) Query parametrica (riusabile per i due topic) ---
    base_sql = f"""
        SELECT *
        FROM vital_signs_metrics
        WHERE year_year_month = :time_type
          AND m1_value = :user_type
          AND topic = :topic
          {"AND langcode IN :langcodes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("langcodes", expanding=True))

    common_params = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        common_params["langcodes"] = langcodes

    # --- 3) Esegui le due query ---
    params_tech = {**common_params, "topic": "technical_editors"}
    params_coord = {**common_params, "topic": "coordinators"}

    df_tech = pd.read_sql_query(stmt, engine, params=params_tech)
    df_coord = pd.read_sql_query(stmt, engine, params=params_coord)

    # --- 4) Colonne derivate + mapping nome lingua ---
    for df in (df_tech, df_coord):
        if df.empty:
            continue
        denom = df["m1_count"].replace(0, pd.NA)
        df["perc"] = ((df["m2_count"] / denom) * 100).round(2).fillna(0)
        df["language_name"] = df["langcode"].map(language_names_inv)

    # --- 5) Etichette & layout comuni ---
    time_text = "Period of Time(Yearly)" if time_type == "y" else "Period of Time(Monthly)"
    user_text = "Active" if user_type == "5" else "Very Active"

    if value_type == "perc":
        value_y = "perc"
        text_template = "%{y:.2f}%"
    else:
        value_y = "m2_count"
        text_template = ""

    # Altezza: proporzionale al numero di lingue selezionate (almeno 1)
    n_langs = max(1, len(langcodes) if filter_lang else 1)
    height_value = 400 if n_langs == 1 else 230 * n_langs

    color_map = {
        "2001-2005": "#636EFA",
        "2006-2010": "#F58518",
        "2011-2015": "#E45756",
        "2016-2020": "#FC0080",
        "2021-2025": "#1C8356",
    }

    # --- 6) Costruisci i grafici ---
    if df_tech.empty:
        fig1 = px.bar(title=f"{user_text} Technical Contributors — No data")
    else:
        fig1 = px.bar(
            df_tech,
            x="year_month",
            y=value_y,
            color="m2_value",
            text=value_y,
            facet_row="language_name",
            width=1200,
            height=height_value,
            title=f"{user_text} Technical Contributors",
            color_discrete_map=color_map,
            labels={
                "language_name": "Language",
                "year_month": time_text,
                "perc": f"{user_text} editors (%)",
                "m2_count": f"{user_text} editors",
                "m2_value": "Lustrum First Edit",
            },
        )
        fig1.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")
        fig1.update_traces(texttemplate=text_template)
        fig1.update_layout(
            xaxis=dict(
                rangeselector=dict(buttons=[
                    dict(count=6,  label="<b>6M</b>",
                         step="month", stepmode="backward"),
                    dict(count=1,  label="<b>1Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=5,  label="<b>5Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=10, label="<b>10Y</b>",
                         step="year",  stepmode="backward"),
                    dict(label="<b>ALL</b>", step="all"),
                ]),
                rangeslider=dict(visible=False),
                type="date",
            )
        )

    if df_coord.empty:
        fig2 = px.bar(title=f"{user_text} Project Coordinators — No data")
    else:
        fig2 = px.bar(
            df_coord,
            x="year_month",
            y=value_y,
            color="m2_value",
            text=value_y,
            facet_row="language_name",
            height=height_value,
            title=f"{user_text} Project Coordinators",
            color_discrete_map=color_map,
            labels={
                "language_name": "Language",
                "m1_count": "Editors",
                "year_month": time_text,
                "perc": f"{user_text} editors (%)",
                "m2_count": f"{user_text} editors",
                "m2_value": "Lustrum First Edit",
            },
        )
        fig2.update_layout(uniformtext_minsize=12, uniformtext_mode="hide")
        fig2.update_traces(texttemplate=text_template)
        fig2.update_layout(
            xaxis=dict(
                rangeselector=dict(buttons=[
                    dict(count=6,  label="<b>6M</b>",
                         step="month", stepmode="backward"),
                    dict(count=1,  label="<b>1Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=5,  label="<b>5Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=10, label="<b>10Y</b>",
                         step="year",  stepmode="backward"),
                    dict(label="<b>ALL</b>", step="all"),
                ]),
                rangeslider=dict(visible=False),
                type="date",
            )
        )

    # --- 7) Output Dash ---
    return html.Div(
        children=[
            dcc.Graph(id="my_graph1", figure=fig1),

            html.H5("Highlights"),
            html.Div(id="highlights_container_additional", children=[]),

            dcc.Graph(id="my_graph2", figure=fig2),
        ]
    )


# ADMIN FLAG GRAPH
##########################################################################################################################################################################################################


def admin_graph(language, admin_type: str, time_type: str):
    # --- 1) Normalizza lingue -> lista di langcode ---
    if language is None or language == "":
        ui_langs = []
    elif isinstance(language, str):
        ui_langs = [s.strip() for s in language.split(",")]
    else:
        ui_langs = list(language)

    langcodes = []
    for x in ui_langs:
        try:
            langcodes.append(language_names[x])   # etichetta UI -> codice
        except Exception:
            langcodes.append(x)                   # già codice

    filter_lang = bool(langcodes)
    time_text = "Yearly" if time_type == "y" else "Monthly"

    # --- 2) Query 1: conteggio per mese e lustrum (per lingua) ---
    sql_q1 = f"""
        SELECT
            langcode,
            year_year_month,
            year_month,
            m2_value,
            SUM(m2_count) AS count
        FROM vital_signs_metrics
        WHERE topic = 'flags'
          AND m1 = 'granted_flag'
          AND m1_value = :admin_type
          AND m2_value IS NOT NULL
          {"AND langcode IN :langcodes" if filter_lang else ""}
        GROUP BY langcode, year_year_month, year_month, m2_value
        ORDER BY langcode, year_month, m2_value
    """
    stmt_q1 = text(sql_q1)
    if filter_lang:
        stmt_q1 = stmt_q1.bindparams(bindparam("langcodes", expanding=True))
    params_q1 = {"admin_type": admin_type}
    if filter_lang:
        params_q1["langcodes"] = langcodes

    df1 = pd.read_sql_query(stmt_q1, engine)

    # --- 3) Query 2: totale per lustrum (per lingua) ---
    sql_q2 = f"""
        SELECT
            langcode,
            m2_value,
            SUM(m2_count) AS count
        FROM vital_signs_metrics
        WHERE topic = 'flags'
          AND m1 = 'granted_flag'
          AND m1_value = :admin_type
          AND m2_value IS NOT NULL
          {"AND langcode IN :langcodes" if filter_lang else ""}
    GROUP BY langcode, m2_value
    ORDER BY langcode, m2_value
    """
    stmt_q2 = text(sql_q2)
    if filter_lang:
        stmt_q2 = stmt_q2.bindparams(bindparam("langcodes", expanding=True))
    params_q2 = {"admin_type": admin_type}
    if filter_lang:
        params_q2["langcodes"] = langcodes

    df2 = pd.read_sql_query(stmt_q2, engine)
    if not df2.empty:
        df2["x"] = ""  # per il bar compatto a colonne uniche

    # --- 4) Query 3: percentuale admin tra active editors (per mese/lingua) ---
    # Nota: mantengo il calcolo in SQL come in origine, ma parametricamente.
    sql_q3 = f"""
        SELECT
            *,
            ROUND((m2_count::numeric / NULLIF(m1_count,0)) * 100, 2) AS perc
        FROM vital_signs_metrics
        WHERE topic = 'flags'
          AND year_year_month = :time_type
          AND m2_value = :admin_type
          {"AND langcode IN :langcodes" if filter_lang else ""}
        ORDER BY langcode, year_month
    """
    stmt_q3 = text(sql_q3)
    if filter_lang:
        stmt_q3 = stmt_q3.bindparams(bindparam("langcodes", expanding=True))
    params_q3 = {"time_type": time_type, "admin_type": admin_type}
    if filter_lang:
        params_q3["langcodes"] = langcodes

    df3 = pd.read_sql_query(stmt_q3, engine)

    # --- 5) Mapping nome lingua & altezze ---
    def add_language_name(df):
        if df.empty:
            return df
        df["language_name"] = df["langcode"].map(language_names_inv)
        return df

    df1 = add_language_name(df1)
    df2 = add_language_name(df2)
    df3 = add_language_name(df3)

    n_langs = max(1, len(langcodes) if filter_lang else 1)
    height_value = 400 if n_langs == 1 else 230 * n_langs

    color_map = {
        "2001-2005": "#3366CC",
        "2006-2010": "#F58518",
        "2011-2015": "#E45756",
        "2016-2020": "#FC0080",
        "2021-2025": "#1C8356",
    }

    # --- 6) Figure 1: flags per mese e lustrum ---
    if df1.empty:
        fig1 = px.bar(title=f"[{admin_type}] flags granted — No data")
    else:
        fig1 = px.bar(
            df1,
            x="year_month",
            y="count",
            color="m2_value",
            text="count",
            facet_row="language_name",
            height=height_value,
            width=850,
            color_discrete_map=color_map,
            title=f"[{admin_type}] flags granted over the years by lustrum of first edit",
            labels={
                "language_name": "Language",
                "count": "Number of Admins",
                "year_month": f"Period of Time({time_text})",
                "m2_value": "Lustrum First Edit",
            },
        )
        fig1.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")
        fig1.update_layout(
            xaxis=dict(
                rangeselector=dict(buttons=[
                    dict(count=6,  label="<b>6M</b>",
                         step="month", stepmode="backward"),
                    dict(count=1,  label="<b>1Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=5,  label="<b>5Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=10, label="<b>10Y</b>",
                         step="year",  stepmode="backward"),
                    dict(label="<b>ALL</b>", step="all"),
                ]),
                rangeslider=dict(visible=False),
                type="date",
            )
        )

    # --- 7) Figure 2: totale per lustrum (per lingua) ---
    if df2.empty:
        fig2 = px.bar(
            title=f"Total Num. of [{admin_type}] — No data", width=300, height=height_value)
    else:
        fig2 = px.bar(
            df2,
            x="x",
            y="count",
            color="m2_value",
            text="count",
            facet_row="language_name",
            height=height_value,
            width=300,
            color_discrete_map=color_map,
            title=f"Total Num. of [{admin_type}]",
            labels={
                "count": "",
                "x": "",
                "language_name": "Language",
            },
        )
        fig2.layout.update(showlegend=False)

    # --- 8) Figure 3: percentuale admin tra active editors ---
    if df3.empty:
        fig3 = px.bar(title=f"Percentage of [{admin_type}] flags among active editors — No data",
                      width=1000, height=height_value)
    else:
        fig3 = px.bar(
            df3,
            x="year_month",
            y="perc",
            text="perc",
            facet_row="language_name",
            height=height_value,
            width=1000,
            title=f"Percentage of [{admin_type}] flags among active editors on a {time_text} basis",
            labels={
                "language_name": "Language",
                "m2_count": "Number of Admins per Active Editors",
                "perc": "Percentage",
                "year_month": f"Period of Time({time_text})",
            },
        )
        fig3.update_traces(marker_color="indigo", texttemplate="%{y}%")
        fig3.update_layout(uniformtext_minsize=12, uniformtext_mode="hide")
        fig3.update_layout(
            xaxis=dict(
                rangeselector=dict(buttons=[
                    dict(count=6,  label="<b>6M</b>",
                         step="month", stepmode="backward"),
                    dict(count=1,  label="<b>1Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=5,  label="<b>5Y</b>",
                         step="year",  stepmode="backward"),
                    dict(count=10, label="<b>10Y</b>",
                         step="year",  stepmode="backward"),
                    dict(label="<b>ALL</b>", step="all"),
                ]),
                rangeslider=dict(visible=False),
                type="date",
            )
        )

    # --- 9) Output ---
    return html.Div(children=[
        dcc.Graph(id="my_graph", figure=fig1, style={
                  'display': 'inline-block'}),
        dcc.Graph(id="my_subgraph", figure=fig2,
                  style={'display': 'inline-block'}),
        html.Hr(),
        html.H5('Highlights'),
        html.Div(id='highlights_container_additional', children=[]),
        dcc.Graph(id="my_graph2", figure=fig3),
    ])


# GLOBAL COMMUNITY GRAPH
##########################################################################################################################################################################################################

def global_graph(language, user_type: str, value_type: str, time_type: str, year: str, month: str):
    # --- 1) Normalizza lingue -> lista di langcode ---
    if language is None or language == "":
        ui_langs = []
    elif isinstance(language, str):
        ui_langs = [s.strip() for s in language.split(",")]
    else:
        ui_langs = list(language)

    langcodes = []
    for x in ui_langs:
        try:
            langcodes.append(language_names[x])   # etichetta UI -> codice
        except Exception:
            langcodes.append(x)                   # già codice

    filter_lang = bool(langcodes)

    # --- 2) Query 1: primary_editors filtrati per periodo/utente/(lingue) ---
    sql_q1 = f"""
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'primary_editors'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :langcodes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt_q1 = text(sql_q1)
    if filter_lang:
        stmt_q1 = stmt_q1.bindparams(bindparam("langcodes", expanding=True))

    params_q1 = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params_q1["langcodes"] = langcodes

    df1 = pd.read_sql_query(stmt_q1, engine)
    if df1.empty:
        # Grafico “gentile” se non ci sono dati
        empty_fig = px.bar(title="No data for selected filters")
        return html.Div(children=[
            dcc.Graph(id="my_graph", figure=empty_fig,
                      style={'display': 'inline-block'}),
            html.Hr(),
            html.H5('Highlights'),
            html.Div(id='highlights_container_additional', children=[]),
            dcc.Graph(id="my_graph2", figure=empty_fig)
        ])

    # --- 3) Derivate + mapping lingua ---
    denom = df1["m1_count"].replace(0, pd.NA)
    df1["perc"] = ((df1["m2_count"] / denom) * 100).round(2).fillna(0)
    # enrich + language names
    df1 = enrich_dataframe(df1)  # mantiene compatibilità con il tuo flusso
    # Se vuoi solo il nome leggibile: df1["language_name"] = df1["langcode"].map(language_names_inv)

    time_text = "(Yearly)" if time_type == "y" else "(Monthly)"
    user_text = "Active Editors" if user_type == "5" else "Very Active Editors"

    # Altezza grafico proporzionale al numero di lingue selezionate (almeno 1)
    n_langs = max(1, len(langcodes) if filter_lang else 1)
    height_value = 400 if n_langs == 1 else 270 * n_langs

    # --- 4) Figure 1: bar facet per language/primary language ---
    fig1 = px.bar(
        df1,
        y=value_type,                # 'perc' o 'm2_count' ecc.
        x='year_month',
        color='language_name',
        text=value_type,
        title=f"{user_text} by primary language",
        facet_row=df1['langcode'],   # coerente con codice originale
        height=height_value,
        labels={
            "language_name": "Language",
            "m1_count": "Active Editors",
            "year_month": f"Period of Time {time_text}",
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
            "pl": "#1C8356",
        },
    )
    fig1.update_layout(width=1300)

    # --- 5) Query 2: snapshot per treemap (meta, anno/mese scelti) ---
    #   L'originale fissava 'ym' e '2022-03' su 'meta'.
    ym_value = f"{year}-{month}"
    sql_q2 = text("""
        SELECT *
        FROM vital_signs_metrics
        WHERE topic = 'primary_editors'
          AND year_year_month = 'ym'
          AND year_month = :ym_value
          AND m1_value = :user_type
          AND langcode = 'meta'
        ORDER BY m2_value
    """)
    df2 = pd.read_sql_query(sql_q2, engine, params={
                            "ym_value": ym_value, "user_type": user_type})

    if df2.empty:
        fig2 = px.treemap(
            title=f"Meta-wiki {user_text} by primary language — No data for {ym_value}")
    else:
        denom2 = df2["m1_count"].replace(0, pd.NA)
        df2["perc"] = ((df2["m2_count"] / denom2) * 100).round(2).fillna(0)
        df2 = enrich_dataframe(df2)

        fig2 = go.Figure()
        fig2.add_trace(go.Treemap(
            parents=df2["langcode"],
            labels=df2["language_name"],
            values=df2["m2_count"],
            customdata=df2["perc"],
            text=df2["m2_value"],
            texttemplate="<b>%{label}</b><br>Percentage: %{customdata}%<br>Editors: %{value}<br>",
            hovertemplate='<b>%{label}</b><br>Percentage: %{customdata}%<br>Editors: %{value}<br>%{text}<br><extra></extra>',
        ))
        fig2.update_layout(
            width=1200, title_text=f"Meta-wiki {user_text} by primary language — {ym_value}")

    # --- 6) Output ---
    return html.Div(children=[
        dcc.Graph(id="my_graph", figure=fig1, style={
                  'display': 'inline-block'}),

        html.Hr(),
        html.H5('Highlights'),
        html.Div(id='highlights_container_additional', children=[]),

        dcc.Graph(id="my_graph2", figure=fig2),
    ])


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

    # normalizza sempre in lista
    if not language:
        language_list = []
    elif isinstance(language, str):
        language_list = [s.strip() for s in language.split(",")]
    else:
        language_list = list(language)

    # mappa nomi → codici, evitando KeyError
    langs = []
    for x in language_list:
        key = x
        if x in language_names:          # es. 'Ligurian (lij)'
            key = language_names[x]
        elif x in language_names_inv:     # già codice 'lij'
            pass
        elif x in languages.index:        # ulteriore safety
            pass
        else:
            continue
        langs.append(key)

    params = ""
    for x in langs:
        # count+=1
        params += "'"
        params += x
        params += "',"

    params = params[:-1]

    fig = ""

    if metric == 'activity':

        fig = activity_graph(language, user_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'retention':

        array = []
        res = html.Div(children=array)

        for x in language:
            # print(x)
            fig = retention_graph(x, retention_rate, len(langs))
            array.append(fig)

        return res, False, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y', 'disabled': True}, {'label': 'Monthly', 'value': 'ym', 'disabled': True}], [{'label': 'Active', 'value': '5', 'disabled': True}, {'label': 'Very Active', 'value': '100', 'disabled': True}]
    elif metric == 'stability':

        fig = stability_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y', 'disabled': True}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'balance':

        fig = balance_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'special':
        fig = special_graph(language, user_type, value_type, time_type)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]
    elif metric == 'admin':
        fig = admin_graph(language, admin_type, time_type)

        return fig, True, False, [{'label': 'Percentage', 'value': 'perc', 'disabled': True}, {'label': 'Number', 'value': 'm2_count', 'disabled': True}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5', 'disabled': True}, {'label': 'Very Active', 'value': '100', 'disabled': True}]
    elif metric == 'global':

        # Get current date and format for 2 months ago

        # Get 2 months ago year and month
        two_months_ago = (datetime.now().replace(day=1) -
                          timedelta(days=32)).replace(day=1)
        year = str(two_months_ago.year)
        month = str(two_months_ago.month).zfill(2)  # Ensure 2-digit format

        fig = global_graph(language, user_type, value_type,
                           time_type, year, month)

        return fig, True, True, [{'label': 'Percentage', 'value': 'perc', 'disabled': False}, {'label': 'Number', 'value': 'm2_count', 'disabled': False}], [{'label': 'Yearly', 'value': 'y'}, {'label': 'Monthly', 'value': 'ym'}], [{'label': 'Active', 'value': '5'}, {'label': 'Very Active', 'value': '100'}]

####################################################################################################################################################################################
####################################################################################################################################################################################
####################################################################################################################################################################################
