# -*- coding: utf-8 -*-
from config import *
from flask import Flask, redirect, send_file, send_from_directory, abort

server = Flask(__name__)
app = Dash(__name__, server=server, external_stylesheets=external_stylesheets)
app.title = "Wikimedia Community Health Metrics"
app.config['suppress_callback_exceptions'] = True

# ---------- Utilità URL/state ----------


def parse_state(url: str) -> dict:
    if not url:
        return {}
    pr = urlparse(url)
    return dict(parse_qsl(pr.query))


# id di componenti "multi" per decodificare correttamente dai query param

MULTI_IDS = {"langcode"}


def apply_default_value(params: dict):
    """Injector: se un id è nei parametri URL, imposta .value di default.
       Per gli id in MULTI_IDS, converte 'a,b' -> ['a','b'].
    """
    def wrapper(Comp):
        def inner(*args, **kwargs):
            cid = kwargs.get("id")
            if cid and cid in params:
                raw = params[cid]
                if cid in MULTI_IDS:
                    kwargs["value"] = [s for s in raw.split(",") if s]
                else:
                    kwargs["value"] = raw
            return Comp(*args, **kwargs)
        return inner
    return wrapper


# ---------- UI (blocchi descrittivi come nel tuo main_app.py) ----------
text_default = (
    "Vital Signs are 6 indicators related to the community capacity and function "
    "to grow or renew itself. In this page you can select a Wikipedia language edition "
    "and check its vital signs."
)

interface_row1 = html.Div([
    html.Div([html.P(["Vital Sign"])], style={
             'display': 'inline-block', 'width': '300px'}),
    html.Div([
        html.P(["", html.Span("Language", id="tooltip-languages",
               style={"textDecoration": "underline"})]),
        dbc.Tooltip(
            html.P("Select the language editions you want to see the results for the selected Vital Sign. You can select a single language edition if you want. Some highlights are generated for the first selected language edition.",
                   style={"width": "42rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                          'backgroundColor': '#F7FBFE', 'padding': '12px'}),
            target="tooltip-languages", placement="bottom",
            style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
        ),
    ], style={'display': 'inline-block', 'width': '200px'}),
])

interface_row2 = html.Div([
    html.Div([html.P([html.Span("Editors", id="tooltip-editors", style={"textDecoration": "underline"})])],
             style={'display': 'inline-block', 'width': '300px'}),
    dbc.Tooltip(
        html.P('Select “active editors” or “very active editors” to see editors who have done a minimum of 5 or 100 edits per month.',
               style={"width": "47rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                      'backgroundColor': '#F7FBFE', 'padding': '12px'}),
        target="tooltip-editors", placement="bottom",
        style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
    ),
    html.Div([
        html.P(["", html.Span("Flag", id="tooltip-admin-flag",
               style={"textDecoration": "underline"})]),
        dbc.Tooltip(
            html.P("Select the admin flag you want to see in the graphs.",
                   style={"width": "auto", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                          'backgroundColor': '#F7FBFE', 'padding': '12px'}),
            target="tooltip-admin-flag", placement="bottom",
            style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
        ),
    ], style={'display': 'inline-block', 'width': '290px'}),
    html.Div([
        html.P(["", html.Span("Retention rate", id="tooltip-retention-rate",
               style={"textDecoration": "underline"})]),
        dbc.Tooltip(
            html.P("Select the survival period to compute the editor retention. The survival period is the time between the first edit and the second edit to consider that an editor has survived.",
                   style={"width": "42rem", 'font-size': 12, 'color': 'black',  'text-align': 'left',
                          'backgroundColor': '#F7FBFE', 'padding': '12px'}),
            target="tooltip-retention-rate", placement="bottom",
            style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
        ),
    ], style={'display': 'inline-block', 'width': '200px'}),
])

interface_row3 = html.Div([
    html.Div([html.P(["Time ", html.Span("aggregation", id="tooltip-time-aggregation",
                                         style={"textDecoration": "underline"})])],
             style={'display': 'inline-block', 'width': '300px'}),
    dbc.Tooltip(
        html.P(' Select the time aggregation period: months or years.',
               style={"width": "47rem", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                      'backgroundColor': '#F7FBFE', 'padding': '12px'}),
        target="tooltip-time-aggregation", placement="bottom",
        style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
    ),
    html.Div([
        html.P(["", html.Span("Percentage or number (y-axis)", id="tooltip-perc-num",
                              style={"textDecoration": "underline"})]),
        dbc.Tooltip(
            html.P("Select whether you want to see absolute or percentage values in the y-axis.",
                   style={"width": "auto", 'font-size': 12, 'color': 'black', 'text-align': 'left',
                          'backgroundColor': '#F7FBFE', 'padding': '12px'}),
            target="tooltip-perc-num", placement="bottom",
            style={'color': '#FFFFFF', 'backgroundColor': '#FFFFFF'},
        ),
    ], style={'display': 'inline-block', 'width': '290px'}),
])

# ---------- Builder del layout principale (ex main_app_build_layout) ----------


def main_app_build_layout(params: dict) -> html.Div:
    inject = apply_default_value(params)

    return html.Div([
        dcc.Markdown(text_default.replace('  ', '')),
        html.Br(),
        html.H5('Select the parameters'),

        interface_row1,

        html.Div(
            inject(dcc.Dropdown)(
                id='metric',
                options=[{'label': k, 'value': k} for k in metrics],
                value='activity', clearable=False, style={'width': '290px'}
            ),
            style={'display': 'inline-block', 'width': '290px'}
        ),

        html.Div(
            inject(dcc.Dropdown)(
                id='langcode',
                options=[{'label': k, 'value': k} for k in wikilanguagecodes],
                multi=True, value=['it'],
                style={'width': '570px'}
            ),
            style={'display': 'inline-block',
                   'width': '570px', 'padding': '0 0 0 10px'}
        ),

        html.Br(),
        interface_row2,

        html.Div(
            inject(dcc.RadioItems)(
                id='active_veryactive',
                options=[{'label': 'Active', 'value': '5'}, {
                    'label': 'Very Active', 'value': '100'}],
                value='5', labelStyle={'display': 'inline-block', "margin": "0 5px 0 0"},
            ),
            style={'display': 'inline-block', 'width': '290px'}
        ),

        html.Div(
            inject(dcc.Dropdown)(
                id='admin',
                options=[{'label': k, 'value': k} for k in admin_type],
                value='sysop', clearable=False,
            ),
            style={'display': 'inline-block',
                   'width': '290px', 'padding': '0 0 0 10px'}
        ),

        html.Div(
            inject(dcc.Dropdown)(
                id='retention_rate',
                options=[{'label': '24h', 'value': '24h'}, {'label': '30d', 'value': '30d'},
                         {'label': '60d', 'value': '60d'}, {
                             'label': '365d', 'value': '365d'},
                         {'label': '730d', 'value': '730d'}],
                value='60d', clearable=False,
            ),
            style={'display': 'inline-block',
                   'width': '290px', 'padding': '0 0 0 10px'}
        ),

        html.Br(),
        interface_row3,

        html.Div(
            inject(dcc.RadioItems)(
                id='year_yearmonth',
                options=[{'label': 'Yearly', 'value': 'y'},
                         {'label': 'Monthly', 'value': 'ym'}],
                value='ym', labelStyle={'display': 'inline-block', "margin": "0 5px 0 0"},
            ),
            style={'display': 'inline-block', 'width': '290px'}
        ),

        html.Div(
            inject(dcc.RadioItems)(
                id='percentage_number',
                options=[{'label': 'Percentage', 'value': 'perc'},
                         {'label': 'Number', 'value': 'm2_count'}],
                value='perc', labelStyle={'display': 'inline-block', "margin": "0 5px 0 0"},
            ),
            style={'display': 'inline-block',
                   'width': '290px', 'padding': '0 0 0 10px'}
        ),

        html.Div(id='chosen-metric'),
        html.Div(id='output_container'),
        html.Br(),
        html.Div(id='graph_container'),
        html.Div(id='highlights_container_additional',
                 style={'visibility': 'hidden'}),
    ], className="container")


# ---------- Layout globale + Router ----------
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div([html.H1(html.A("Vital Signs Dashboards", href="/"),
             style={'textAlign': 'center'})]),
    html.Div(id='content'),
    footbar
])



@app.callback(
    Output('content', 'children'),
    Input('url', 'pathname'),
    Input('url', 'href'),
)
def display_page(pathname, href):
    pathname = pathname or "/"
    params = parse_state(href) if href else {}
    
    if pathname in ("/data", "/architecture"):
        
        return dcc.Location(pathname=pathname, refresh=True)

    return main_app_build_layout(params)



# ---------- Sincronizzazione controlli <-> URL (con anti-loop) ----------
COMPONENT_IDS = ['metric', 'langcode', 'active_veryactive',
                 'year_yearmonth', 'retention_rate', 'percentage_number', 'admin']


@app.callback(Output('url', 'search'),
              [Input(i, 'value') for i in COMPONENT_IDS],
              State('url', 'search'))
def update_url_state(*vals_and_search):
    *values, current_search = vals_and_search

    data = {}
    for key, val in zip(COMPONENT_IDS, values):
        if isinstance(val, (list, tuple)):
            data[key] = ','.join(val)
        elif val is None:
            data[key] = ''
        else:
            data[key] = str(val)

    new_search = '?' + urlencode(data)
    if (current_search or '') == new_search:
        return no_update
    return new_search

# ---------- Callback grafico principale (usa le tue funzioni già rifattorizzate) ----------


def _opts_percentage(disabled=False):
    return [{'label': 'Percentage', 'value': 'perc', 'disabled': disabled},
            {'label': 'Number', 'value': 'm2_count', 'disabled': disabled}]


def _opts_yearmonth(disabled=False):
    return [{'label': 'Yearly', 'value': 'y', 'disabled': disabled},
            {'label': 'Monthly', 'value': 'ym', 'disabled': disabled}]


def _opts_active(disabled=False):
    return [{'label': 'Active', 'value': '5', 'disabled': disabled},
            {'label': 'Very Active', 'value': '100', 'disabled': disabled}]


@app.callback(
    [Output('graph_container', 'children'),
     Output('retention_rate', 'disabled'),
     Output('admin', 'disabled'),
     Output('percentage_number', 'options'),
     Output('year_yearmonth', 'options'),
     Output('active_veryactive', 'options')],
    [Input('metric', 'value'),
     Input('langcode', 'value'),
     Input('active_veryactive', 'value'),
     Input('year_yearmonth', 'value'),
     Input('retention_rate', 'value'),
     Input('percentage_number', 'value'),
     Input('admin', 'value')],
)
def change_graph(metric, language, user_type, time_type, retention_rate, value_type, admin_type):
    langs = [] if not language else list(language)
    nlangs = max(1, len(langs))

    try:
        if metric == 'activity':
            fig = activity_graph(langs, user_type, time_type)
            return (fig, True, True, _opts_percentage(True), _opts_yearmonth(False), _opts_active(False))
        elif metric == 'retention':
            children = [retention_graph(code, retention_rate, nlangs) for code in langs] or \
                       [html.Div("Select at least one language.",
                                 className="text-muted m-2")]
            return (html.Div(children), False, True, _opts_percentage(True), _opts_yearmonth(True), _opts_active(True))
        elif metric == 'stability':
            fig = stability_graph(langs, user_type, value_type, time_type)
            return (fig, True, True, _opts_percentage(False), _opts_yearmonth(False), _opts_active(False))
        elif metric == 'balance':
            fig = balance_graph(langs, user_type, value_type, time_type)
            return (fig, True, True, _opts_percentage(False), _opts_yearmonth(False), _opts_active(False))
        elif metric == 'special':
            fig = special_graph(langs, user_type, value_type, time_type)
            return (fig, True, True, _opts_percentage(False), _opts_yearmonth(False), _opts_active(False))
        elif metric == 'admin':
            fig = admin_graph(langs, admin_type, time_type)
            return (fig, True, False, _opts_percentage(True), _opts_yearmonth(False), _opts_active(True))
        elif metric == 'global':
            import datetime as _dt
            t2 = (_dt.datetime.now().replace(day=1) -
                  _dt.timedelta(days=32)).replace(day=1)
            year, month = str(t2.year), f"{t2.month:02d}"
            fig = global_graph(langs, user_type, value_type,
                               time_type, year, month)
            return (fig, True, True, _opts_percentage(False), _opts_yearmonth(False), _opts_active(False))

        # fallback
        return (html.Div("Select a Vital Sign."), True, True, _opts_percentage(False), _opts_yearmonth(False), _opts_active(False))

    except Exception as e:
        import traceback
        traceback.print_exc()
        return (
            html.Div(
                dbc.Alert(f"Errore nel grafico: {e}", color="danger", className="m-2")),
            True, True, [], [], []
        )


# ACTIVITY GRAPH
##########################################################################################################################################################################################################


def activity_graph(language, user_type, time_type):
    
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    
    base_sql = """
        SELECT langcode, year_month, m1_count
        FROM vital_signs_metrics
        WHERE topic = 'active_editors'
          AND year_year_month = :time_type
          AND m1_calculation = 'threshold'
          AND m1_value = :user_type
          {lang_filter}
        ORDER BY year_month
    """
    lang_filter = "AND langcode IN :codes" if codes else ""
    stmt = text(base_sql.format(lang_filter=lang_filter))
    if codes:
        stmt = stmt.bindparams(bindparam("codes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if codes:
        params["codes"] = codes

    df = pd.read_sql_query(stmt, engine, params=params)

    # caso vuoto
    if df.empty:
        fig = px.line(title="No data for current selection")
        return html.Div(dcc.Graph(id="activity_graph", figure=fig))

    #
    x_col = "year_month"
    xaxis_cfg = {}
    try:
        df["dt"] = pd.to_datetime(df["year_month"], format="%Y-%m")
        x_col = "dt"
        xaxis_cfg = dict(rangeslider=dict(visible=True), type="date")
    except Exception:
        pass

    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Yearly" if time_type == "y" else "Monthly"

    fig = px.line(
        df,
        x=x_col,
        y="m1_count",
        color="langcode",
        height=500,
        width=1200,
        title=f"{incipit} Users",
        labels={
            "m1_count": f"{incipit} Editors",
            x_col: f"Period ({time_text})",
            "langcode": "wiki",
        },
    )
    if xaxis_cfg:
        fig.update_layout(xaxis=xaxis_cfg)
    else:
        fig.update_layout(xaxis=dict(rangeslider=dict(visible=True)))

    return html.Div(dcc.Graph(id="my_graph", figure=fig))


# RETENTION GRAPH
##########################################################################################################################################################################################################


def retention_graph(langcode: str, retention_rate: str, nlangs: int):
    """
    langcode: codice wiki (es. "pms")
    retention_rate: "24h"| "7d" | "30d" | "60d"
    nlangs: numero di lingue selezionate (per scalare l'altezza del grafico)
    """

    code = (langcode or "").strip()

    # 1) Query unica e parametrica per entrambe le serie (register + first_edit)
    sql = text("""
        SELECT year_month, langcode, m1, m1_count, m2_count
        FROM vital_signs_metrics
        WHERE topic = 'retention'
          AND year_year_month = 'ym'
          AND m2_value = :retention_rate
          AND langcode = :code
          AND m1 IN :m1s
        ORDER BY year_month
    """).bindparams(bindparam("m1s", expanding=True))

    params = {
        "retention_rate": retention_rate,
        "code": code,
        "m1s": ["register", "first_edit"],
    }

    df = pd.read_sql_query(sql, engine, params=params)

    # 2) Stato vuoto
    if df.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.update_layout(title=f"Retention in {code} — no data")
        return html.Div(dcc.Graph(id=f"retention_graph_{code}", figure=fig))

    # 3) Prepara le due componenti
    reg = df[df["m1"] == "register"][["year_month", "m1_count"]].rename(
        columns={"m1_count": "registered"})
    fe = df[df["m1"] == "first_edit"][["year_month", "m1_count", "m2_count"]].rename(
        columns={"m1_count": "fe_m1", "m2_count": "fe_m2"})
    merged = pd.merge(reg, fe, on="year_month",
                      how="outer").sort_values("year_month")

    # Retention = 100 * (first_edit.m2_count / first_edit.m1_count)
    denom = merged["fe_m1"].replace(0, pd.NA)
    merged["retention"] = (merged["fe_m2"] / denom) * 100
    merged["retention"] = merged["retention"].fillna(0)

    # 4) Asse X in datetime se possibile
    x = "year_month"
    xaxis_type = "linear"
    try:
        merged["dt"] = pd.to_datetime(merged["year_month"], format="%Y-%m")
        x = "dt"
        xaxis_type = "date"
    except Exception:
        pass

    # 5) Grafico: barre (registered) + linea (retention) con secondary y
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    if "registered" in merged:
        fig.add_bar(
            x=merged[x],
            y=merged["registered"].fillna(0),
            name="Registered Editors",
        )
    fig.add_trace(
        go.Scatter(
            x=merged[x],
            y=merged["retention"].fillna(0),
            name="Retention Rate",
            hovertemplate="%{y:.2f}%"
        ),
        secondary_y=True
    )

    # 6) Layout
    titles = {"24h": "24 hours", "7d": "7 days", 
              "30d": "30 days", "60d": "60 days"}
    rate_label = titles.get(retention_rate, retention_rate)
    height = 600 if nlangs == 1 else 350

    fig.update_layout(
        title=f"Retention in {code} — editors editing again after {rate_label}",
        autosize=False, width=1200, height=height,
        legend=dict(orientation="h"),
        xaxis=dict(
            type=xaxis_type,
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
        ),
    )
    fig.update_yaxes(title_text="Registered Editors", secondary_y=False)
    fig.update_yaxes(title_text="Retention Rate (%)", secondary_y=True)

    return html.Div(dcc.Graph(id=f"retention_graph_{code}", figure=fig))


# STABILITY GRAPH
##########################################################################################################################################################################################################


def stability_graph(language, user_type: str, value_type: str, time_type: str):
    """
    language: lista di codici (es. ["pms","lij"]) oppure stringa "pms,lij" o None
    user_type: "5" (Active) | "100" (Very Active)
    value_type: "perc" | "m2_count"
    time_type: "y" | "ym"
    """

    
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    filter_lang = bool(codes)

    
    base_sql = f"""
        SELECT langcode, year_month, m2_value, m1_count, m2_count
        FROM vital_signs_metrics
        WHERE topic = 'stability'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :codes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("codes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params["codes"] = codes

    df = pd.read_sql_query(stmt, engine, params=params)
    bucket_order = ["1", "2", "3-6", "7-12", "13-24", "24+"]


    
    if df.empty:
        return html.Div(
            dcc.Graph(id="my_graph", figure=px.bar(
                title="No data for selected filters"))
        )

    
    denom = df["m1_count"].replace(0, pd.NA)
    df["perc"] = (df["m2_count"] / denom) * 100.0
    df["perc"] = df["perc"].fillna(0).round(2)
    df["m2_value"] = df["m2_value"].astype(str).str.strip()
    df["m2_value"] = df["m2_value"].replace({"+24": "24+", ">24": "24+"})

    df["m2_value"] = pd.Categorical(df["m2_value"], categories=bucket_order, ordered=True)

    lang_order = sorted(df["langcode"].unique())


   
    x_col = "year_month"
    xaxis_cfg = {"rangeslider": {"visible": False}, "type": "category"}
    try:
        df["dt"] = pd.to_datetime(df["year_month"], format="%Y-%m")
        x_col = "dt"
        xaxis_cfg = {"rangeslider": {"visible": False}, "type": "date"}
    except Exception:
        pass

    
    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Yearly" if time_type == "y" else "Monthly"
    y_col = "perc" if value_type == "perc" else "m2_count"
    text_tmpl = "%{y:.2f}%" if value_type == "perc" else ""

    
    n_langs = max(1, df["langcode"].nunique())
    height_value = 400 if n_langs == 1 else min(230 * n_langs, 1200)

    
    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        color="m2_value",          # bucket: "1","2","3-6","7-12","13-24","24+"
        text=y_col,
        facet_row="langcode",      # una riga per lingua (codice)
        width=1200,
        height=height_value,
        category_orders={
        "m2_value": bucket_order,
        "langcode": lang_order,
        },
        labels={
            x_col: f"Period ({time_text})",
            "perc": f"{incipit} Editors (%)",
            "m2_count": f"{incipit} Editors",
            "m2_value": "Active Months in a row",
            "langcode": "wiki",
        },
        title=f"{incipit} users stability",
    )

    fig.update_traces(texttemplate=text_tmpl)
    fig.update_layout(font_size=12, uniformtext_minsize=12,
                      uniformtext_mode="hide")
    fig.update_layout(xaxis=xaxis_cfg)
    # range selector utile anche sui bar con asse datetime
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
        )
    )

    return html.Div(dcc.Graph(id="my_graph", figure=fig))

# BALANCE GRAPH
##########################################################################################################################################################################################################


def balance_graph(language, user_type: str, value_type: str, time_type: str):
    """
    language: lista di codici (["pms","lij"]) oppure stringa "pms,lij" o None
    user_type: "5" (Active) | "100" (Very Active)
    value_type: "perc" | "m2_count"
    time_type: "y" | "ym"
    """

    # 1) Normalizza lingue -> lista di codici
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    filter_lang = bool(codes)

    # 2) Query parametrica (solo colonne necessarie)
    base_sql = f"""
        SELECT langcode, year_month, m2_value, m1_count, m2_count
        FROM vital_signs_metrics
        WHERE topic = 'balance'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :codes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("codes", expanding=True))

    params = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params["codes"] = codes

    df = pd.read_sql_query(stmt, engine, params=params)

    # 3) Empty state
    if df.empty:
        return html.Div(
            dcc.Graph(id="balance_graph", figure=px.bar(
                title="No data for selected filters"))
        )

    # 4) Colonne derivate (senza NumPy)
    # Evita divisioni per zero con where: i 0 diventano NaN
    denom = df["m1_count"].replace(0, pd.NA)
    df["perc"] = (df["m2_count"] / denom) * 100.0
    df["perc"] = df["perc"].fillna(0).round(2)

    # Asse X in datetime se possibile
    x_col = "year_month"
    xaxis_cfg = {"rangeslider": {"visible": False}, "type": "category"}
    try:
        df["dt"] = pd.to_datetime(df["year_month"], format="%Y-%m")
        x_col = "dt"
        xaxis_cfg = {"rangeslider": {"visible": False}, "type": "date"}
    except Exception:
        pass

    # 5) Testi e layout
    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Yearly" if time_type == "y" else "Monthly"

    y_col = "perc" if value_type == "perc" else "m2_count"
    text_tmpl = "%{y:.2f}%" if value_type == "perc" else ""

    # Altezza proporzionale al numero di lingue mostrate
    n_langs = max(1, df["langcode"].nunique())
    height_value = 400 if n_langs == 1 else min(230 * n_langs, 1200)

    # 6) Grafico: facet per codice, colori per coorti (m2_value = lustri)
    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        color="m2_value",            # es: "2001-2005", "2006-2010", …
        text=y_col,                  # mostra il valore coerente col tipo selezionato
        facet_row="langcode",        # una riga per ciascun codice
        width=1200,
        height=height_value,
        labels={
            x_col: f"Period ({time_text})",
            "perc": f"{incipit} Editors (%)",
            "m2_count": f"{incipit} Editors",
            "m2_value": "Lustrum First Edit",
            "langcode": "Project (code)",
        },
        title=f"{incipit} editors balance",
    )

    fig.update_traces(texttemplate=text_tmpl)
    fig.update_layout(font_size=12, uniformtext_minsize=12,
                      uniformtext_mode="hide")
    fig.update_layout(xaxis=xaxis_cfg)
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
        )
    )

    return html.Div(dcc.Graph(id="balance_graph", figure=fig))

# SPECIALISTS GRAPH
##########################################################################################################################################################################################################


def special_graph(language, user_type: str, value_type: str, time_type: str):
    """
    language: lista di codici (es. ["pms","lij"]) oppure stringa "pms,lij" o None
    user_type: "5" (Active) | "100" (Very Active)
    value_type: "perc" | "m2_count"
    time_type: "y" | "ym"
    """

    # 1) Normalizza lingue -> lista di codici
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    filter_lang = bool(codes)

    # 2) Query parametrica riusabile (solo colonne necessarie)
    base_sql = f"""
        SELECT langcode, year_month, m2_value, m1_count, m2_count
        FROM vital_signs_metrics
        WHERE year_year_month = :time_type
          AND m1_value       = :user_type
          AND topic          = :topic
          {"AND langcode IN :codes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt = text(base_sql)
    if filter_lang:
        stmt = stmt.bindparams(bindparam("codes", expanding=True))

    common = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        common["codes"] = codes

    # 3) Esegui le due query
    df_tech = pd.read_sql_query(
        stmt, engine, params={**common, "topic": "technical_editors"})
    df_coord = pd.read_sql_query(
        stmt, engine, params={**common, "topic": "coordinators"})

    def prep(df: pd.DataFrame) -> tuple[pd.DataFrame, str, dict]:
        if df.empty:
            return df, "year_month", {"rangeslider": {"visible": False}, "type": "category"}
        # evita divisione per zero
        denom = df["m1_count"].where(df["m1_count"] != 0)
        df["perc"] = (df["m2_count"] / denom) * 100.0
        df["perc"] = df["perc"].fillna(0).round(2)
        x_col = "year_month"
        xaxis_cfg = {"rangeslider": {"visible": False}, "type": "category"}
        try:
            df["dt"] = pd.to_datetime(df["year_month"], format="%Y-%m")
            x_col = "dt"
            xaxis_cfg = {"rangeslider": {"visible": False}, "type": "date"}
        except Exception:
            pass
        return df, x_col, xaxis_cfg

    df_tech,  x1, xaxis1 = prep(df_tech)
    df_coord, x2, xaxis2 = prep(df_coord)

    # 5) Testi/layout comuni
    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Yearly" if time_type == "y" else "Monthly"
    y_col = "perc" if value_type == "perc" else "m2_count"
    text_tmpl = "%{y:.2f}%" if value_type == "perc" else ""

    # Altezza proporzionale ai progetti (codici) visualizzati
    n_langs_tech = max(
        1, df_tech["langcode"].nunique()) if not df_tech.empty else 1
    n_langs_coord = max(
        1, df_coord["langcode"].nunique()) if not df_coord.empty else 1
    h1 = 400 if n_langs_tech == 1 else min(230 * n_langs_tech,  1200)
    h2 = 400 if n_langs_coord == 1 else min(230 * n_langs_coord, 1200)

    # 6) Costruzione grafici (facet per langcode, colori per coorti m2_value)
    if df_tech.empty:
        fig1 = px.bar(title=f"{incipit} Technical Contributors — No data")
    else:
        fig1 = px.bar(
            df_tech,
            x=x1, y=y_col,
            color="m2_value",             # coorti (lustri)
            text=y_col,
            facet_row="langcode",         # una riga per codice
            width=1200, height=h1,
            labels={
                x1: f"Period ({time_text})",
                "perc": f"{incipit} editors (%)",
                "m2_count": f"{incipit} editors",
                "m2_value": "Lustrum First Edit",
                "langcode": "wiki",
            },
            title=f"{incipit} Technical Contributors",
        )
        fig1.update_traces(texttemplate=text_tmpl)
        fig1.update_layout(font_size=12, uniformtext_minsize=12,
                           uniformtext_mode="hide", xaxis=xaxis1)
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
            )
        )

    if df_coord.empty:
        fig2 = px.bar(title=f"{incipit} Project Coordinators — No data")
    else:
        fig2 = px.bar(
            df_coord,
            x=x2, y=y_col,
            color="m2_value",
            text=y_col,
            facet_row="langcode",
            width=1200, height=h2,
            labels={
                x2: f"Period ({time_text})",
                "perc": f"{incipit} editors (%)",
                "m2_count": f"{incipit} editors",
                "m2_value": "Lustrum First Edit",
                "langcode": "wiki",
            },
            title=f"{incipit} Project Coordinators",
        )
        fig2.update_traces(texttemplate=text_tmpl)
        fig2.update_layout(font_size=12, uniformtext_minsize=12,
                           uniformtext_mode="hide", xaxis=xaxis2)
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
            )
        )

    # 7) Output
    return html.Div(
        children=[
            dcc.Graph(id="special_graph_tech",  figure=fig1),
            html.Div(id="highlights_container_additional", children=[]),
            dcc.Graph(id="special_graph_coord", figure=fig2),
        ]
    )


# ADMIN FLAG GRAPH
##########################################################################################################################################################################################################


def admin_graph(language, admin_type: str, time_type: str):
    """
    language: lista di codici (es. ["pms","lij"]) oppure stringa "pms,lij" o None
    admin_type: es. "sysop", "bureaucrat", ...
    time_type: "y" | "ym"
    """

    # 1) Normalizza lingue -> lista di codici
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    filter_lang = bool(codes)
    time_text = "Yearly" if time_type == "y" else "Monthly"

    # 2) Query 1: flags per mese e lustrum (per lingua)
    sql_q1 = f"""
        SELECT
            langcode,
            year_month,
            m2_value,
            SUM(m2_count) AS count
        FROM vital_signs_metrics
        WHERE topic = 'flags'
          AND m1 = 'granted_flag'
          AND m1_value = :admin_type
          AND m2_value IS NOT NULL
          {"AND langcode IN :codes" if filter_lang else ""}
        GROUP BY langcode, year_month, m2_value
        ORDER BY langcode, year_month, m2_value
    """
    stmt_q1 = text(sql_q1)
    if filter_lang:
        stmt_q1 = stmt_q1.bindparams(bindparam("codes", expanding=True))
    params_q1 = {"admin_type": admin_type}
    if filter_lang:
        params_q1["codes"] = codes
    df1 = pd.read_sql_query(stmt_q1, engine, params=params_q1)
    

    # 3) Query 2: totale per lustrum (per lingua)
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
          {"AND langcode IN :codes" if filter_lang else ""}
        GROUP BY langcode, m2_value
        ORDER BY langcode, m2_value
    """
    stmt_q2 = text(sql_q2)
    if filter_lang:
        stmt_q2 = stmt_q2.bindparams(bindparam("codes", expanding=True))
    params_q2 = {"admin_type": admin_type}
    if filter_lang:
        params_q2["codes"] = codes
    df2 = pd.read_sql_query(stmt_q2, engine, params=params_q2)
    if not df2.empty:
        df2["x"] = ""  # asse X “finto” per impilare per-facet

    # 4) Query 3: % admin tra active editors (per mese/lingua)
    # Nota: manteniamo il filtro che hai in origine (m2_value = :admin_type).
    sql_q3 = f"""
        SELECT langcode, year_month, m1_count, m2_count
        FROM vital_signs_metrics
        WHERE topic = 'flags'
          AND year_year_month = :time_type
          AND m2_value = :admin_type
          AND m1 = 'monthly_edits'
          AND m1_value = '5'
          {"AND langcode IN :codes" if filter_lang else ""}
        ORDER BY langcode, year_month
    """
    stmt_q3 = text(sql_q3)
    if filter_lang:
        stmt_q3 = stmt_q3.bindparams(bindparam("codes", expanding=True))
    params_q3 = {"time_type": time_type, "admin_type": admin_type}
    if filter_lang:
        params_q3["codes"] = codes
    df3 = pd.read_sql_query(stmt_q3, engine, params=params_q3)

    # 5) Derivate + asse tempo
    def to_datetime_if_possible(df: pd.DataFrame) -> tuple[pd.DataFrame, str, dict]:
        if df.empty:
            return df, "year_month", {"rangeslider": {"visible": False}, "type": "category"}
        x_col = "year_month"
        xaxis_cfg = {"rangeslider": {"visible": False}, "type": "category"}
        try:
            df["dt"] = pd.to_datetime(df["year_month"], format="%Y-%m")
            x_col = "dt"
            xaxis_cfg = {"rangeslider": {"visible": False}, "type": "date"}
        except Exception:
            pass
        return df, x_col, xaxis_cfg

    df1, x1, xcfg1 = to_datetime_if_possible(df1)
    df3, x3, xcfg3 = to_datetime_if_possible(df3)

    if x1 == "dt":
        df1 = df1.sort_values(["langcode", "dt", "m2_value"])
    else:
        df1["_x_sort"] = pd.to_datetime(df1["year_month"], errors="coerce")
        df1 = df1.sort_values(["langcode", "_x_sort", "m2_value"])
        ordered = df1["year_month"].drop_duplicates().tolist()

    # % tra active editors (senza NumPy)
    if not df3.empty:
        denom = df3["m1_count"].where(df3["m1_count"] != 0)
        df3["perc"] = (df3["m2_count"] / denom) * 100.0
        df3["perc"] = df3["perc"].fillna(0).round(2)

    # 6) Altezze
    n_langs = max(
        1,
        max(
            df1["langcode"].nunique() if not df1.empty else 0,
            df2["langcode"].nunique() if not df2.empty else 0,
            df3["langcode"].nunique() if not df3.empty else 0,
        )
    )
    height_value = 400 if n_langs == 1 else min(230 * n_langs, 1200)

    # 7) Figure 1: flags per mese e lustrum
    if df1.empty:
        fig1 = px.bar(
            title=f"{admin_type} flags granted — No data", width=850, height=height_value)
    else:
        fig1 = px.bar(
            df1,
            x=x1, y="count",
            color="m2_value",
            text="count",
            facet_row="langcode",
            width=850, height=height_value,
            labels={
                x1: f"Period ({time_text})",
                "count": "Number of Admins",
                "m2_value": "Lustrum First Edit",
                "langcode": "wiki",
            },
            title=f"{admin_type} flags granted over the years by lustrum of first edit",
        )
        if x1 != "dt":
            fig1.update_xaxes(categoryorder="array", categoryarray=ordered)
        fig1.update_traces(texttemplate="%{text}")
        fig1.update_layout(uniformtext_minsize=8, uniformtext_mode="hide")
        fig1.update_xaxes(**xcfg1) 

        fig1.update_xaxes(
            rangeselector=dict(buttons=[
                dict(count=6,  label="<b>6M</b>", step="month", stepmode="backward"),
                dict(count=1,  label="<b>1Y</b>", step="year",  stepmode="backward"),
                dict(count=5,  label="<b>5Y</b>", step="year",  stepmode="backward"),
                dict(count=10, label="<b>10Y</b>", step="year",  stepmode="backward"),
                dict(label="<b>ALL</b>", step="all"),
            ])
        )


    # 8) Figure 2: totale per lustrum (per lingua)
    if df2.empty:
        fig2 = px.bar(
            title=f"Total Num. of [{admin_type}] — No data", width=300, height=height_value)
    else:
        fig2 = px.bar(
            df2,
            x="x", y="count",
            color="m2_value",
            text="count",
            facet_row="langcode",
            width=300, height=height_value,
            labels={"count": "", "x": "", "langcode": "wiki"},
            title=f"Total Num. of {admin_type}",
        )
        fig2.update_traces(texttemplate="%{text}")
        fig2.update_layout(uniformtext_minsize=8,
                           uniformtext_mode="hide", showlegend=False)

    # 9) Figure 3: percentuale admin tra active editors
    if df3.empty:
        fig3 = px.bar(
            title=f"Percentage of {admin_type} flags among active editors — No data",
            width=1000, height=height_value
        )
    else:
        # prepara l’etichetta già formattata (opzionale ma pulito)
        df3["label"] = df3["perc"].round(2).astype(str) + "%"

        fig3 = px.bar(
            df3,
            x=x3, y="perc",
            facet_row="langcode",
            text=None,                      # <— forza: nessun testo di base
            width=1000, height=height_value,
            labels={x3: f"Period ({time_text})", "perc": "Percentage", "langcode": "wiki"},
            title=f"Percentage of {admin_type} flags among active editors on a {time_text} basis",
        )

        # azzera ogni eventuale testo residuo nel/i trace
        fig3.update_traces(text=None, texttemplate=None)

        # imposta UNA sola etichetta, esterna
        # (se preferisci usare direttamente y: texttemplate="%{y:.2f}%")
        fig3.update_traces(text=df3["label"], texttemplate="%{text}", textposition="outside", cliponaxis=False)

        fig3.update_layout(uniformtext_minsize=12, uniformtext_mode="hide", xaxis=xcfg3)
        fig3.update_layout(
            xaxis=dict(
                rangeselector=dict(buttons=[
                    dict(count=6,  label="<b>6M</b>", step="month", stepmode="backward"),
                    dict(count=1,  label="<b>1Y</b>", step="year",  stepmode="backward"),
                    dict(count=5,  label="<b>5Y</b>", step="year",  stepmode="backward"),
                    dict(count=10, label="<b>10Y</b>", step="year",  stepmode="backward"),
                    dict(label="<b>ALL</b>", step="all"),
                ]),
            )
        )


    # 10) Output
    return html.Div(children=[
        dcc.Graph(id="admin_graph_flags_over_time", figure=fig1,
                  style={'display': 'inline-block'}),
        dcc.Graph(id="admin_graph_totals", figure=fig2,
                  style={'display': 'inline-block'}),
        html.Hr(),
        html.Div(id='highlights_container_additional', children=[]),
        dcc.Graph(id="admin_graph_percentage", figure=fig3),
    ])


# GLOBAL COMMUNITY GRAPH
##########################################################################################################################################################################################################

def global_graph(language, user_type: str, value_type: str, time_type: str, year: str, month: str):
    """
    language: lista di codici (es. ["pms","lij"]) oppure stringa "pms,lij" o None
    user_type: "5" (Active) | "100" (Very Active)
    value_type: "perc" | "m2_count"
    time_type: "y" | "ym"
    year, month: periodo per lo snapshot (es. "2024", "03")
    """

    # 1) Normalizza lingue -> lista di codici
    if not language:
        codes = []
    elif isinstance(language, str):
        codes = [s.strip() for s in language.split(",") if s.strip()]
    else:
        codes = list(language)

    filter_lang = bool(codes)

    # 2) Query 1 — serie temporale primary_editors
    base_q1 = f"""
        SELECT langcode, year_month, m1_count, m2_count, m2_value
        FROM vital_signs_metrics
        WHERE topic = 'primary_editors'
          AND year_year_month = :time_type
          AND m1_value = :user_type
          {"AND langcode IN :codes" if filter_lang else ""}
        ORDER BY year_month
    """
    stmt_q1 = text(base_q1)
    if filter_lang:
        stmt_q1 = stmt_q1.bindparams(bindparam("codes", expanding=True))

    params_q1 = {"time_type": time_type, "user_type": user_type}
    if filter_lang:
        params_q1["codes"] = codes

    df1 = pd.read_sql_query(stmt_q1, engine, params=params_q1)

    # Stato totalmente vuoto → restituisco due grafici "gentili"
    if df1.empty:
        empty = px.bar(title="No data for selected filters")
        return html.Div([
            dcc.Graph(id="global_graph_timeseries", figure=empty,
                      style={'display': 'inline-block'}),
            html.Hr(),
            html.Div(id='highlights_container_additional', children=[]),
            dcc.Graph(id="global_graph_treemap", figure=empty),
        ])

    # 3) Derivate + asse tempo
    # perc = 100 * m2_count / m1_count, evitando divisione per zero
    denom = df1["m1_count"].where(df1["m1_count"] != 0)
    df1["perc"] = (df1["m2_count"] / denom) * 100.0
    df1["perc"] = df1["perc"].fillna(0).round(2)

    # year_month -> datetime se possibile
    x_col = "year_month"
    xaxis_cfg = {"rangeslider": {"visible": True}, "type": "category"}
    try:
        df1["dt"] = pd.to_datetime(df1["year_month"], format="%Y-%m")
        x_col = "dt"
        xaxis_cfg = {"rangeslider": {"visible": True}, "type": "date"}
    except Exception:
        pass

    # 4) Etichette/layout
    incipit = "Active" if user_type == "5" else "Very Active"
    time_text = "Yearly" if time_type == "y" else "Monthly"
    y_col = "perc" if value_type == "perc" else "m2_count"
    text_tmpl = "%{y:.2f}%" if value_type == "perc" else ""

    # altezza proporzionale ai progetti mostrati
    n_langs = max(1, df1["langcode"].nunique())
    h1 = 400 if n_langs == 1 else min(270 * n_langs, 1200)

    # 5) Figura 1 — bar facet per progetto, colori per primary language (m2_value)
    fig1 = px.bar(
        df1,
        x=x_col,
        y=y_col,
        color="m2_value",           # primary language
        text=y_col,
        facet_row="langcode",       # una riga per ciascun codice
        width=1300,
        height=h1,
        labels={
            x_col: f"Period ({time_text})",
            "perc": f"{incipit} editors (%)",
            "m2_count": f"{incipit} editors",
            "m2_value": "Primary language",
            "langcode": "wiki",
        },
        title=f"{incipit} editors by primary language",
    )
    fig1.update_traces(texttemplate=text_tmpl)
    fig1.update_layout(uniformtext_minsize=10,
                       uniformtext_mode="hide", xaxis=xaxis_cfg)

    # 6) Query 2 — snapshot per treemap (Meta, anno/mese selezionati, sempre 'ym')
    ym_value = f"{year}-{month}"
    stmt_q2 = text("""
        SELECT langcode, m2_value, m1_count, m2_count, year_month
        FROM vital_signs_metrics
        WHERE topic = 'primary_editors'
          AND year_year_month = 'ym'
          AND year_month = :ym_value
          AND m1_value = :user_type
          AND langcode = 'meta'
        ORDER BY m2_value
    """)
    df2 = pd.read_sql_query(stmt_q2, engine, params={
                            "ym_value": ym_value, "user_type": user_type})

    if df2.empty:
        fig2 = px.treemap(
            title=f"Meta-wiki {incipit} editors by primary language — No data for {ym_value}")
    else:
        denom2 = df2["m1_count"].where(df2["m1_count"] != 0)
        df2["perc"] = (df2["m2_count"] / denom2) * 100.0
        df2["perc"] = df2["perc"].fillna(0).round(2)

        # Treemap: metto i riquadri come “root” (niente genitore), etichetta = primary language (m2_value)
        fig2 = go.Figure(go.Treemap(
            labels=df2["m2_value"],                         # primary language
            # nessun genitore esplicito
            parents=[""] * len(df2),
            values=df2["m2_count"],
            customdata=df2["perc"],
            text=df2["m2_value"],
            texttemplate="<b>%{label}</b><br>%{value} editors<br>%{customdata}% retention",
            hovertemplate="<b>%{label}</b><br>Editors: %{value}<br>Percent: %{customdata}%<extra></extra>",
        ))
        fig2.update_layout(
            width=1200,
            title_text=f"Meta-wiki {incipit} editors by primary language — {ym_value}"
        )

    # 7) Output
    return html.Div([
        dcc.Graph(id="global_graph_timeseries", figure=fig1,
                  style={'display': 'inline-block'}),
        html.Hr(),
        html.Div(id='highlights_container_additional', children=[]),
        dcc.Graph(id="global_graph_treemap", figure=fig2),
    ])



@server.route('/code')
def code():
    return redirect("https://github.com/WikiCommunityHealth/vital-signs-pipeline", code=301)


@server.route("/data/<path:filename>")
def downloads(filename):
    
    if not (filename.endswith(".sqlite.gz") or filename.endswith(".zip")):
        abort(404)
    return send_from_directory("/app/public_downloads", filename, as_attachment=True)

@server.route("/architecture")
def architecture_page():
    
    return send_file("/app/static/architecture.html")

@server.route("/data")
def dataset_page():
    
    return send_file("/app/static/dataset.html")



# ---------- MAIN ----------
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8050, debug=False)
