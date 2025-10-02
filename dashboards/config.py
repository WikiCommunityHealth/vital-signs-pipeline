import dash
import pandas as pd
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc
from dash.dependencies import Input, Output, State
import datetime
import time
from sqlalchemy import create_engine, text, bindparam
import plotly
import chart_studio.plotly as py
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib
from urllib.parse import urlparse, parse_qsl, urlencode
import os

last_period = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m')
LOGO = "./assets/logo.png"
LOGO_foot = "./assets/wikimedia-logo.png"

# Web resources
title_addenda = ' - Wikimedia Community Health Metrics'
external_stylesheets = [dbc.themes.BOOTSTRAP]
external_scripts = []
webtype = ''
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB   = os.getenv("POSTGRES_DB", "vital_signs_web")

assert PG_USER and PG_PASS, "Missing POSTGRES_USER/POSTGRES_PASSWORD in env"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}",
    pool_pre_ping=True,
)
metrics = ['activity', 'stability', 'balance',
           'retention', 'special', 'global', 'admin']

# languages

wikilanguagecodes = ['lij', 'pms', 'lmo']


admin_type = ['autopatrolled', 'sysop', 'bureaucrat', 'checkuser', 'ipblock-exempt', 'rollbacker', 'confirmed', 'extendedconfirmed', 'interface-admin', 'patroller', 'import', 'abusefilter',
              'reviewer', 'accountcreator', 'oversight', 'founder', 'autoreviewer', 'eventcoordinator', 'filemover', 'researcher', 'massmessage-sender', 'extendedmover', 'templateeditor', 'steward']

year_list = [str(year) for year in range(2002, datetime.datetime.now().year + 1)]

month_list = ['01', '02', '03', '04', '05',
              '06', '07', '08', '09', '10', '11', '12']

navbar = html.Div([
    html.Br(),
    dbc.Navbar(
        dbc.Container([
            dbc.NavbarBrand(html.Div([
                html.Img(src=LOGO_foot, height="80px",
                         style={'marginLeft': '10px'}),
                html.Img(src=LOGO, height="80px", style={'marginLeft': '10px'})
            ]), href="https://meta.wikimedia.org/wiki/Community_Health_Metrics"),
        ]),
        color="white",
        dark=False
    )
])

# Dropdown menu (commented out)
# dbc.Nav([
#     dbc.DropdownMenu(
#         label="Metrics",
#         children=[
#             dbc.DropdownMenuItem("Activity", href="/activity"),
#             dbc.DropdownMenuItem("Retention", href="/retention"),
#             dbc.DropdownMenuItem("Stability", href="/stability"),
#             dbc.DropdownMenuItem("Balance", href="/balance"),
#             dbc.DropdownMenuItem("Admin", href="/admin"),
#             dbc.DropdownMenuItem("Special", href="/special"),
#             dbc.DropdownMenuItem("Global", href="/global")
#         ],
#         nav=True,
#     ),
# ], className="ms-auto")


footbar = html.Div([
    html.Br(), html.Hr(),
    dbc.Container([
        dbc.Row([
            dbc.Col(html.Div(f"Updated with dataset from: {last_period}", style={
                    'textAlign': 'right'}))
        ])
    ])
])

