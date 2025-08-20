import pandas as pd
import dash_bootstrap_components as dbc
from dash import html
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import datetime
import time
from sqlalchemy import create_engine
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
database = f'postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres/vital_signs_web'
metrics = ['activity', 'stability', 'balance',
           'retention', 'special', 'global', 'admin']

# languages

languages = pd.DataFrame({
    'Wikimedialanguagecode': ['lij', 'pms', 'lmo'],
    'languagename': ['ligurian', 'piedmontese', 'lombard']
}).set_index('Wikimedialanguagecode')

wikilanguagecodes = list(languages.index.tolist())

language_names_list = []
language_names = {}
language_names_full = {}
language_name_wiki = {}
for languagecode in wikilanguagecodes:
    lang_name = languages.loc[languagecode]['languagename'] + \
        ' ('+languagecode+')'
    language_name_wiki[lang_name] = languages.loc[languagecode]['languagename']
    language_names_full[languagecode] = languages.loc[languagecode]['languagename']
    language_names[lang_name] = languagecode
    language_names_list.append(lang_name)

language_names_list = sorted(language_names_list)
language_names_inv = {v: k for k, v in language_names.items()}

admin_type = ['autopatrolled', 'sysop', 'bureaucrat', 'checkuser', 'ipblock-exempt', 'rollbacker', 'confirmed', 'extendedconfirmed', 'interface-admin', 'patroller', 'import', 'abusefilter',
              'reviewer', 'accountcreator', 'oversight', 'founder', 'autoreviewer', 'eventcoordinator', 'filemover', 'researcher', 'massmessage-sender', 'extendedmover', 'templateeditor', 'steward']

year_list = ['2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012',
             '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025']
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
            dbc.Nav([
                dbc.DropdownMenu(
                    label="Metrics",
                    children=[
                        dbc.DropdownMenuItem("Activity", href="/activity"),
                        dbc.DropdownMenuItem("Retention", href="/retention"),
                        dbc.DropdownMenuItem("Stability", href="/stability"),
                        dbc.DropdownMenuItem("Balance", href="balance"),
                        dbc.DropdownMenuItem("Admin", href="/admin"),
                        dbc.DropdownMenuItem("Special", href="/special"),
                        dbc.DropdownMenuItem("Global", href="/global")
                    ],
                    nav=True,
                ),
            ], className="ms-auto"),
        ]),
        color="white",
        dark=False
    )
])


footbar = html.Div([
    html.Br(), html.Hr(),
    dbc.Container([
        dbc.Row([
            dbc.Col(html.Div(f"Updated with dataset from: {last_period}", style={
                    'textAlign': 'right'}))
        ])
    ])
])

