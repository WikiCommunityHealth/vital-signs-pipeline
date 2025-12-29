import dash
import pandas as pd
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, no_update
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

last_period = (datetime.datetime.now().replace(day=1) -
               datetime.timedelta(days=1)).strftime('%Y-%m')
LOGO = "./assets/logo.png"
LOGO_foot = "./assets/wikimedia-logo.png"

# Web resources
title_addenda = ' - Wikimedia Community Health Metrics'
external_stylesheets = [dbc.themes.BOOTSTRAP]
external_scripts = []
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres-frontend")
PG_DB = os.getenv("POSTGRES_DB", "vital_signs_web")

assert PG_USER and PG_PASS, "Missing POSTGRES_USER/POSTGRES_PASSWORD in env"

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}",
    pool_pre_ping=True,
)
metrics = ['activity', 'stability', 'balance',
           'retention', 'special', 'global', 'admin']

# languages
#wikilanguagecodes = ['it', 'es', 'de', 'fr', 'meta']

wikilanguagecodes = ['ab', 'ace', 'ady', 'af', 'ak', 'als', 'alt', 'ami', 'am', 'ang', 'ann', 'anp', 'an', 'arc', 'ar', 'ary', 'arz', 'ast', 'as', 'atj',
                     'avk', 'av', 'awa', 'ay', 'azb', 'az', 'ban', 'bar', 'bat_smg', 'ba', 'bbc', 'bcl', 'bdr', 'be_x_old', 'be', 'bew', 'bg', 'bh', 'bi', 'bjn',
                     'blk', 'bm', 'bn', 'bo', 'bpy', 'br', 'bs', 'btm', 'bug', 'bxr', 'ca', 'cbk_zam', 'cdo', 'ceb', 'ce', 'cho', 'chr', 'ch', 'chy', 'ckb',
                     'co', 'crh', 'cr', 'csb', 'cs', 'cu', 'cv', 'cy', 'dag', 'da', 'de', 'dga', 'din', 'diq', 'dsb', 'dtp', 'dty', 'dv', 'dz', 'ee', 'el',
                     'eml', 'en', 'eo', 'es', 'et', 'eu', 'ext', 'fat', 'fa', 'ff', 'fiu_vro', 'fi', 'fj', 'fon', 'fo', 'frp', 'frr', 'fr', 'fur', 'fy',
                     'gag', 'gan', 'ga', 'gcr', 'gd', 'glk', 'gl', 'gn', 'gom', 'gor', 'got', 'gpe', 'guc', 'gur', 'gu', 'guw', 'gv', 'hak', 'ha', 'haw', 'he', 'hif',
                           'hi', 'ho', 'hr', 'hsb', 'ht', 'hu', 'hy', 'hyw', 'hz', 'ia', 'iba', 'id', 'ie', 'igl', 'ig', 'ii', 'ik', 'ilo', 'inh', 'io', 'is', 'it',
                     'iu', 'jam', 'ja', 'jbo', 'jv', 'kaa', 'kab', 'ka', 'kbd', 'kbp', 'kcg', 'kge', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'knc', 'kn', 'koi', 'ko', 'krc', 'kr', 'ksh',
                     'ks', 'kus', 'ku', 'kv', 'kw', 'ky', 'labs', 'lad', 'la', 'lbe', 'lb', 'lez', 'lfn', 'lg', 'lij', 'li', 'lld', 'lmo', 'ln',
                     'lo', 'lrc', 'ltg', 'lt', 'lv', 'mad', 'mai', 'map_bms', 'mdf', 'meta', 'mg', 'mhr', 'mh', 'min', 'mi', 'mk', 'ml',
                     'mni', 'mn', 'mnw', 'mos', 'mrj', 'mr', 'ms', 'mt', 'mus', 'mwl', 'myv', 'my', 'mzn', 'nah', 'nap', 'na', 'nds_nl', 'nds', 'ne', 'new', 'ng',
                     'nia', 'nl', 'nn', 'nov', 'no', 'nqo', 'nrm', 'nr', 'nso', 'nv', 'ny', 'oc', 'olo', 'om', 'or', 'os', 'pag', 'pam', 'pap', 'pa', 'pcd',
                     'pcm', 'pdc', 'pfl', 'pih', 'pi', 'pl', 'pms', 'pnb', 'pnt', 'ps', 'pt', 'pwn', 'qu', 'rm', 'rmy', 'rn', 'roa_rup', 'roa_tara', 'ro', 'rsk', 'rue', 'ru', 'rw',
                     'sah', 'sat', 'sa', 'scn', 'sco', 'sc', 'sd', 'se', 'sg', 'shi', 'shn', 'sh', 'simple', 'si', 'skr', 'sk', 'sl', 'smn', 'sm', 'sn', 'so', 'sq',
                     'srn', 'sr', 'ss', 'stq', 'st', 'su', 'sv', 'sw', 'syl', 'szl', 'szy', 'ta', 'tay', 'tcy', 'tdd', 'ten', 'tet', 'te', 'tg', 'th', 'tig',
                     'ti', 'tk', 'tl', 'tly', 'tn', 'to', 'tpi', 'trv', 'tr', 'ts', 'tt', 'tum', 'tw', 'tyv', 'ty', 'udm', 'ug', 'uk', 'ur', 'uz', 'vec', 'vep', 've', 'vi', 'vls', 'vote', 'vo', 'war',
                     'wa', 'wo', 'wuu', 'xal', 'xh', 'xmf', 'yi', 'yo', 'za', 'zea', 'zgh', 'zh_classical', 'zh_min_nan', 'zh_yue', 'zh', 'zu']


admin_type = ['autopatrolled', 'sysop', 'bureaucrat', 'checkuser', 'ipblock-exempt', 'rollbacker', 'confirmed', 'extendedconfirmed', 'interface-admin', 'patroller', 'import', 'abusefilter',
              'reviewer', 'accountcreator', 'oversight', 'founder', 'autoreviewer', 'eventcoordinator', 'filemover', 'researcher', 'massmessage-sender', 'extendedmover', 'templateeditor', 'steward']

year_list = [str(year)
             for year in range(2002, datetime.datetime.now().year + 1)]

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


footbar = html.Div([
    html.Br(), html.Hr(),
    dbc.Container([
        dbc.Row([
            dbc.Col(html.Div(f"Updated with dataset from: {last_period}", style={
                    'textAlign': 'right'}))
        ])
    ])
])
