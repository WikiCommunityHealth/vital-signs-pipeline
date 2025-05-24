# -*- coding: utf-8 -*-
from config import *
from flask import Flask
import dash


##### FLASK APP #####
server = Flask(__name__)  # usato da Dash

##### DASH APP #####
app = Dash(__name__, server=server,
           external_stylesheets=external_stylesheets)
app.title = "Wikimedia Community Health Metrics"
app.config['suppress_callback_exceptions'] = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div([
        html.H1("Vital Signs Dashboards", style={'textAlign': 'center'})
    ]),
    html.Div(id='content'),
    footbar
])

from apps import activity, admin, balance, globall, main_app, retention, special, stability

@app.callback(
    Output('content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    if pathname == '/main':
        return main_app.layout
    elif pathname == '/activity':
        return activity.layout
    elif pathname == '/admin':
        return admin.layout
    elif pathname == '/balance':
        return balance.layout
    elif pathname == '/global':
        return globall.layout
    elif pathname == '/retention':
        return retention.layout
    elif pathname == '/special':
        return special.layout
    elif pathname == '/stability':
        return stability.layout
    else:
        return html.A("Welcome!", href="/main")
        # default: main

##### APP LAYOUT #####


##### RUN LOCAL SERVER #####
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)
