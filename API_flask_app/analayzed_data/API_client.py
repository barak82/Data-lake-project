import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import os
import sys

dir_path_main = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(f'{dir_path_main}/data_AWS/analyzed/')
sys.path.append(f'{dir_path_main}')
# Connect to main app.py file
from app import app
from app import server

# Connect to your app pages
from apps import over_all_df_property
from apps import def_analysis


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div([
        dcc.Link('deflection|', href='/apps/def'),
        dcc.Link('overall days data', href='/apps/all_pro'),
    ], className="row"),
    html.Div(id='page-content', children=[])
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/def':
        return def_analysis.layout
    if pathname == '/apps/all_pro':
        return over_all_df_property.layout
    else:
        return "404 Page Error! Please choose a link"


if __name__ == '__main__':
    app.run_server(debug=False)