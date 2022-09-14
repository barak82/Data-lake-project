import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import pathlib
from app import app
import os
import sys
from glob import  glob

# get relative data folder
dir_path_main = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(f'{dir_path_main}/data_AWS/analyzed/')
main_path=f'{dir_path_main}/data_AWS/analyzed/'
sub_folders_path=glob(main_path+"/*" ,recursive = True)
sub_folders=[f.split("/")[-1] for f in sub_folders_path]

# read the data
axle_counts = ["2","3","4","5","6","All"]
property_df = {"GVW":"average_GVW","average_speed":"average_speed","average_wheel_weight":"average_wheel_weight","deflection_mean":"deflection_mean","deflection_min":"deflection_min","deflection_max":"deflection_max","deflection_per_load":"deflection_per_load"}

layout = html.Div([
    html.H1('acc sensors data analysis', style={"textAlign": "center"}),
       
    html.Div([
        html.Div(dcc.Dropdown(
            id='dates', value=sub_folders[0], clearable=False,
            options=[{'label': k, 'value': k} for k in sub_folders]
        ), className='three columns'),

        html.Div(dcc.Dropdown(
            id='axle-counts', value=axle_counts[0], clearable=False,
            persistence=True, persistence_type='memory',
            options=[{'label': "Axle num "+x, 'value': x} for x in axle_counts]
        ), className='three columns'),

        html.Div(dcc.Dropdown(
            id='pd-properties-1', value="average_GVW", clearable=False,
            options=[{'label': k, 'value': v} for k,v in property_df.items()]
        ), className='three columns'),
        html.Div(dcc.Dropdown(
            id='pd-properties-2', value="average_speed", clearable=False,
            options=[{'label': k, 'value': v} for k,v in property_df.items()]
        ), className='three columns'),

    ], style={"display": "flex", "flexWrap": "wrap"}),

    dcc.Graph(id='chart-def', figure={}),
])


@app.callback(
    Output(component_id='chart-def', component_property='figure'),
    [Input(component_id='dates', component_property='value'),
    Input(component_id='pd-properties-1', component_property='value'),
    Input(component_id='pd-properties-2', component_property='value'),
    Input(component_id='axle-counts', component_property='value')]
)
def display_value(day,pd_property_1,pd_property_2, axle_count):
    data_parq=f'{dir_path_main}/data_AWS/analyzed/{day}'
    # csv_file=glob(f'{data_csv}*csv')
    data = pd.read_parquet(data_parq)
    data["deflection_per_load"]=data["deflection_max"]/(data["average_wheel_weight"]/1e3)
    if axle_count=="All":
        fig = px.scatter(data, x=pd_property_1, y=pd_property_2, color=pd_property_2)
        
    else:
        mask = data["axleCount"] == int(axle_count)
        fig = px.scatter(data[mask], x=pd_property_1, y=pd_property_2, color=pd_property_2,
    title=f"Date- {day}    Axle={axle_count}      num of dataset={data[mask].shape[0]}")
    fig.update_layout(title_x=0.5)

    return fig