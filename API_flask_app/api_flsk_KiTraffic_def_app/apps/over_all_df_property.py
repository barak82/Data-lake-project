import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import pathlib
from app import app
import sys
import os
from glob import glob
dir_path_main = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(f'{dir_path_main}/data_AWS/analyzed/')
main_path=f'{dir_path_main}/data_AWS/analyzed/'
sub_folders_path=glob(main_path+"/*" ,recursive = True)
sub_folders=[f.split("/")[-1] for f in sub_folders_path]
data=[]
for day,folder in zip(sub_folders_path,sub_folders):
    # data_file=glob(f'{day}/*csv')
    # pd_data=pd.read_csv(data_file[0])
    pd_data=pd.read_parquet(day)
    pd_data["date"]=folder
    data.append(pd_data)
data_all=pd.concat(data)
# data_all=[]
# read the data
axle_counts = ["2","3","4","5","6","All"]
property_df = {"GVW":"average_GVW","average_speed":"average_speed","average_wheel_weight":"average_wheel_weight","deflection_mean":"deflection_mean","deflection_min":"deflection_min","deflection_max":"deflection_max","deflection_per_load":"deflection_per_load"}

layout = html.Div([
    html.H1('acc sensors data analysis', style={"textAlign": "center"}),
    html.Div([

        html.Div(dcc.Dropdown(
            id='axle-counts2', value=axle_counts[0], clearable=False,
            persistence=True, persistence_type='memory',
            options=[{'label': "Axle num "+x, 'value': x} for x in axle_counts]
        ), className='two columns'),

        html.Div(dcc.Dropdown(
            id='pd-properties-12', value="average_wheel_weight", clearable=False,
            options=[{'label': k, 'value': v} for k,v in property_df.items()]
        ), className='two columns'),

        html.Div(dcc.Dropdown(
            id='pd-properties-22', value="average_wheel_weight", clearable=False,
            options=[{'label': k, 'value': v} for k,v in property_df.items()]
        ), className='two columns'),

        dbc.Input(
            placeholder="input min value", size="md", className="mb-3",id="range1",
            type="number",value=0
        ),
        dbc.Input(
            placeholder="input max value", size="md", className="mb-3",id="range2", 
            type="number",value=1e10
        ),
    ], style={"display": "flex", "flexWrap": "wrap"}),

    dcc.Graph(id='chart-def2', figure={}),
])

@app.callback(
    Output(component_id='chart-def2', component_property='figure'),
    [
    Input(component_id='range1', component_property='value'),
    Input(component_id='range2', component_property='value'),
    Input(component_id='pd-properties-12', component_property='value'),
    Input(component_id='pd-properties-22', component_property='value'),
    Input(component_id='axle-counts2', component_property='value')]
)
def display_value(range_1,range_2,pd_property_1,pd_property_2, axle_count):
    data_all["deflection_per_load"]=data_all["deflection_mean"]/(data_all["average_wheel_weight"]/1e3)
    if axle_count=="All":
        data_=data_all
    else:
        mask = data_all["axleCount"] == int(axle_count)
        data_=data_all[mask]
    data_pd=data_[(data_[pd_property_2] >= range_1) & (data_[pd_property_2] <= range_2)]
    # fig = px.scatter(data_, x="date", y=pd_property_1, color="species", marginal_y="violin",
    #        marginal_x="box", template="simple_white")
    fig = px.box(data_pd, x="date", y=pd_property_1, color="date", 
    title=f"{pd_property_1} Axle ={axle_count} num of dataset={data_pd.shape[0]}")
    fig.update_layout(title_x=0.5)
    return fig
