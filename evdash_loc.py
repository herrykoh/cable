import pandas as pd

import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.express as px
from datetime import datetime, timedelta
import math
import logging
import dash_leaflet as dl
import random

WEEKLY_INTERVAL_ON_WEEKDAY = 4  # 4 is Friday

COLOURS = ['Blue', 'Red', 'Green', 'Magenta', 'Violet', 'DarkOliveGreen',
           'SteelBlue', 'Yellow', 'Indigo', 'Aquamarine', 'MediumSlateBlue', 'Fuchsia',
           'LightSeaGreen', 'GoldenRod', 'DarkCyan', 'CadetBlue']


# markerHtmlStyles = {
#   'background-color': "",
#   'width': '3rem',
#   'height': '3rem',
#   'display': 'block',
#   'left': '-1.5rem',
#   'top': '-1.5rem',
#   'position': 'relative',
#   'border-radius': '3rem 3rem 0',
#   'transform': 'rotate(45deg)',
#   'border': '1px solid #FFFFFF'
# }
#



def calc_next_friday(from_date: datetime) -> datetime:
    # except when it is Saturday or Sunday, then return the Friday before
    return from_date + timedelta(days=WEEKLY_INTERVAL_ON_WEEKDAY - from_date.weekday())


t = pd.read_csv("data/by_loc.csv", index_col=0)

# get daterange from start to finish in table and calculate intervals of every Friday
t['import_datestamp'] = pd.to_datetime(t['import_datestamp']).dt.date
all_dates = sorted(t['import_datestamp'].unique())
end_friday = calc_next_friday(all_dates[-1])
num_of_weeks = math.floor((all_dates[-1] - all_dates[0]).days / 7)
all_fridays = [(end_friday - timedelta(days=n * 7)) for n in range(num_of_weeks)]
all_fridays.reverse()

agg_by_loc_count = t.groupby(['operatorName', 'import_datestamp'])['postcode'].count().reset_index().rename(
    columns={'postcode': 'loc_count'})
agg_by_sum_conn = t.groupby(['operatorName', 'import_datestamp'])['numDC'].sum().reset_index()

agg_t = agg_by_loc_count.merge(agg_by_sum_conn, how="inner", on=['operatorName', 'import_datestamp'])

opnames = t['operatorName'].unique()
op_list_items = [{'label': l, 'value': l} for l in opnames]

# associate a colour with each operator
use_colours = COLOURS[:len(opnames)]
random.shuffle(use_colours)
op_colour_dict = dict(zip(opnames, use_colours))

# p = t.pivot(columns='operatorName', index='import_date', values='numDC')

app = dash.Dash(__name__)

app.config.suppress_callback_exceptions = True

#
# slider_style = {'writing-mode': 'vertical-lr', 'text-orientation': 'upright'}
slider_style = {}
slider_marks = {d: {'label': all_fridays[d].strftime('%b-%d'), 'style': slider_style} for d in range(len(all_fridays))}

app.layout = html.Div(children=[html.H1('EV Chargers Growth in the UK'),
                                html.H2('Select Operator:', style={'margin-right': '2em'}),
                                html.Div([
                                    # dcc.RadioItems(opnames, value = "Ionity", id='operator',inline=True)]),
                                    dcc.Checklist(opnames, value=opnames[0], id='operator', inline=False),
                                ], style={'width': '33%', 'display': 'inline-block'}),
                                html.Div([dcc.RadioItems([{'label': 'Number of DCs', 'value': 'numDC'},
                                                          {'label': 'Number of locations', 'value': 'loc_count'}],
                                                         id='gtype', value='numDC')],
                                         style={'width': '33%', 'display': 'inline-block', 'vertical-align': "top"}),
                                html.Div([
                                    dcc.RangeSlider(id="date_slider", min=0, max=len(all_fridays) - 1,
                                                    step=None,
                                                    value=[len(all_fridays) - 2, len(all_fridays) - 1],
                                                    marks=slider_marks, )
                                ], style={'width': '67%', 'align': 'center'}),
                                html.Div([
                                    html.Div([], id='plot1')
                                ]),
                                html.Div([], style={'height': '10vh'}),
                                html.Div([
                                    html.Div([], id='mymap')
                                ]),
                                ])


@app.callback([Output(component_id='plot1', component_property='children'),
               Output(component_id="mymap", component_property="children")],
              [Input(component_id='operator', component_property='value'),
               Input(component_id='gtype', component_property='value'),
               Input(component_id="date_slider", component_property="value")],
              )
def operator_numDC_display(input_operators, graphtype, dateslider):
    # print('Input ops is :' + str(input_operators))

    min_ds, max_ds = dateslider
    min_date = all_fridays[min_ds]
    max_date = all_fridays[max_ds]

    # min_date = datetime(min_date.year, min_date.month, min_date.day)
    # max_date = datetime(max_date.year, max_date.month, max_date.day)

    logging.info(
        "Input date slider is " + str(dateslider) + " and corresponding date is " + str(min_date) + ',' + str(max_date))

    in_ops_list = input_operators
    if isinstance(input_operators, str):
        in_ops_list = input_operators.split(',')

    df = agg_t[(agg_t['operatorName'].isin(in_ops_list)) & (agg_t['import_datestamp'] > min_date) & (
                agg_t['import_datestamp'] <= max_date)]

    locs_df = t[t['operatorName'].isin(in_ops_list) & (t['import_datestamp'] > min_date) & (
                t['import_datestamp'] <= max_date)]

    markers = [dl.Marker(position=[lat,lng], children=[dl.Tooltip(content=name)], icon={'color':op_colour_dict[name]})
               for lat, lng, name in zip(locs_df['lat'], locs_df['lng'], locs_df['operatorName'])]
    markers.insert(0, dl.TileLayer())


    # ops_to_display = ops_to_display[['operatorName', 'import_date', 'numDC']]
    # ops_to_display = p[in_ops_list]

    fig = px.bar(df, x='import_datestamp', y=graphtype, color='operatorName', color_discrete_map=op_colour_dict)

    mymap = dl.Map(markers, center=[51.5,-0.1], style={'height': '70vh'}, zoom=8, id="mymap")

    return [dcc.Graph(figure=fig), mymap]


if __name__ == '__main__':
    app.run_server()
