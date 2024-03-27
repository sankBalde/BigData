from dash import html, dash, dcc, Output, Input, State
import pandas as pd
import plotly.graph_objects as go

data = pd.DataFrame({
    'Year': [2019, 2019, 2019, 2020, 2020, 2020, 2021, 2021, 2021],
    'City': ['Paris', 'New York', 'London', 'Paris', 'New York', 'London', 'Paris', 'New York', 'London'],
    'Action': ['Action1', 'Action1', 'Action1', 'Action2', 'Action2', 'Action2', 'Action3', 'Action3', 'Action3'],
    'Value': [100, 120, 110, 90, 110, 100, 130, 100, 120]
})

app = dash.Dash(__name__, title="Boursorama - Dashboard", suppress_callback_exceptions=True)
server = app.server

styles = {
    'color': 'white',
    'border-radius': '20px',
}

img_style = {
    'width': '200px',
    'height': 'auto',
    'borderRadius': '20px',
    'margin': '30px'
}

final_styles = {**styles}

years = [{'label': str(year), 'value': year} for year in data['Year'].unique()]
cities = [{'label': city, 'value': city} for city in data['City'].unique()]
actions = [{'label': action, 'value': action} for action in data['Action'].unique()]

app.layout = html.Div([
    html.Img(src='assets/logoboursorama.jpg', style=img_style),
    html.Div(id='main-container', children=[
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id='year-dropdown',
                    options=years,
                    placeholder='Select Year',
                    clearable=False,
                    style={'width': '150px', 'border-radius': '10px', 'border': '1px solid', 'margin-right': '40px',
                           'color': 'black'}
                ),
            ], style={'margin-right': '20px'}),
            html.Div([
                dcc.Dropdown(
                    id='action-dropdown',
                    options=actions,
                    placeholder='Select Action',
                    clearable=False,
                    style={'width': '150px', 'border-radius': '10px', 'border': '1px solid', 'margin-right': '40px',
                           'color': 'black'},
                ),
            ], style={'margin-right': '20px'}),
            html.Div([
                dcc.Dropdown(
                    id='city-dropdown',
                    options=cities,
                    clearable=False,
                    placeholder='Select City',
                    style={'width': '150px', 'border-radius': '10px', 'border': '1px solid', 'color': 'black'},
                ),
            ]),
        ], style={'margin-bottom': '40px', 'display': 'flex', 'justify-content': 'center'}),
        html.Button('Display Table', id='execute-query', n_clicks=0, style={'border': '1px solid',
                                                                            'display': 'block', 'margin': 'auto',
                                                                            'margin-bottom': '20px'}),
        html.Div(id='query-result-container', style={'text-align': 'center', 'color': 'black', 'margin-top': '20px'}),
        html.Div(id='display-graph-container', style={'margin-top': '20px'}),
        html.Div(id='graph-container', style={'text-align': 'center', 'color': 'black'})
    ])
], style=styles)


@app.callback(
    Output('query-result-container', 'children'),
    [Input('execute-query', 'n_clicks')],
    [State('year-dropdown', 'value'),
     State('action-dropdown', 'value'),
     State('city-dropdown', 'value')]
)
def update_table(n_clicks, selected_year, selected_action, selected_city):
    if n_clicks == 0:
        return ''
    if not selected_year or not selected_action or not selected_city:
        return ''
    filtered_data = data[
        (data['Year'] == selected_year) & (data['Action'] == selected_action) & (data['City'] == selected_city)]
    table = html.Table(
        [html.Tr([html.Th(col) for col in filtered_data.columns])] +
        [html.Tr([html.Td(filtered_data.iloc[i][col]) for col in filtered_data.columns]) for i in
         range(len(filtered_data))]
    )
    return html.Div(table, style={'display': 'inline-block'})


@app.callback(
    Output('display-graph-container', 'children'),
    [Input('execute-query', 'n_clicks')]
)
def display_graph_button(n_clicks):
    if n_clicks == 0:
        return ''
    return html.Button('Display Graphic', id='display-graphic', n_clicks=0,
                       style={'border': '1px solid', 'display': 'block', 'margin': 'auto'})


@app.callback(
    Output('graph-container', 'children'),
    [Input('display-graphic', 'n_clicks')],
    [State('year-dropdown', 'value'),
     State('city-dropdown', 'value')]
)
def draw_graph(n_clicks, selected_year, selected_city):
    if n_clicks == 0:
        return ''
    if not selected_year or not selected_city:
        return ''
    filtered_data = data[(data['Year'] == selected_year) & (data['City'] == selected_city)]
    fig = go.Figure(data=[go.Scatter(x=filtered_data['Year'], y=filtered_data['Value'], mode='lines+markers')])
    return dcc.Graph(figure=fig)


if __name__ == '__main__':
    app.run(debug=True)
