import plotly.express as px
import pandas as pd
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

# Load the data from the Excel file
#df = pd.read_excel("/informacion/DireccionesCompradoresAntiguos.xlsx")
df = pd.read_excel("DireccionesCompradoresAntiguos.xlsx")
#df = pd.read_excel("C:/Users/cesar.gil/Downloads/DireccionesCompradoresAntiguos.xlsx")
df = df.dropna(subset=['CY', 'CX'])
df.fillna({'LOCALIDAD': "Sin información"}, inplace=True)
df['cuenta'] = 1

# Get unique project and locality names for the dropdown filters
project_options = df['PROYECTO'].unique()
locality_options = df['LOCALIDAD'].unique()

# Create the Dash app
app = Dash(__name__)

# Define the app layout
app.layout = html.Div(children=[
    html.Div(children=[  # Wrap dropdowns in a container
        dcc.Dropdown(
            id='project-dropdown',
            options=[{'label': project, 'value': project} for project in project_options],
            # No initial value set (allows empty selection)
            multi=True  # Enable multi-select
        ),
        dcc.Dropdown(
            id='locality-dropdown',
            options=[{'label': locality, 'value': locality} for locality in locality_options],
            # No initial value set (allows empty selection)
            multi=True  # Enable multi-select
        )
    ]),
    dcc.Graph(id='scatter-mapbox-graph')
])

# Update options and scatter mapbox plot based on selections
@app.callback(
    [Output('project-dropdown', 'options'), Output('locality-dropdown', 'options'), Output('scatter-mapbox-graph', 'figure')],
    [Input('project-dropdown', 'value'), Input('locality-dropdown', 'value')]
)
def update_graph(selected_projects, selected_localities):
    # Update project options based on selected localities
    if not selected_localities:
        project_options = df['PROYECTO'].unique()  # Show all projects if no locality selected
    else:
        filtered_df = df[df['LOCALIDAD'].isin(selected_localities)]
        project_options = filtered_df['PROYECTO'].unique()  # Filter projects based on localities

    # Update locality options based on selected projects
    if not selected_projects:
        locality_options = df['LOCALIDAD'].unique()  # Show all localities if no project selected
    else:
        filtered_df = df[df['PROYECTO'].isin(selected_projects)]
        locality_options = filtered_df['LOCALIDAD'].unique()  # Filter localities based on projects

    # Update the scatter mapbox plot based on selections
    if not selected_projects and not selected_localities:
        filtered_df = df.copy()  # Show all data if no selections
    elif selected_projects and not selected_localities:
        filtered_df = df[df['PROYECTO'].isin(selected_projects)]
    elif not selected_projects and selected_localities:
        filtered_df = df[df['LOCALIDAD'].isin(selected_localities)]
    else:
        filtered_df = df[
            (df['PROYECTO'].isin(selected_projects)) &
            (df['LOCALIDAD'].isin(selected_localities))
        ]

    fig = px.scatter_mapbox(
        filtered_df,
        lat=filtered_df['CY'],
        lon=filtered_df['CX'],
        color= filtered_df['PROYECTO'],
        zoom=1,
        mapbox_style="open-street-map"
    )
    #fig.update_traces(cluster=dict(color='#CA005D', enabled=True))
    #fig.update_traces(text=filtered_df['cuenta'], textposition='top center')
    return [
        [{'label': project, 'value': project} for project in project_options],
        [{'label': locality, 'value': locality} for locality in locality_options],
        fig
    ]

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)
