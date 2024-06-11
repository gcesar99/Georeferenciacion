# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 10:37:53 2024

@author: cesar.gil
"""

from flask import Flask
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import dash
from dash import Dash, html, dcc, dash_table
from dash.dependencies import Input, Output
import geopandas as gpd
import geojson
import numpy as np
import folium
from folium.plugins import MarkerCluster
import plotly.graph_objects as go
import os
import sys

# DEFINE THE DATABASE CREDENTIALS
user = 'usr_bi'
password = 'YS4Wh4thE79a'
host = 'msql-crm-eastus2-prod-01.mysql.database.azure.com'
port = 3306
database = 'alcabama_crm'
 
# PYTHON FUNCTION TO CONNECT TO THE MYSQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
cnx =create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        ), pool_pre_ping=True
    )
sql_query = '''
SELECT resultadoFinal.ID_contacto, contactoNombreCompleto, contactoTipoIdentificacion, contactoDocumento, contactoEmail, contactoCelular,
	contactoCiudadNombre, UPPER(TRIM(contactoLocalidadResidencia)) as contactoLocalidadResidencia, contactoNivelAcademicoNombre, contactoProfesionNombre, contactoOcupacionNombre, contactoFechaNacimiento, contactoEdad,
	if ( contactoEdad is null, "Sin informacion", if( contactoEdad < 18, "A. Menor a 18 Años", if( contactoEdad <= 25, "B. 18-25 Años", if( contactoEdad <= 30, "C. 25-30 Años", if( contactoEdad <= 35, "D. 30-35 Años", if( contactoEdad <= 40, "E. 35-40 Años", if( contactoEdad <= 50, "F. 40-50 Años", if( contactoEdad <= 60, "G. 50-60 Años", "H. Mayor a 60 Años" ) ) ) ) ) ) ) ) as rangoEdad,
	ID_oportunidad, UPPER(TRIM(macroproyectoNombre)) as macroproyectoNombre, oportunidadEstado, rangoIngresosFamiliares, oportunidadFinalidadCompra, oportunidadMedioDeAtencion, oportunidadDondeNosVio,
	oportunidadPrimeraVivienda, direccionCompleta, concat(direccionCompleta, ', ', contactoCiudadNombre) as direccionFinal
FROM ( 
SELECT * FROM (
WITH direcciones AS (
    SELECT
        distinct(C.ID_contacto), CONCAT(UPPER(TRIM(C.contactoNombre)),' ', UPPER(TRIM(C.contactoApellido))) AS contactoNombreCompleto, C.contactoTipoIdentificacionDescripcion AS contactoTipoIdentificacion, C.contactoDocumento, C.contactoEmail, C.contactoCelular,	
		C.contactoDireccionResidencia, C.contactoCiudadNombre, C.contactoLocalidadResidencia, c.contactoNivelAcademicoNombre, C.contactoProfesionNombre, C.contactoOcupacionNombre, C.contactoFechaNacimiento, ROUND( DATEDIFF(DATE(NOW()) , C.contactoFechaNacimiento)/365, 1) as contactoEdad,
        REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', 1), ',', 1), ':', -1), '"', '') AS direccionTipo,
        REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', -5), ',', 1), ':', -1), '"', '') AS direccionNumero,
        REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', -4), ',', 1), ':', -1), '"', '') AS direccionCon,
        REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', -3), ',', 1), ':', -1), '"', '') AS direccionNomenclatura,
        REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', -2), ',', 1), ':', -1), '"', '') AS direccionInterior,
        REPLACE(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(C.contactoDireccionResidencia, ',', -1), ',', 1), ':', -1), '"', ''), '}', '') AS direccionOtroTipo
    FROM
        contactos C
)
SELECT
    distinct(ID_contacto), contactoNombreCompleto, contactoTipoIdentificacion, contactoDocumento, contactoEmail, contactoCelular, contactoCiudadNombre, contactoLocalidadResidencia, contactoNivelAcademicoNombre, contactoProfesionNombre, contactoOcupacionNombre, contactoFechaNacimiento, contactoEdad,
    RTRIM(LTRIM(regexp_replace( REPLACE(REPLACE(CONCAT_WS(' ', direccionTipo, direccionNumero, direccionCon, direccionNomenclatura, direccionInterior, direccionOtroTipo), 'OTRO TIPO', ''), 'N/A', ''), '[[:space:]]+', ' ') ))  AS direccionCompleta
FROM
    direcciones
) resultado WHERE direccionCompleta IS NOT null and direccionCompleta <> ""
) resultadoFinal
left join 
	(
		select o.ID_contacto, O.ID_oportunidad, m.macroproyectoNombre, oe.oportunidadEstado, o.oportunidadRangoIngresosFamiliaresComoSmmlv as rangoIngresosFamiliares, o.oportunidadFinalidadCompra, O.oportunidadMedioDeAtencion, o.oportunidadDondeNosVio, o.oportunidadMotivoDescarte, O.oportunidadPrimeraVivienda 
		from oportunidades o
			left join oportunidades_estados oe  on o.ID_oportunidadEstado = oe.ID_oportunidadEstado
			left join macroproyectos m on o.ID_macroproyecto = m.ID_macroproyecto 
		where m.macroproyectoNombre not like '%%PRUEBAS CRM'
		order by O.ID_oportunidad desc
	) infOportunidades on resultadoFinal.ID_contacto = infOportunidades.ID_contacto
'''

# Crear el dataframe con la consulta SQL
df = pd.read_sql(sql = sql_query, con = cnx)

# Cerrar la conexión a la base de datos
cnx.dispose()

def obtener_ruta_archivo(nombre_archivo):
    # Obtener la ruta del directorio donde se encuentra el script
    ruta_script = sys.argv[0]
    directorio_script = os.path.dirname(ruta_script)
    # Combinar la ruta del directorio del script con el nombre del archivo
    ruta_archivo = os.path.join(directorio_script, nombre_archivo)
    return ruta_archivo

with open(obtener_ruta_archivo('assets\poligonos-localidades.geojson')) as f:
    gj = geojson.load(f)

with open(obtener_ruta_archivo('assets\poligonos-localidades_espacios.geojson')) as f:
    gjPrueba = geojson.load(f)

# Importar el dataframe para pintar los polígonos de las localidades de Bogotá
df_localidades = pd.read_excel(obtener_ruta_archivo('assets\poligonos-localidades.xlsx'))

# Crear el dataframe con las coordenadas
dfCoordenadas = pd.read_excel(obtener_ruta_archivo('assets\ResidenciaActualizadaNEXO.xlsx'), sheet_name='infoCoordenadas')

# Crear el dataframe de las Salas de Ventas
dfSalas = pd.read_excel(obtener_ruta_archivo('assets\GeoreferenciacionBaseSalas.xlsx'))

# Left Join para extraer las coordenadas
df = pd.merge(df, dfCoordenadas.drop(["direccionCompleta", "direccionFinal"], axis='columns').drop_duplicates(), how='left', on='ID_contacto')

# Dataframe Compradores Antiguos
df_compradoresAntiguos = pd.read_excel(obtener_ruta_archivo('assets\GeoreferenciaCompradoresAntiguos.xlsx'))

# Anexar Dataframes de compradores (Antiguos y NEXO)
frames = [df, df_compradoresAntiguos]
df = pd.concat(frames, ignore_index=True)

# Tratamiento de datos
columnasParaRemplazar = [ 'contactoCiudadNombre', 'contactoLocalidadResidencia', 'contactoNivelAcademicoNombre', 'contactoProfesionNombre', 'contactoOcupacionNombre', 'contactoEdad', 'rangoEdad',  'macroproyectoNombre', 'oportunidadEstado', 'rangoIngresosFamiliares', 'oportunidadFinalidadCompra', 'oportunidadMedioDeAtencion', 'oportunidadDondeNosVio', 'oportunidadPrimeraVivienda']
df.fillna( { columna: "Sin informacion" for columna in columnasParaRemplazar }, inplace=True) # Reemplazar valores nulos en las columnas deseadas
df[columnasParaRemplazar] = df[columnasParaRemplazar].replace("", "Sin informacion") # Reemplazar valores vacíos en las columnas deseadas
df = df.query('Coordenadas != "Sin informacion"') # Seleccionar las filas que tienen Coordenadas
df = df.dropna(subset=['CY', 'CX'])
df['cuenta'] = 1

# Get unique names for the dropdown filters
project_options = df['macroproyectoNombre'].sort_values().unique()
city_options = df['contactoCiudadNombre'].sort_values().unique()
locality_options = df['contactoLocalidadResidencia'].sort_values().unique()
edad_options = df['rangoEdad'].unique()
profesion_options = df['contactoProfesionNombre'].sort_values().unique()
ocupacion_options = df['contactoOcupacionNombre'].sort_values().unique()
estado_options = df['oportunidadEstado'].sort_values().unique()
ingresos_options = df['rangoIngresosFamiliares'].sort_values().unique()
finalidadCompra_options = df['oportunidadFinalidadCompra'].sort_values().unique()
lugarVista_options = df['oportunidadDondeNosVio'].sort_values().unique()

server = Flask(__name__)

app = dash.Dash(__name__, server=server)

# Create the Dash app
#app = Dash(__name__)

# Define the app layout
app.layout = html.Div(
    style={'display': 'flex', 'flex-direction': 'column'},  # Center content vertically
    children=[
        html.Div(
            style={
                'display': 'flex',
                'flex-direction': 'row',  # Arrange image and title horizontally
                'align-items': 'center',  # Align vertically
                'width': '100%',  # Full width
            },
            children=[
                html.Img(id='logo', src=obtener_ruta_archivo('assets/Alcabama.png'), style={'height': '40px'}, alt="Nexo Logo"),  # Replace 'your_image.png' with your image name
                html.H1('GEORREFERENCIACIÓN ALCABAMA', style={'margin-left': '500px'})  # Add your title
            ]
        ),
        html.Div(
            style={
                'display': 'grid',
                'grid-template-columns': 'repeat(2, 1fr)',  # Two columns
                'grid-gap': '10px',  # Spacing between filters
                'width': '100%',
            },
            children=[
                dcc.Dropdown(
                    id='project-dropdown',
                    options=[{'label': project, 'value': project} for project in project_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="MACROPROYECTO"
                ),
                dcc.Dropdown(
                    id='estado-dropdown',
                    options=[{'label': estado, 'value': estado} for estado in estado_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="ESTADO OPORTUNIDAD"
                ),
                dcc.Dropdown(
                    id='city-dropdown',
                    options=[{'label': city, 'value': city} for city in city_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="CIUDAD"
                ),
                dcc.Dropdown(
                    id='locality-dropdown',
                    options=[{'label': locality, 'value': locality} for locality in locality_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="LOCALIDAD"
                ),
                dcc.Dropdown(
                    id='edad-dropdown',
                    options=[{'label': edad, 'value': edad} for edad in edad_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="EDAD"
                ),
                dcc.Dropdown(
                    id='ingresos-dropdown',
                    options=[{'label': ingresos, 'value': ingresos} for ingresos in ingresos_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="INGRESOS"
                ),
                dcc.Dropdown(
                    id='finalidad-dropdown',
                    options=[{'label': finalidadCompra, 'value': finalidadCompra} for finalidadCompra in finalidadCompra_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="FINALIDAD COMPRA"
                ),
                dcc.Dropdown(
                    id='lugarVista-dropdown',
                    options=[{'label': lugarVista, 'value': lugarVista} for lugarVista in lugarVista_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="DONDE NOS VIO?"
                ),
                dcc.Dropdown(
                    id='profesion-dropdown',
                    options=[{'label': profesion, 'value': profesion} for profesion in profesion_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="PROFESIÓN"
                ),
                dcc.Dropdown(
                    id='ocupacion-dropdown',
                    options=[{'label': ocupacion, 'value': ocupacion} for ocupacion in ocupacion_options],
                    # No initial value set (allows empty selection)
                    multi=True,
                    placeholder="OCUPACIÓN"
                ),
            ]
        ),
        html.Div([
            dcc.Tabs(id='tabs-graph', value='scatter-graph',
                     children=[
                         dcc.Tab(label='Nube de Puntos', value='scatter-graph'),
                         dcc.Tab(label='Puntos Agrupados', value='cluster-graph'),
                         dcc.Tab(label='Heat Map', value='heatmap-graph')
                     ]
            ),
            html.Div(id='tabs-content-graph')
        ]),
        html.Div(id='content-table')
        #html.Div([
        #    html.Button("Download CSV", id="btn_csv"),
        #    html.Button("Download Excel", id="btn_excel"),
        #    dcc.Download(id="download_data")
        #])
    ]
)

# Asignar color de fondo a los polígonos de localidades (aplica para la vista de PUNTOS GRUPADOS)
def colorLocalidad(feature):
    colores={
        "CIUDAD BOLIVAR": "#636EFA",
        "SUBA": "#EF553B",
        "RAFAEL URIBE URIBE": "#00CC96",
        "KENNEDY": "#AB63FA",
        "USME": "#AB63FA",
        "LOS MARTIRES": "#19D3F3",
        "ANTONIO NARIÑO": "#FF6692",
        "TEUSAQUILLO": "#B6E880",
        "SUMAPAZ": "#FF97FF",
        "SAN CRISTOBAL": "#FECB52",
        "USAQUEN": "#636EFA",
        "TUNJUELITO": "#EF553B",
        "BOSA": "#00CC96",
        "PUENTE ARANDA": "#AB63FA",
        "SANTA FE": "#FFA15A",
        "BARRIOS UNIDOS": "#19D3F3",
        "FONTIBON": "#FF6692",
        "ENGATIVA": "#B6E880",
        "CANDELARIA": "#FF97FF",
        "CHAPINERO": "#FECB52"
    }
    return{
        'color':'#000000',
        'weight': 1,
        'opacity': 0.2,
        'fillColor': colores.get(feature['properties']['NombreDeLaLocalidad'], "#CA005D"),
        'fillOpacity': 0.2,
    }

# Asignar color de fondo a los polígonos de las SALAS DE VENTAS (aplica para la vista de PUNTOS GRUPADOS)
def colorSalasVentas(feature):
    colores={
        "PROYECTO ANKARA": "#636EFA",
        "PROYECTO BELLALUNA": "#EF553B",
        "PROYECTO AKANTI": "#00CC96",
        "PROYECTO ARUMA": "#AB63FA",
        "PROYECTO CIIEN": "#AB63FA",
        "PROYECTO AKAI 95": "#19D3F3",
        "PROYECTO BLUE": "#FF6692",
        "PROYECTO ORANGE": "#B6E880",
        "PROYECTO CORAL": "#FF97FF",
        "PROYECTO SUN": "#FECB52",
        "PROYECTO IRIS": "#636EFA",
        "PROYECTO AMARO": "#EF553B",
        "PROYECTO ARBORE": "#00CC96",
        "PROYECTO MAGNOLIAS": "#AB63FA",
        "PROYECTO MADERO": "#FFA15A",
        "PROYECTO AMALFI": "#19D3F3",
        "PROYECTO LA QUINTA 3": "#FF6692",
        "PROYECTO VENTURA": "#B6E880",
        "PROYECTO ARMONIA": "#FF97FF",
        "PROYECTO VENTURA": "#FECB52"
    }
    return{
        'color':'#000000',
        'weight': 1,
        'opacity': 0.2,
        'fillColor': colores.get(feature['properties']['PROYECTO'], "#CA005D"),
        'fillOpacity': 0.45,
    }

# Crear GeoJson para trazar los polígonos (buffer alrededor de las Salas de Ventas), es una aproximación discreta y dibuja actualmente 40 segmentos para crear el círculo
def create_circle(lat, lon, radius):
    circle_points = []
    for theta in np.linspace(0, 2*np.pi, 40):
        d_lat = np.cos(theta) * (radius / 111111)
        d_lon = np.sin(theta) * (radius / (111111 * np.cos(np.radians(lat))))
        circle_points.append((lon + d_lon, lat + d_lat))  # Reordenamos para (lon, lat)
    return circle_points
features_salas = []
for i, row in dfSalas.iterrows():
    circle_coords = create_circle(row["CY"], row["CX"], 1000)  # 1000 metros de radio
    circle = geojson.Polygon([circle_coords])
    feature = geojson.Feature(geometry=circle, properties={"PROYECTO": row["PROYECTO"], "ID_SERVI": row["ID_SERVI"]})
    features_salas.append(feature)
geojson_salasVentas = geojson.FeatureCollection(features_salas)

# Update options and scatter mapbox plot based on selections
@app.callback(
    [
        Output('project-dropdown', 'options'),
        Output('estado-dropdown', 'options'),
        Output('city-dropdown', 'options'),
        Output('locality-dropdown', 'options'),
        Output('edad-dropdown', 'options'),
        Output('ingresos-dropdown', 'options'),
        Output('finalidad-dropdown', 'options'),
        Output('lugarVista-dropdown', 'options'),
        Output('profesion-dropdown', 'options'),
        Output('ocupacion-dropdown', 'options'),
        Output('tabs-content-graph', 'children'),
        Output('content-table', 'children')
    ],
    [
        Input('project-dropdown', 'value'),
        Input('estado-dropdown', 'value'),
        Input('city-dropdown', 'value'),
        Input('locality-dropdown', 'value'),
        Input('edad-dropdown', 'value'),
        Input('ingresos-dropdown', 'value'),
        Input('finalidad-dropdown', 'value'),
        Input('lugarVista-dropdown', 'value'),
        Input('profesion-dropdown', 'value'),
        Input('ocupacion-dropdown', 'value'),
        Input('tabs-graph', 'value')
    ]
)

def update_graph(selected_projects, selected_estados, selected_cities, selected_localities, selected_edades, selected_ingresos, selected_finalidades, selected_lugaresVista, selected_profesiones, selected_ocupaciones, tab):
    # Update project options based on the other filters selection
    filtered_df = df.copy()
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)] 
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)] 
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    project_options = filtered_df['macroproyectoNombre'].sort_values().unique()

    # Update city options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]  
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    city_options = filtered_df['contactoCiudadNombre'].sort_values().unique()

    # Update locality options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)] 
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    locality_options = filtered_df['contactoLocalidadResidencia'].sort_values().unique()

    # Update edad options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)] 
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)] 
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    edad_options = filtered_df['rangoEdad'].sort_values().unique()

    # Update profesion options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)] 
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    profesion_options = filtered_df['contactoProfesionNombre'].sort_values().unique()

    # Update ocupacion options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)] 
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    ocupacion_options = filtered_df['contactoOcupacionNombre'].sort_values().unique()

    # Update finalidadCompra options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)] 
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    finalidadCompra_options = filtered_df['oportunidadFinalidadCompra'].sort_values().unique()

    # Update estadoOportunidad options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)] 
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    estado_options = filtered_df['oportunidadEstado'].sort_values().unique()

    # Update rangoIngresosFamiliares options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    ingresos_options = filtered_df['rangoIngresosFamiliares'].sort_values().unique()

    # Update oportunidadDondeNosVio options based on the other filters selection
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    lugarVista_options = filtered_df['oportunidadDondeNosVio'].sort_values().unique()

    # Update the scatter mapbox plot based on selections
    filtered_df = df.copy()
    if selected_projects:
        filtered_df = filtered_df[filtered_df['macroproyectoNombre'].isin(selected_projects)]
    if selected_estados:
        filtered_df = filtered_df[filtered_df['oportunidadEstado'].isin(selected_estados)]
    if selected_cities:
        filtered_df = filtered_df[filtered_df['contactoCiudadNombre'].isin(selected_cities)]
    if selected_localities:
        filtered_df = filtered_df[filtered_df['contactoLocalidadResidencia'].isin(selected_localities)]
    if selected_edades:
        filtered_df = filtered_df[filtered_df['rangoEdad'].isin(selected_edades)]
    if selected_ingresos:
        filtered_df = filtered_df[filtered_df['rangoIngresosFamiliares'].isin(selected_ingresos)]
    if selected_finalidades:
        filtered_df = filtered_df[filtered_df['oportunidadFinalidadCompra'].isin(selected_finalidades)]
    if selected_lugaresVista:
        filtered_df = filtered_df[filtered_df['oportunidadDondeNosVio'].isin(selected_lugaresVista)]
    if selected_profesiones:
        filtered_df = filtered_df[filtered_df['contactoProfesionNombre'].isin(selected_profesiones)]
    if selected_ocupaciones:
        filtered_df = filtered_df[filtered_df['contactoOcupacionNombre'].isin(selected_ocupaciones)]

    # Crear la capa que contiene las LOCALIDADES de Bogotá
    fig_geojson_localidades = folium.GeoJson(
        gjPrueba,
        style_function=colorLocalidad,
        tooltip=folium.GeoJsonTooltip(fields=['NombreDeLaLocalidad'])
    )
    fig_localidades  = px.choropleth_mapbox(
        df_localidades,
        geojson=gj,
        color=df_localidades["Nombre de la localidad"],
        locations=df_localidades["Nombre de la localidad"],
        featureidkey='properties.Nombre de la localidad',
        opacity=0.1,
        height=700,
        zoom=9,
        center= {"lat": 4.66, "lon": -74.4931},
        mapbox_style="open-street-map"                          
    )
    fig_localidades.update_traces(showlegend=False)
    fig_localidades.update_layout(legend_title="PROYECTO")

    # Crear la capa que contiene las ubicaciones de las SALAS DE VENTAS
    fig_geojson_salas = folium.GeoJson(
        geojson_salasVentas,
        style_function=colorSalasVentas,
        tooltip=folium.GeoJsonTooltip(fields=['PROYECTO'])
    )
    fig_salas = px.choropleth_mapbox(
        dfSalas,
        geojson=geojson_salasVentas,
        color="PROYECTO",
        locations="ID_SERVI",
        featureidkey='properties.ID_SERVI',
        opacity=0.45,
        mapbox_style="open-street-map",
        hover_data={'ID_SERVI': False, 'PROYECTO': True}
    )
    fig_salas.update_traces(showlegend=False)

    # Crear la figura de Scatter (NUBE DE PUNTOS)
    fig_scatter = px.scatter_mapbox(
        filtered_df,
        lat=filtered_df['CY'],
        lon=filtered_df['CX'],
        color= filtered_df['macroproyectoNombre'],
        zoom=5,
        mapbox_style="open-street-map",
        hover_data={'CY': False, 'CX': False, 'macroproyectoNombre': True}
    )

    # Crear la figura de Cluster (PUNTOS AGRUPADOS)
    fig_cluster = folium.Map(location=[4.65495, -74.1077], zoom_start=10)
    cluster = MarkerCluster(locations=filtered_df[['CY', 'CX']])

    # Crar la figura para usar como Heat Map
    conversion_factor = 0.01  # 1 meter = 0.01 marker radius
    fig_heatmap = px.density_mapbox(
        filtered_df,
        lat=filtered_df['CY'],
        lon=filtered_df['CX'],
        z=filtered_df['cuenta'],  # Assuming 'cuenta' represents point density
        radius= 500 * conversion_factor,  # Scaled radius
        zoom=1,
        mapbox_style="open-street-map"
    )

    filtered_df = filtered_df.rename(columns={
        'ID_contacto': 'idContacto',
        'contactoNombreCompleto': 'Nombre',
        'contactoTipoIdentificacion': 'Tipo Documento',
        'contactoDocumento': 'Documento',
        'contactoEmail': 'Email',
        'contactoCelular': 'Celular',
        'contactoCiudadNombre': 'Ciudad',
        'contactoLocalidadResidencia': 'Localidad',
        'contactoNivelAcademicoNombre': 'Nivel Academico',
        'contactoProfesionNombre': 'Profesion',
        'contactoOcupacionNombre': 'Ocupacion',
        'contactoEdad': 'Edad',
        'rangoEdad': 'Rango Edad',
        'macroproyectoNombre': 'Proyecto',
        'oportunidadEstado': 'Estado Oportunidad',
        'rangoIngresosFamiliares': 'Ingresos',
        'oportunidadFinalidadCompra': 'Motivo Compra',
        'oportunidadMedioDeAtencion': 'Medio Atencion',
        'oportunidadDondeNosVio': 'Donde nos Vio',
        'oportunidadPrimeraVivienda': 'Primera Vivienda',
        'direccionCompleta': 'Direccion Residencia',
        'NIVEL SOCIOECONOMICO': 'Estrato',
        'BARRIO': 'Barrio'
    })

    filtered_df = filtered_df.reindex(['idContacto', 'Tipo Documento', 'Documento', 'Nombre', 'Edad', 'Rango Edad', 'Celular', 'Email','Proyecto', 'Estado Oportunidad', 'Ingresos', 'Motivo Compra',
                                       'Primera Vivienda', 'Nivel Academico', 'Ocupacion', 'Profesion', 'Donde nos Vio', 'Medio Atencion', 'Ciudad', 'Localidad', 'Direccion Residencia'], axis = 1)

    tablaDetalle = dash_table.DataTable(
        data=filtered_df.to_dict('records'),
        columns=[{'id': c, 'name': c} for c in filtered_df.columns],
        css=[
            {
                "selector": ".previous-page, "
                ".next-page, .first-page, "
                ".last-page, .export, .show-hide",
                "rule": "color: black;",
            },
            {
                "selector": ".current-page",
                "rule": "padding-right: 5px;",
            },
        ],
        fixed_rows={'headers': True, 'data': 0},
        style_header={
            "backgroundColor": "#CA005D",
            "fontWeight": "bold",
            "color": "white",
            "textAlign": "center",
        },
        style_cell={
            "whiteSpace": "normal",
            "height": "auto",
            "textAlign": "center",
        },
        style_table={'height': '300px', 'overflowX': 'auto', 'overflowY': 'auto'},
        sort_action="native",
        filter_action="native",
        export_format="xlsx",
    )

    # Mostrar la figura determinada dependiendo de la pestaña seleccionada
    if tab == 'scatter-graph':
        for trace in fig_scatter.data:
            fig_salas.add_trace(trace)
        for trace in fig_salas.data:
            fig_localidades.add_trace(trace)
        final = dcc.Graph(id='scatter-map', figure=fig_localidades)
    elif tab == 'heatmap-graph': 
        for trace in fig_heatmap.data:
            fig_salas.add_trace(trace)
        for trace in fig_salas.data:
            fig_localidades.add_trace(trace)
        final = dcc.Graph(id='heatmap-map', figure=fig_localidades)
    else:
        fig_cluster.add_child(cluster)
        fig_cluster.add_child(fig_geojson_localidades)
        fig_cluster.add_child(fig_geojson_salas)
        final = html.Iframe(
            srcDoc=fig_cluster._repr_html_(),
            width="100%",
            height="600px",  # Adjust height as needed
            style={'border': 'none'}
        )

    return [
        [{'label': project, 'value': project} for project in project_options],
        [{'label': estado, 'value': estado} for estado in estado_options],
        [{'label': city, 'value': city} for city in city_options],
        [{'label': locality, 'value': locality} for locality in locality_options],
        [{'label': edad, 'value': edad} for edad in edad_options],
        [{'label': ingreso, 'value': ingreso} for ingreso in ingresos_options],
        [{'label': finalidad, 'value': finalidad} for finalidad in finalidadCompra_options],
        [{'label': lugar, 'value': lugar} for lugar in lugarVista_options],
        [{'label': profesion, 'value': profesion} for profesion in profesion_options],
        [{'label': ocupacion, 'value': ocupacion} for ocupacion in ocupacion_options],
        final,
        tablaDetalle
    ]

# Run the Dash app
if __name__ == '__main__':
    server.run(debug=False, port = 8051)
    #app.run_server(debug=False, port = 8051)