# !pip install streamlit pandas plotly gspread google-auth

import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# Configuración de la página
st.set_page_config(page_title="Momentum Estrategia Dashboard", layout="wide")
st.title("📈 Momentum Estrategia Dashboard (2005-2025)")

# Autenticación con Google Sheets
def autenticar_google_sheets(creds_file):
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    client = gspread.authorize(creds)
    return client

# Leer datos de Google Sheets
@st.cache_data
def cargar_datos_google_sheets(spreadsheet_url, creds_file):
    client = autenticar_google_sheets(creds_file)
    spreadsheet = client.open_by_url(spreadsheet_url)
    sheet_mes = spreadsheet.worksheet("Por Mes")
    sheet_activo = spreadsheet.worksheet("Por Activo")
    df_selecciones = pd.DataFrame(sheet_mes.get_all_records())
    df_metricas_activos = pd.DataFrame(sheet_activo.get_all_records())
    # Convertir columnas necesarias
    df_selecciones['fecha'] = pd.to_datetime(df_selecciones['fecha'])
    df_metricas_activos['fecha'] = pd.to_datetime(df_metricas_activos['fecha'])
    return df_selecciones, df_metricas_activos

# Configuración de credenciales y URL de la hoja
creds_file = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento/importfromapi-33fbea8a3fc7.json'
spreadsheet_url = st.text_input("Ingresa la URL de la Google Sheet:", "https://docs.google.com/spreadsheets/d/1iT2Sel3bfjt8FAAeQ4O4LRI2_zaBiuRD_sJseT6oqFU")
if not spreadsheet_url.startswith("https://docs.google.com/spreadsheets/"):
    st.error("Por favor, ingresa una URL válida de Google Sheets.")
    st.stop()

# Cargar datos
try:
    df_selecciones, df_metricas_activos = cargar_datos_google_sheets(spreadsheet_url, creds_file)
except Exception as e:
    st.error(f"Error al cargar datos: {e}")
    st.stop()

# Sidebar para filtros
st.sidebar.header("Filtros")
fecha_inicio = st.sidebar.date_input("Fecha Inicio", datetime(2005, 5, 31))
fecha_fin = st.sidebar.date_input("Fecha Fin", datetime(2025, 4, 30))
activos_disponibles = df_metricas_activos['activo'].unique()
activo_seleccionado = st.sidebar.multiselect("Seleccionar Activos", activos_disponibles, default=activos_disponibles[:3])

# Filtrar datos
df_selecciones_filtrado = df_selecciones[(df_selecciones['fecha'] >= pd.to_datetime(fecha_inicio)) & 
                                        (df_selecciones['fecha'] <= pd.to_datetime(fecha_fin))]
df_metricas_filtrado = df_metricas_activos[(df_metricas_activos['fecha'] >= pd.to_datetime(fecha_inicio)) & 
                                          (df_metricas_activos['fecha'] <= pd.to_datetime(fecha_fin)) & 
                                          (df_metricas_activos['activo'].isin(activo_seleccionado))]

# Pestañas para navegación
tab1, tab2 = st.tabs(["Por Mes", "Por Activo"])

# Pestaña "Por Mes"
with tab1:
    st.header("Resultados Mensuales")
    
    # KPI: Rentabilidad Acumulada
    rentabilidad_acumulada = (1 + df_selecciones_filtrado['rentabilidad_mensual']).cumprod().iloc[-1] - 1
    sharpe_promedio = df_selecciones_filtrado['sharpe'].mean()
    col1, col2 = st.columns(2)
    col1.metric("Rentabilidad Acumulada", f"{rentabilidad_acumulada:.2%}")
    col2.metric("Sharpe Promedio", f"{sharpe_promedio:.2f}")
    
    # Gráfico: Capitalización Final
    fig_capital = px.line(df_selecciones_filtrado, x='fecha', y='capitalizacion_final', 
                         title="Evolución de la Capitalización Final")
    st.plotly_chart(fig_capital, use_container_width=True)
    
    # Gráfico: Rentabilidad Mensual
    fig_rentabilidad = px.bar(df_selecciones_filtrado, x='fecha', y='rentabilidad_mensual', 
                             title="Rentabilidad Mensual")
    st.plotly_chart(fig_rentabilidad, use_container_width=True)
    
    # Tabla de datos
    st.subheader("Datos Mensuales")
    st.dataframe(df_selecciones_filtrado)

# Pestaña "Por Activo"
with tab2:
    st.header("Métricas por Activo")
    
    # Gráfico: Momentum Score por Activo
    fig_momentum = px.line(df_metricas_filtrado, x='fecha', y='momentum_score', color='activo', 
                          title="Momentum Score por Activo")
    st.plotly_chart(fig_momentum, use_container_width=True)
    
    # Gráfico: Volatilidad Corta vs Larga
    fig_volatilidad = px.scatter(df_metricas_filtrado, x='volatilidad_corta', y='volatilidad_larga', 
                                color='activo', size='retorno_activo', hover_data=['fecha', 'activo'],
                                title="Volatilidad Corta vs Larga")
    st.plotly_chart(fig_volatilidad, use_container_width=True)
    
    # Tabla de datos
    st.subheader("Datos por Activo")
    st.dataframe(df_metricas_filtrado)

st.sidebar.markdown("Creado con [Streamlit](https://streamlit.io/)")