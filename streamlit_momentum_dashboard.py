import streamlit as st
import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import json
import os

# Configuración de la página
st.set_page_config(page_title="Momentum Estrategia Dashboard", layout="wide")
st.title("📈 Momentum Estrategia Dashboard (2005-2025)")

# Autenticación con Google Sheets usando secrets
def autenticar_google_sheets():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_dict = {
        "type": "service_account",
        "project_id": st.secrets["gcp_service_account"]["project_id"],
        "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
        "private_key": st.secrets["gcp_service_account"]["private_key"],
        "client_email": st.secrets["gcp_service_account"]["client_email"],
        "client_id": st.secrets["gcp_service_account"]["client_id"],
        "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
        "token_uri": st.secrets["gcp_service_account"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
    }
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client

# Leer datos de Google Sheets
@st.cache_data
def cargar_datos_google_sheets(spreadsheet_url):
    try:
        client = autenticar_google_sheets()
        spreadsheet = client.open_by_url(spreadsheet_url)
        sheet_mes = spreadsheet.worksheet("Por Mes")
        sheet_activo = spreadsheet.worksheet("Por Activo")
        
        # Cargar datos como DataFrame
        df_selecciones = pd.DataFrame(sheet_mes.get_all_records())
        df_metricas_activos = pd.DataFrame(sheet_activo.get_all_records())
        
        # Verificar columnas
        if 'fecha' not in df_selecciones.columns:
            st.error(f"Columna 'fecha' no encontrada en 'Por Mes'. Columnas disponibles: {list(df_selecciones.columns)}")
            st.stop()
        if 'fecha' not in df_metricas_activos.columns:
            st.error(f"Columna 'fecha' no encontrada en 'Por Activo'. Columnas disponibles: {list(df_metricas_activos.columns)}")
            st.stop()
        
        # Depuración: Mostrar primeras filas de 'fecha'
        st.write("Primeras filas de 'fecha' en 'Por Mes':")
        st.write(df_selecciones['fecha'].head())
        st.write("Primeras filas de 'fecha' en 'Por Activo':")
        st.write(df_metricas_activos['fecha'].head())
        
        # Convertir 'fecha' a datetime, manejando errores
        df_selecciones['fecha'] = pd.to_datetime(df_selecciones['fecha'], errors='coerce')
        df_metricas_activos['fecha'] = pd.to_datetime(df_metricas_activos['fecha'], errors='coerce')
        
        # Verificar si hay fechas no parseadas
        if df_selecciones['fecha'].isna().any():
            st.warning(f"Algunas fechas en 'Por Mes' no se pudieron parsear. Filas con NaT: {df_selecciones[df_selecciones['fecha'].isna()]['fecha'].index.tolist()}")
        if df_metricas_activos['fecha'].isna().any():
            st.warning(f"Algunas fechas en 'Por Activo' no se pudieron parsear. Filas con NaT: {df_metricas_activos[df_metricas_activos['fecha'].isna()]['fecha'].index.tolist()}")
        
        return df_selecciones, df_metricas_activos
    except Exception as e:
        st.error(f"Error al cargar datos: {str(e)}")
        raise e

# Cargar spreadsheet_url desde secrets
try:
    spreadsheet_url = st.secrets["google_sheets"]["spreadsheet_url"]
except KeyError:
    st.error("No se encontró 'spreadsheet_url' en st.secrets. Configura los secrets en Streamlit Community Cloud.")
    st.stop()

# Cargar datos
try:
    df_selecciones, df_metricas_activos = cargar_datos_google_sheets(spreadsheet_url)
except Exception as e:
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
