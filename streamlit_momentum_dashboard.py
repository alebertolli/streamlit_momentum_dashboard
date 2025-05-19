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
    # Cargar credenciales desde st.secrets
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
    client = autenticar_google_sheets()
    spreadsheet = client.open_by_url(spreadsheet_url)
    sheet_mes = spreadsheet.worksheet("Por Mes")
    sheet_activo = spreadsheet.worksheet("Por Activo")
    df_selecciones = pd.DataFrame(sheet_mes.get_all_records())
    df_metricas_activos = pd.DataFrame(sheet_activo.get_all_records())
    df_selecciones['fecha'] = pd.to_datetime(df_selecciones['fecha'])
    df_metricas_activos['fecha'] = pd.to_datetime(df_metricas_activos['fecha'])
    return df_selecciones, df_metricas_activos

# Cargar spreadsheet_url desde secrets
spreadsheet_url = st.secrets["google_sheets"]["spreadsheet_url"]

# Cargar datos
try:
    df_selecciones, df_metricas_activos = cargar_datos_google_sheets(spreadsheet_url)
except Exception as e:
    st.error(f"Error al cargar datos: {e}")
    st.stop()

# Resto del código (filtros, pestañas, gráficos) permanece igual
# [Sidebar, Tabs, Visualizations, etc.]
