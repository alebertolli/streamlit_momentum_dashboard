# 1. Importar Librerías
import pandas as pd
import numpy as np
import sqlite3
from sqlite3 import Error
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.colab import drive
import os
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 2. Montar Google Drive y Configurar Credenciales
def montar_drive():
    drive.mount('/content/drive', force_remount=True)
    output_dir = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento'
    os.makedirs(output_dir, exist_ok=True)
    print(f"Directorio de salida: {output_dir}")
    return output_dir

def autenticar_google_sheets(creds_file):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        client = gspread.authorize(creds)
        print("Autenticación exitosa con Google Sheets")
        return client, creds
    except Exception as e:
        print(f"Error al autenticar con Google Sheets: {e}")
        return None, None

# 3. Obtener o Crear Carpeta en Google Drive
def obtener_o_crear_carpeta(creds, folder_path, parent_folder='root'):
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        folder_name = folder_path.split('/')[-1]
        folder_query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        # Buscar la carpeta
        response = drive_service.files().list(
            q=folder_query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=10
        ).execute()
        
        folders = response.get('files', [])
        if folders:
            folder_id = folders[0]['id']
            print(f"Carpeta encontrada: {folder_name}, ID: {folder_id}")
            return folder_id
        
        # Crear la carpeta si no existe
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder]
        }
        folder = drive_service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()
        folder_id = folder.get('id')
        print(f"Carpeta creada: {folder_name}, ID: {folder_id}")
        return folder_id
    except HttpError as e:
        print(f"Error al obtener o crear carpeta: {e}")
        return None

# 4. Crear Conexión a la Base de Datos
def crear_conexion(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
    return conn

# 5. Obtener Lista de Activos

def obtener_activos(db_file):
       activos = ['SPY', 'QQQ', 'GLD', 'EEM', 'FXI', 'XLF', 'XLC', 'IEUR', 'XLY', 'VEA', 'XLRE', 'XLB', 'IVE', 'IVW']
       return activos

"""
def obtener_activos(db_file):
    conn = crear_conexion(db_file)
    if conn is None:
        return []
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        activos = [row[0] for row in c.fetchall()]
        conn.close()
        return activos
    except sqlite3.Error as e:
        print(f"Error al obtener activos: {e}")
        conn.close()
        return []
"""
# 6. Leer Datos de un Activo
def leer_datos_activo(db_file, activo, fecha_inicio, fecha_fin):
    conn = crear_conexion(db_file)
    if conn is None:
        return None
    try:
        query = f"""
            SELECT date, adj_close AS Adj_Close
            FROM {activo}
            WHERE date BETWEEN ? AND ?
            ORDER BY date
        """
        df = pd.read_sql_query(query, conn, params=(fecha_inicio, fecha_fin), parse_dates=['date'])
        df.set_index('date', inplace=True)
        conn.close()
        return df
    except sqlite3.Error as e:
        print(f"Error al leer datos de {activo}: {e}")
        conn.close()
        return None

# 7. Calcular Momentum Score
def calcular_momentum(df, activo):
    try:
        if len(df) < 13:
            return None
        p0 = df['Adj_Close'].iloc[-1]
        p1 = df['Adj_Close'].iloc[-2]
        p3 = df['Adj_Close'].iloc[-4]
        p6 = df['Adj_Close'].iloc[-7]
        p12 = df['Adj_Close'].iloc[-13]
        momentum_score = (
            12 * (p0 / p1) +
            4 * (p0 / p3) +
            2 * (p0 / p6) +
            (p0 / p12) -
            19
        )
        return {'activo': activo, 'momentum_score': momentum_score if np.isfinite(momentum_score) else 0.0}
    except Exception as e:
        print(f"Error al calcular momentum para {activo}: {e}")
        return None

# 8. Calcular Volatilidades
def calcular_volatilidad(df, activo, meses):
    try:
        if len(df) < meses:
            return None
        retornos = df['Adj_Close'].pct_change().dropna()
        retornos = retornos.iloc[-meses:]
        volatilidad = retornos.std()
        return volatilidad if np.isfinite(volatilidad) else 0.0
    except Exception as e:
        print(f"Error al calcular volatilidad para {activo}: {e}")
        return None

# 9. Calcular Matriz de Correlación
def calcular_correlaciones(datos_activos, activos):
    try:
        retornos = pd.DataFrame()
        for activo in activos:
            df = datos_activos[activo]
            if df is not None and len(df) >= 12:
                retornos[activo] = df['Adj_Close'].pct_change().dropna()
        if retornos.empty:
            return None
        return retornos.corr(method='pearson')
    except Exception as e:
        print(f"Error al calcular correlaciones: {e}")
        return None

# 10. Seleccionar Activos con Métricas Detalladas
def seleccionar_activos(db_file, fecha_fin, momentum_min=0.7, momentum_max=3, max_activos=3, vol_corta_meses=4, vol_larga_meses=12):
    fecha_inicio_12m = (fecha_fin - relativedelta(months=13)).strftime('%Y-%m-%d')
    fecha_inicio_4m = (fecha_fin - relativedelta(months=vol_corta_meses)).strftime('%Y-%m-%d')
    fecha_fin_str = fecha_fin.strftime('%Y-%m-%d')
    
    activos = obtener_activos(db_file)
    if not activos:
        print(f"No se encontraron activos para {fecha_fin_str}")
        return pd.DataFrame({'fecha': [fecha_fin_str], 'activos_seleccionados': [[]]}), []
    
    datos_activos = {}
    momentum_results = []
    volatilidades = []
    
    for activo in activos:
        df_12m = leer_datos_activo(db_file, activo, fecha_inicio_12m, fecha_fin_str)
        df_4m = leer_datos_activo(db_file, activo, fecha_inicio_4m, fecha_fin_str)
        
        if df_12m is None or df_4m is None or df_12m.empty or df_4m.empty:
            continue
        
        datos_activos[activo] = df_12m
        
        vol_corta = calcular_volatilidad(df_4m, activo, vol_corta_meses)
        vol_larga = calcular_volatilidad(df_12m, activo, vol_larga_meses)
        
        if vol_corta is None or vol_larga is None:
            continue
            
        volatilidades.append({
            'activo': activo,
            'vol_corta': vol_corta,
            'vol_larga': vol_larga
        })
        
        momentum_data = calcular_momentum(df_12m, activo)
        if momentum_data:
            momentum_results.append(momentum_data)
    
    if not momentum_results:
        print(f"No hay datos de momentum para {fecha_fin_str}")
        return pd.DataFrame({'fecha': [fecha_fin_str], 'activos_seleccionados': [[]]}), []
    
    df_volatilidades = pd.DataFrame(volatilidades)
    df_volatilidades = df_volatilidades[df_volatilidades['vol_corta'] <= df_volatilidades['vol_larga']]
    activos_validos = df_volatilidades['activo'].tolist()
    
    df_momentum = pd.DataFrame(momentum_results)
    df_momentum = df_momentum[df_momentum['activo'].isin(activos_validos)]
    df_momentum = df_momentum[(df_momentum['momentum_score'] >= momentum_min) & (df_momentum['momentum_score'] <= momentum_max)]
    
    if df_momentum.empty:
        print(f"No hay activos con momentum score entre {momentum_min} y {momentum_max} para {fecha_fin_str}")
        return pd.DataFrame({'fecha': [fecha_fin_str], 'activos_seleccionados': [[]]}), []
    
    df_momentum = df_momentum.sort_values(by='momentum_score', ascending=False)
    
    matriz_correlacion = calcular_correlaciones(datos_activos, activos_validos)
    seleccionados = []
    activos_restantes = df_momentum['activo'].tolist()
    metricas_por_activo = []
    
    if activos_restantes:
        primer_activo = df_momentum.iloc[0]['activo']
        seleccionados.append(primer_activo)
        activos_restantes.remove(primer_activo)
    
    while len(seleccionados) < max_activos and activos_restantes:
        mejor_activo = None
        menor_correlacion_promedio = float('inf')
        
        for candidato in activos_restantes:
            if matriz_correlacion is not None and candidato in matriz_correlacion.columns:
                correlaciones = [matriz_correlacion.loc[candidato, seleccionado] for seleccionado in seleccionados if seleccionado in matriz_correlacion.index]
                if correlaciones:
                    correlacion_promedio = np.mean(correlaciones)
                    if correlacion_promedio < menor_correlacion_promedio:
                        menor_correlacion_promedio = correlacion_promedio
                        mejor_activo = candidato
        if mejor_activo:
            seleccionados.append(mejor_activo)
            activos_restantes.remove(mejor_activo)
        else:
            break
    
    # Calcular métricas por activo seleccionado
    for activo in seleccionados:
        momentum = df_momentum[df_momentum['activo'] == activo]['momentum_score'].iloc[0] if not df_momentum[df_momentum['activo'] == activo].empty else 0.0
        vol_corta = df_volatilidades[df_volatilidades['activo'] == activo]['vol_corta'].iloc[0] if not df_volatilidades[df_volatilidades['activo'] == activo].empty else 0.0
        vol_larga = df_volatilidades[df_volatilidades['activo'] == activo]['vol_larga'].iloc[0] if not df_volatilidades[df_volatilidades['activo'] == activo].empty else 0.0
        correlaciones = [matriz_correlacion.loc[activo, otro] for otro in seleccionados if otro != activo and matriz_correlacion is not None and activo in matriz_correlacion.index and otro in matriz_correlacion.columns]
        correlacion_promedio = np.mean(correlaciones) if correlaciones else 0.0
        metricas_por_activo.append({
            'fecha': fecha_fin_str,
            'activo': activo,
            'momentum_score': momentum,
            'volatilidad_corta': vol_corta,
            'volatilidad_larga': vol_larga,
            'correlacion_promedio': correlacion_promedio
        })
    
    return pd.DataFrame({'fecha': [fecha_fin_str], 'activos_seleccionados': [seleccionados]}), metricas_por_activo

# 11. Calcular Retorno del Portafolio con Detalles
def calcular_retorno_portafolio(db_file, activos_seleccionados, fecha_anterior, fecha_actual, capital, comision=0.0025):
    if not activos_seleccionados:
        return 0.0, []
    
    retornos = []
    detalles_activos = []
    num_activos = len(activos_seleccionados)
    capital_por_activo = capital / num_activos if num_activos > 0 else 0
    
    for activo in activos_seleccionados:
        df = leer_datos_activo(db_file, activo, fecha_anterior.strftime('%Y-%m-%d'), fecha_actual.strftime('%Y-%m-%d'))
        if df is not None and len(df) >= 2:
            precio_compra = df['Adj_Close'].iloc[-2]
            precio_venta = df['Adj_Close'].iloc[-1]
            retorno = (precio_venta / precio_compra) - 1 if precio_compra > 0 else 0.0
            cantidad_activos = capital_por_activo / precio_compra if precio_compra > 0 else 0.0
            retornos.append(retorno)
            detalles_activos.append({
                'activo': activo,
                'precio_compra': precio_compra,
                'precio_venta': precio_venta,
                'cantidad_activos': cantidad_activos,
                'retorno_activo': retorno
            })
    
    if not retornos:
        return 0.0, []
    
    retorno_bruto = np.mean(retornos) if retornos else 0.0
    retorno_neto = (1 + retorno_bruto) * (1 - comision) * (1 - comision) - 1
    return retorno_neto if np.isfinite(retorno_neto) else 0.0, detalles_activos

# 12. Calcular Métricas
def calcular_metricas(capital_inicial, capital_final, retornos_mensuales, años):
    if años <= 0 or not retornos_mensuales:
        return 0.0, 0.0, 0.0
    cagr = (capital_final / capital_inicial) ** (1 / años) - 1 if capital_final > 0 else 0.0
    volatilidad_mensual = np.std(retornos_mensuales) if len(retornos_mensuales) > 0 else 0.0
    volatilidad_anualizada = volatilidad_mensual * np.sqrt(12)
    tasa_libre_riesgo = 0.02
    sharpe = (cagr - tasa_libre_riesgo) / volatilidad_anualizada if volatilidad_anualizada > 0 else 0.0
    return sharpe if np.isfinite(sharpe) else 0.0, volatilidad_anualizada if np.isfinite(volatilidad_anualizada) else 0.0, cagr if np.isfinite(cagr) else 0.0

# 13. Backtesting con Métricas
def backtesting_selecciones_con_metricas(db_file, inicio, fin, capital_inicial=10000):
    fechas = pd.date_range(start=inicio, end=fin, freq='ME')
    selecciones = []
    metricas_activos = []
    capital = capital_inicial
    retornos_mensuales = []
    
    for i, fecha in enumerate(fechas[:-1]):
        fecha_siguiente = fechas[i + 1]
        print(f"Procesando selecciones para {fecha.strftime('%Y-%m-%d')}...")
        
        seleccion, metricas_por_activo = seleccionar_activos(db_file, fecha, momentum_min=0.7, momentum_max=3, max_activos=3, vol_corta_meses=4, vol_larga_meses=12)
        if seleccion is None or seleccion.empty:
            selecciones.append({
                'fecha': fecha.strftime('%Y-%m-%d'),
                'activos_seleccionados': [],
                'rentabilidad_mensual': 0.0,
                'capitalizacion_final': capital,
                'sharpe': 0.0,
                'volatilidad_final': 0.0,
                'cagr': 0.0
            })
            retornos_mensuales.append(0.0)
            continue
        
        activos_seleccionados = seleccion.iloc[0]['activos_seleccionados']
        retorno_mensual, detalles_activos = calcular_retorno_portafolio(db_file, activos_seleccionados, fecha, fecha_siguiente, capital)
        capital *= (1 + retorno_mensual)
        años = (fecha - inicio).days / 365.25
        sharpe, volatilidad_final, cagr = calcular_metricas(capital_inicial, capital, retornos_mensuales, años)
        
        selecciones.append({
            'fecha': fecha.strftime('%Y-%m-%d'),
            'activos_seleccionados': activos_seleccionados,
            'rentabilidad_mensual': retorno_mensual,
            'capitalizacion_final': capital,
            'sharpe': sharpe,
            'volatilidad_final': volatilidad_final,
            'cagr': cagr
        })
        retornos_mensuales.append(retorno_mensual)
        
        # Combinar métricas por activo con detalles de retorno
        for metricas in metricas_por_activo:
            activo = metricas['activo']
            detalle = next((d for d in detalles_activos if d['activo'] == activo), {})
            metricas.update({
                'precio_compra': detalle.get('precio_compra', 0.0),
                'precio_venta': detalle.get('precio_venta', 0.0),
                'cantidad_activos': detalle.get('cantidad_activos', 0.0),
                'retorno_activo': detalle.get('retorno_activo', 0.0)
            })
            metricas_activos.append(metricas)
    
    df_selecciones = pd.DataFrame(selecciones)
    df_metricas_activos = pd.DataFrame(metricas_activos)
    return df_selecciones, df_metricas_activos

# 14. Limpiar DataFrames
def limpiar_dataframe(df):
    # Reemplazar NaN, inf, -inf con 0.0 para columnas numéricas
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], 0.0).fillna(0.0)
    # Convertir listas a cadenas y reemplazar None con cadena vacía
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(str)
        if df[col].isna().any():
            df[col] = df[col].fillna('')
    print(f"DataFrame limpiado. Resumen:\n{df.head()}")
    return df

# 15. Escribir en Google Sheets
def escribir_google_sheets(df_selecciones, df_metricas_activos, client, creds, output_dir):
    try:
        # Limpiar DataFrames
        df_selecciones = limpiar_dataframe(df_selecciones)
        df_metricas_activos = limpiar_dataframe(df_metricas_activos)
        
        # Obtener o crear la carpeta en Google Drive
        folder_path = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento'
        folder_id = obtener_o_crear_carpeta(creds, folder_path)
        if folder_id is None:
            print("No se pudo obtener o crear la carpeta. Creando en la raíz de Drive.")
        
        # Crear la hoja
        spreadsheet = client.create('Momentum_Estrategia_20y', folder_id=folder_id)
        spreadsheet.share('', perm_type='anyone', role='writer')
        print(f"Hoja creada: {spreadsheet.title}, URL: {spreadsheet.url}")
        
        # Pestaña Por Mes
        worksheet_mes = spreadsheet.worksheet('Sheet1')
        worksheet_mes.update_title('Por Mes')
        worksheet_mes.update([df_selecciones.columns.values.tolist()] + df_selecciones.values.tolist())
        
        # Pestaña Por Activo
        worksheet_activo = spreadsheet.add_worksheet(title='Por Activo', rows=len(df_metricas_activos) + 1, cols=len(df_metricas_activos.columns))
        worksheet_activo.update([df_metricas_activos.columns.values.tolist()] + df_metricas_activos.values.tolist())
        
        print("Datos escritos en Google Sheets")
    except Exception as e:
        print(f"Error al escribir en Google Sheets: {e}")

# 16. Main
def main():
    db_file = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento/precios_activos_mensual.db'
    creds_file = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento/importfromapi-c1f7294cbbea.json'
    output_dir = montar_drive()
    
    # Autenticar con Google Sheets
    client, creds = autenticar_google_sheets(creds_file)
    if client is None:
        return
    
    inicio = datetime(2005, 5, 31)
    fin = datetime(2025, 4, 30)
    
    print(f"Ejecutando backtesting desde {inicio.strftime('%Y-%m-%d')} hasta {fin.strftime('%Y-%m-%d')}...")
    
    df_selecciones, df_metricas_activos = backtesting_selecciones_con_metricas(db_file, inicio, fin)
    
    if not df_selecciones.empty:
        escribir_google_sheets(df_selecciones, df_metricas_activos, client, creds, output_dir)
        print("\nPrimeras filas de selecciones mensuales:\n", df_selecciones.head())
        print("\nPrimeras filas de métricas por activo:\n", df_metricas_activos.head())
    else:
        print("No se generaron selecciones")

if __name__ == '__main__':
    main()
