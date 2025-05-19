# CARGA DE ACTIVOS A DB
# 1. Importar Librerías
import yfinance as yf
import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 2. Obtener Datos
def obtener_datos(activo, inicio, fin):
    try:
        # Ajustar fecha de inicio según el activo
        inicio_minimo = {
            'SPY': '1993-01-22',
            'QQQ': '1999-03-10',
            'GLD': '2004-11-18',
            'EEM': '2003-04-07',
            'FXI': '2004-10-05',
            'EWZ': '2000-07-10',
            'XLF': '1998-12-16',
            'XLC': '2018-06-18',
            'IEUR': '2014-06-10',
            'XLY': '1998-12-16',
            'VEA': '2007-07-20',
            'XLRE': '2015-10-07',
            'XLB': '1998-12-16',
            'IVE': '2000-05-15',
            'IVW': '2000-05-15'
        }
        inicio_activo = inicio_minimo.get(activo, inicio)
        if datetime.strptime(inicio_activo, '%Y-%m-%d') > datetime.strptime(inicio, '%Y-%m-%d'):
            inicio = inicio_activo
            print(f"Ajustando inicio para {activo} a {inicio} debido a disponibilidad de datos")

        # Descargar datos diarios con auto_adjust=False
        datos = yf.download(activo, start=inicio, end=fin, progress=False, interval='1d', auto_adjust=False)
        if datos.empty:
            print(f"No se encontraron datos para {activo} en el rango {inicio} a {fin}.")
            return None
        
        # Imprimir columnas y filas para depuración
        print(f"Columnas originales para {activo}: {list(datos.columns)}")
        print(f"Filas descargadas para {activo}: {len(datos)}")
        
        # Manejar MultiIndex si existe
        if datos.columns.nlevels > 1:
            datos.columns = [col[0] for col in datos.columns.values]
            print(f"Columnas aplanadas para {activo}: {list(datos.columns)}")
        
        # Resamplear a mensuales, tomando el último día del mes
        datos_mensuales = datos.resample('ME').last()
        
        # Verificar columnas disponibles
        available_columns = list(datos_mensuales.columns)
        print(f"Columnas después de resampleo para {activo}: {available_columns}")
        
        # Definir columnas esperadas
        expected_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        
        # Intentar con nombres alternativos si 'Adj Close' no está
        if 'Adj Close' not in available_columns and 'Adj_Close' in available_columns:
            datos_mensuales = datos_mensuales.rename(columns={'Adj_Close': 'Adj Close'})
            print(f"Renombrada 'Adj_Close' a 'Adj Close' para {activo}")
        elif 'Adj Close' not in available_columns and 'Close' in available_columns:
            datos_mensuales['Adj Close'] = datos_mensuales['Close']
            print(f"No se encontró 'Adj Close' para {activo}. Usando 'Close' como respaldo (puede no reflejar dividendos)")
        
        # Verificar que todas las columnas esperadas estén presentes
        missing_columns = [col for col in expected_columns if col not in datos_mensuales.columns]
        if missing_columns:
            print(f"Error: Faltan columnas para {activo}: {missing_columns}")
            return None
        
        # Seleccionar y renombrar columnas
        datos_mensuales = datos_mensuales[expected_columns].rename(
            columns={'Adj Close': 'Adj_Close'}
        )
        
        # Redondear columnas numéricas a 3 decimales
        datos_mensuales[['Open', 'High', 'Low', 'Close', 'Adj_Close']] = datos_mensuales[['Open', 'High', 'Low', 'Close', 'Adj_Close']].round(3)
        
        # Filtrar datos hasta el último día del mes completo más reciente
        datos_mensuales = datos_mensuales[datos_mensuales.index <= fin]
        
        # Eliminar filas con datos faltantes
        datos_mensuales = datos_mensuales.dropna()
        
        if datos_mensuales.empty:
            print(f"No se encontraron datos válidos para {activo} tras resampleo.")
            return None
        
        print(f"Datos procesados para {activo}: {len(datos_mensuales)} filas")
        return datos_mensuales
    except Exception as e:
        print(f"Error al obtener datos para {activo}: {e}")
        return None

# 3. Almacenar Datos
# 3.1 Crear conexión a la base de datos
def crear_conexion(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Conexión establecida a {db_file}")
        return conn
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
    return conn

# 3.2 Crear tabla para el activo
def crear_tabla(conn, activo):
    try:
        c = conn.cursor()
        c.execute(f'''CREATE TABLE IF NOT EXISTS {activo} (
                        date TEXT PRIMARY KEY,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        adj_close REAL,
                        volume INTEGER
                    )''')
        print(f"Tabla creada o verificada para {activo}")
    except sqlite3.Error as e:
        print(f"Error al crear la tabla: {e}")

# 3.3 Insertar datos en la tabla
def insertar_datos(conn, activo, datos):
    if datos is not None and not datos.empty:
        try:
            c = conn.cursor()
            filas_insertadas = 0
            required_columns = ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
            if not all(col in datos.columns for col in required_columns):
                print(f"Error: Faltan columnas necesarias: {required_columns}")
                return
            
            for index, row in datos.iterrows():
                c.execute(f'''INSERT OR IGNORE INTO {activo} (date, open, high, low, close, adj_close, volume)
                              VALUES (?, ?, ?, ?, ?, ?, ?)''',
                          (index.strftime('%Y-%m-%d'),
                           float(row['Open']),
                           float(row['High']),
                           float(row['Low']),
                           float(row['Close']),
                           float(row['Adj_Close']),
                           int(row['Volume'])))
                filas_insertadas += 1
            conn.commit()
            print(f"{filas_insertadas} filas insertadas para {activo}")
        except sqlite3.Error as e:
            print(f"Error al insertar datos: {e}")
        except Exception as e:
            print(f"Error inesperado al insertar datos: {e}")
            print("Fila problemática:", row.to_dict())
    else:
        print("No hay datos para insertar")

# 4. Verificar Última Fecha Registrada
def obtener_ultima_fecha(conn, activo):
    try:
        c = conn.cursor()
        c.execute(f"SELECT MAX(date) FROM {activo}")
        result = c.fetchone()[0]
        if result:
            return datetime.strptime(result, '%Y-%m-%d').date()
        return None
    except sqlite3.Error:
        return None

# 5. Verificar Existencia de Tabla
def tabla_existe(conn, activo):
    try:
        c = conn.cursor()
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{activo}'")
        return c.fetchone() is not None
    except sqlite3.Error:
        return False

# 6. Integración
def main():
    # Configuración ACTIVOS y FECHA
    activos = ['SPY', 'QQQ', 'GLD', 'EEM', 'FXI', 'EWZ', 'XLF', 'XLC', 'IEUR', 'XLY', 'VEA', 'XLRE', 'XLB', 'IVE', 'IVW']
    inicio_historico = '2005-01-01'  # Fecha de inicio por defecto
    # Último día del mes completo más reciente
    hoy = datetime.today()
    fin = (hoy.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')  # 2025-04-30
    db_file = '/content/drive/MyDrive/Colab Notebooks/EstrategiaMomento/precios_activos_mensual.db'

    # Crear conexión
    conn = crear_conexion(db_file)
    if conn is None:
        print("No se pudo establecer conexión a la base de datos")
        return

    # Procesar cada activo
    for activo in activos:
        print(f"\nProcesando {activo}...")
        # Verificar si el activo ya existe
        if tabla_existe(conn, activo):
            # Activo existente: obtener última fecha registrada
            ultima_fecha = obtener_ultima_fecha(conn, activo)
            if ultima_fecha:
                # Descargar datos desde el día siguiente a la última fecha
                inicio = (ultima_fecha + timedelta(days=1)).strftime('%Y-%m-%d')
                print(f"Activo existente. Descargando datos desde {inicio} hasta {fin}")
            else:
                # Si no hay datos previos, usar histórico completo
                inicio = inicio_historico
                print(f"No se encontraron datos previos. Descargando histórico desde {inicio} hasta {fin}")
        else:
            # Nuevo activo: usar histórico completo
            inicio = inicio_historico
            print(f"Nuevo activo. Descargando histórico desde {inicio} hasta {fin}")

        # Obtener datos
        datos = obtener_datos(activo, inicio, fin)

        # Almacenar datos
        if datos is not None:
            crear_tabla(conn, activo)
            insertar_datos(conn, activo, datos)

    # Cerrar conexión
    conn.close()
    print("\nProceso completado para todos los activos")

if __name__ == '__main__':
    main()