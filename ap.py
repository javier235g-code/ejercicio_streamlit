import pandas as pd
import streamlit as st
from pandas import Series
from sqlalchemy.exc import OperationalError  # Para capturar errores comunes de SQL
import os  # Necesario para obtener la fecha de modificación del archivo
from datetime import datetime
try:
    import reverse_geocoder as rg
except ImportError:
    # Si no está instalada, rg será None. Lo manejaremos más adelante.
    rg = None


def actualizar_csv_con_st_connection(
        nombre_conexion_st: str,
        consulta_sql: str,
        archivo_csv: str = "data.csv"
):
    """
    Conecta a MySQL usando el metodo moderno st.connection y actualiza un CSV.

    Args:
        nombre_conexion_st (str): El nombre de la conexión definida en
                                  secrets.toml (ej: "db_mysql").
        consulta_sql (str): La consulta SQL (SELECT) para extraer los datos.
        archivo_csv (str): El nombre del archivo CSV a sobreescribir.

    Returns:
        bool: True si la operación fue exitosa, False si hubo un error.
    """

    try:
        # NUEVO: Verificar que exista la configuración en secrets
        if nombre_conexion_st not in st.secrets.get("connections", {}):
            st.error(f"❌ No se encontró '[connections.{nombre_conexion_st}]' en secrets.toml")
            st.info("""
                    **Solución:** Crea el archivo `.streamlit/secrets.toml` con:
        ```toml
                    [connections.db_mysql]
                    dialect = "mysql"
                    driver = "mysqlconnector"
                    host = "localhost"
                    port = 3306
                    database = "tu_base_datos"
                    username = "tu_usuario"
                    password = "tu_contraseña"
        ```
                    """)
            return False

        else:
            # 1. Establecer conexión usando el gestor de Streamlit
            # Streamlit buscará en secrets.toml la sección [connections.nombre_conexion_st]
            st.info(f"Conectando a DB MySQL", icon="✅")
            conn = st.connection(nombre_conexion_st, type="sql")

            # 2. Ejecutar la consulta.
            # Usamos ttl=0 para deshabilitar el cacheo y asegurar que los datos
            # sean siempre los más recientes (vital para una "actualización").
            # Si quisiera cachear los resultados por 10 minutos, usaría ttl=600.
            df_datos = conn.query(consulta_sql, ttl=0)

            # 3. Guardar el DataFrame en el archivo CSV
            df_datos.to_csv(archivo_csv, index=False, encoding='utf-8')

            # 4. Notificar al usuario del éxito
            st.info(f"¡Éxito! el archivo de datos local ha sido actualizado correctamente.", icon="✅")
            return True

    except OperationalError as e:
        # Error común si la DB no responde, las credenciales son incorrectas
        # o la consulta SQL tiene un error de sintaxis o tabla no encontrada.
        st.error(f"Error de Conexión/SQL (OperationalError): {e}")
        return False

    except FileNotFoundError:
        # Error si st.connection no encuentra el driver (ej. 'mysqlconnector')
        st.error("Error: Driver de base de datos no encontrado.")
        st.warning("Asegúrese de haber instalado 'mysql-connector-python'.")
        return False

    except Exception as e:
        # 8. Capturar cualquier otro error inesperado
        st.error(f"Error inesperado durante la actualización: {e}")
        return False


def get_last_update_time(archivo_csv: str) -> str:
    """
    Obtiene la fecha de última modificación de un archivo en formato legible.
    """
    try:
        # Obtener el timestamp (en segundos) de la última modificación
        timestamp = os.path.getmtime(archivo_csv)
        # Convertir el timestamp a un objeto datetime
        mod_time = datetime.fromtimestamp(timestamp)
        # Formatear la fecha para mostrarla
        return mod_time.strftime("%Y-%m-%d %H:%M:%S")
    except FileNotFoundError:
        return "Archivo no encontrado"
    except Exception as e:
        return f"Error al leer fecha: {e}"


@st.cache_data  # Usamos cache de Streamlit
def load_data(archivo_csv: str): #-> pd.DataFrame | None:
    """
    Carga y procesa el archivo CSV local.
    Se cachea para mejorar el rendimiento en interacciones (como filtros).
    La caché se limpiará manualmente si el botón de actualizar se pulsa.
    """
    try:
        df = pd.read_csv(archivo_csv)

        if df.empty:
            st.warning("El archivo CSV está vacío.")
            return None
        # 1. Identificar filas donde 'region' está vacía (NaN)
        # (usamos .isna() que detecta NaN, y .fillna('') por si acaso)
        df['region'] = df['region'].fillna(pd.NA)
        missing_region_mask = df['region'].isna()

        # 2. Comprobar si hay alguna región que rellenar
        if missing_region_mask.any():

            # 3. Comprobar si la biblioteca 'reverse_geocoder' está disponible
            if rg is None:
                st.error("Biblioteca 'reverse_geocoder' no encontrada.")
                st.warning("Para rellenar regiones vacías, instala la biblioteca: pip install reverse_geocoder")
                # Rellenar con 'Desconocida' para que el groupby funcione
                df['region'] = df['region'].fillna('Desconocida')
            else:
                st.info(f"Detectadas {missing_region_mask.sum()} regiones vacías. Rellenado.")

                try:
                    # 4. Preparar las coordenadas (lat, lon) SOLO de las filas necesarias
                    coords = list(zip(
                        df.loc[missing_region_mask, 'lat'],
                        df.loc[missing_region_mask, 'lon']
                    ))

                    if coords:
                        # 5. Realizar la búsqueda (es offline y rápida)
                        results = rg.search(coords)  # Retorna una lista de dicts

                        # 6. Extraer el nombre de la región ('admin1' en España es la Comunidad Autónoma)
                        # ej: 'Madrid', 'Andalusia', 'Catalonia'
                        filled_regions = [res['admin1'] for res in results]

                        # 7. Asignar los nuevos valores de vuelta al DataFrame
                        df.loc[missing_region_mask, 'region'] = filled_regions

                except Exception as e:
                    st.error(f"Error durante la geocodificación: {e}")
                    df['region'] = df['region'].fillna('Error Geocodificación')

        # Rellenar cualquier posible vacío restante (ej. coordenadas en el océano)
        df['region'] = df['region'].fillna('Desconocida')
        
        # --- Procesamiento de Fechas ---
        # Convertir la columna de fecha/hora a objeto datetime de pandas
        df['fecha_hora_descarga'] = pd.to_datetime(df['fecha_hora_descarga'])

        # Crear una nueva columna 'fecha' (solo el día) para agrupar y filtrar
        df['fecha'] = df['fecha_hora_descarga'].dt.date

        return df

    except FileNotFoundError:
        # Esto es normal si es la primera vez que se ejecuta
        st.info(f"No se encontró el archivo '{archivo_csv}'. "
                "Pulsa el botón 'Actualizar' para generarlo desde la DB.")
        return None
    except pd.errors.EmptyDataError:
        st.warning(f"El archivo '{archivo_csv}' está vacío.")
        return None
    except Exception as e:
        st.error(f"Error crítico al cargar o procesar el CSV: {e}")
        return None

# --- ESTRUCTURA PRINCIPAL DE LA APLICACIÓN ---
import time
with st.sidebar:
    st.title("🗂️ Panel de Control")

    # Definir la consulta y el nombre de la conexión
    MI_CONSULTA = "SELECT * FROM descargas;"
    MI_CONEXION_SECRETS = "db_mysql"  # Debe coincidir con [connections.db_mysql] en secrets.toml
    ARCHIVO_SALIDA = "data.csv"

    # --- 1. Botón de Actualización (Lógica existente) ---
    # Colocamos el botón primero
    if st.button(f"Actualizar Registros Base de Datos"):

        with st.spinner("Ejecutando consulta y actualizando CSV..."):

            exito = actualizar_csv_con_st_connection(
                nombre_conexion_st=MI_CONEXION_SECRETS,
                consulta_sql=MI_CONSULTA,
                archivo_csv=ARCHIVO_SALIDA)

            if exito:
             st.success("Proceso completado.")
             # ¡IMPORTANTE!
             # Limpiamos la caché de la función 'load_data'.
             # Esto fuerza a Streamlit a releer el archivo CSV
             # modificado en la siguiente línea (load_data).
             st.cache_data.clear()
            # 'else' (fallo) ya es manejado por la función con st.error

    # --- 2. Carga y Visualización de Datos (NUEVA LÓGICA) ---

    # Mostrar estado de carga y fecha de actualización (Metas 1 y 2)
    st.subheader("Estado de los Datos Locales")
    with st.spinner(f"Cargando datos en local."):
       # Intentamos cargar los datos (usará la caché si no se pulsó el botón)
      df_principal = load_data(ARCHIVO_SALIDA)

      # Obtenemos y mostramos la fecha de modificación del archivo
      last_update = get_last_update_time(ARCHIVO_SALIDA)
      st.caption(f"Última actualización del fichero: **{last_update}**")

    # Si la carga falla (ej. no existe el archivo), detenemos la ejecución aquí
    if df_principal is None:
       st.stop()

# --- 3. Análisis y Visualizaciones (Metas 3, 4, 5, 6) ---

# Meta 4: Resumen por Zona


# Agrupar por 'zona'
df_zona = df_principal.groupby('region').agg(
    # Contar todos los registros (id) [cite: 1]
    numero_descargas=('id', 'count'),
    # Contar cuántos 'id_descargado' únicos hay [cite: 1]
    descargas_unicas=('id_descargado', 'nunique')
).reset_index()

df_zona.rename(columns={'region': 'Región', 'numero_descargas':'Descargas', 'descargas_unicas':'Descargas Únicas'}, inplace=True)
# Calcular totales globales (antes de añadir la fila de total)
total_descargas_global = df_principal.shape[0]  # Total de filas
total_unicas_global = df_principal['id_descargado'].nunique()  # Total de IDs únicos

st.title("📈 Análisis de Descargas | Certificados AyN")

col1, col2 = st.columns(2)
col1.markdown(":blue[Sistema de Seguimiento]")
col2.page_link("https://dedalogestion.com/ayn/", label="Dédalo", icon="🟩")

st.markdown("""
<style>
    [data-testid="stMetricValue"] {
        font-size: 40px;
        font-weight: bold;  /* Hace el número más grueso */
        color: #023b61;     /* Color azul (opcional) */
    }

    /* Intenta varios selectores para la etiqueta */

    [data-testid="stMetric"] [data-testid="stMarkdownContainer"] {
        font-size: 20px !important;
        font-weight: bold !important;
    }
    }
</style>
""", unsafe_allow_html=True)


col1, col2 = st.columns(2)
col1.metric("**Total Descargas** ️", total_descargas_global, border=True)
col2.metric("**Total Descargas Únicas** ", total_unicas_global, border=True)
ficheros_totales = 15151
porcentaje = round((total_unicas_global/ficheros_totales)*100, 2)
st.progress(porcentaje/100)
st.text(f"Porcentaje de Ficheros Únicos Descargados: {porcentaje}%")
st.divider()

st.header("Resumen por Región")
# Mostrar el DataFrame
st.dataframe(df_zona, hide_index='region', width= "content", use_container_width=True)

st.map(df_principal, color='#1b8210')

st.divider()

# Metas 5 y 6: Análisis por Fecha
st.header("Análisis por Fecha")
# Definir fechas mínimas y máximas para el selector (basado en la columna 'fecha')
min_date = df_principal['fecha'].min()
max_date = df_principal['fecha'].max()

# Meta 5 (parcial): Calendario para seleccionar rango
selected_range = st.date_input(
    "Seleccione un rango de fechas",
    (min_date, max_date),  # Valor por defecto (rango completo)
    min_value=min_date,
    max_value=max_date,
    key="date_range_selector")

# Asegurarse de que el selector devolvió un rango válido (inicio y fin)
if len(selected_range) == 2:
    start_date, end_date = selected_range

    # Filtrar el DF principal según el rango seleccionado
    df_filtrado_fecha = df_principal[
        (df_principal['fecha'] >= start_date) &
        (df_principal['fecha'] <= end_date)
        ]

    # Comprobar si hay datos en el rango seleccionado
    if df_filtrado_fecha.empty:
        st.warning("No hay datos en el rango de fechas seleccionado.")
    else:
        st.subheader("Histograma de Descargas Totales por Día (en rango)")
        # Agregamos para el histograma (contando 'id' totales, no únicos)
        df_hist = df_filtrado_fecha.groupby('fecha').agg(
            numero_descargas_totales=('id', 'count')).reset_index()

        # Pintar el histograma
        st.bar_chart(
            df_hist,
            x='fecha',
            y='numero_descargas_totales',
            x_label = 'Fecha',
            y_label = 'Numero de Descargas',
            use_container_width=True)

        st.subheader("Descargas Únicas por Día (en rango)")
        df_fecha_agg = df_filtrado_fecha.groupby('fecha').agg(numero_descargas_unicas=('id_descargado', 'nunique')).reset_index()
        st.dataframe(df_fecha_agg, use_container_width=True)

else:
    # Caso por si el selector de fecha no devuelve un rango (ej. solo 1 día)
    st.info("Por favor, seleccione un rango de fechas válido (inicio y fin).")

st.divider()