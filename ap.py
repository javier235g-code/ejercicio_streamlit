# main_app.py
# Importaciones necesarias
import pandas as pd
import streamlit as st
# Ya NO necesitamos 'mysql.connector' directamente
from sqlalchemy.exc import OperationalError  # Para capturar errores comunes de SQL


def actualizar_csv_con_st_connection(
        nombre_conexion_st: str,
        consulta_sql: str,
        archivo_csv: str = "data.csv"
):
    """
    Conecta a MySQL usando el método moderno 'st.connection' y actualiza un CSV.

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


        # 1. Establecer conexión usando el gestor de Streamlit
        # Streamlit buscará en secrets.toml la sección [connections.nombre_conexion_st]
        st.info(f"Intentando conectar a '{nombre_conexion_st}'...")
        conn = st.connection(nombre_conexion_st, type="sql")

        # 2. Ejecutar la consulta.
        # Usamos ttl=0 para deshabilitar el cacheo y asegurar que los datos
        # sean siempre los más recientes (vital para una "actualización").
        # Si quisiera cachear los resultados por 10 minutos, usaría ttl=600.
        df_datos = conn.query(consulta_sql, ttl=0)

        # 3. Guardar el DataFrame en el archivo CSV
        df_datos.to_csv(archivo_csv, index=False, encoding='utf-8')

        # 4. Notificar al usuario del éxito
        st.toast(f"¡Éxito! '{archivo_csv}' ha sido actualizado correctamente.", icon="✅")
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


# --- Ejemplo de uso en la aplicación Streamlit ---

st.title("Panel de Control")

# Definir la consulta y el nombre de la conexión
MI_CONSULTA = "SELECT * FROM descargas;"
MI_CONEXION_SECRETS = "db_mysql"  # Debe coincidir con [connections.db_mysql] en secrets.toml
ARCHIVO_SALIDA = "data.csv"

if st.button(f"Actualizar Registros Base de Datos"):

    with st.spinner("Ejecutando consulta y actualizando CSV..."):

        exito = actualizar_csv_con_st_connection(
            nombre_conexion_st=MI_CONEXION_SECRETS,
            consulta_sql=MI_CONSULTA,
            archivo_csv=ARCHIVO_SALIDA
        )

        if exito:
            st.success("Proceso completado.")
            # Opcional: Mostrar los datos actualizados
            try:
                df_preview = pd.read_csv(ARCHIVO_SALIDA)
                st.dataframe(df_preview.head())
            except FileNotFoundError:
                st.warning("El archivo CSV no se encontró para la vista previa.")