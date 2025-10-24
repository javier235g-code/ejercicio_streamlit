import streamlit as st

# DIAGNÓSTICO TEMPORAL
st.write("### 🔍 Diagnóstico de Configuración")

import os

ruta_secrets = r"C:\dev\ejercicio_streamlit\.streamlit\secrets.toml"

if os.path.exists(ruta_secrets):
    st.success(f"✅ ¡Archivo encontrado!")

    # Leer y mostrar el contenido
    with open(ruta_secrets, 'r', encoding='utf-8') as f:
        contenido = f.read()
        st.code(contenido, language="toml")

    # Probar que Streamlit lo detecte
    try:
        st.write("### 🔍 Streamlit detecta:")
        st.write(st.secrets["connections"]["db_mysql"])
        st.success("✅ ¡Streamlit puede leer el archivo!")
    except Exception as e:
        st.error(f"❌ Streamlit no puede leerlo: {e}")
        st.warning("⚠️ REINICIA la app de Streamlit (Ctrl+C y vuelve a ejecutar)")
else:
    st.error(f"❌ Archivo no encontrado en: {ruta_secrets}")

st.write("### 📂 Rutas de Streamlit")
st.write(f"**Directorio actual:** `{os.getcwd()}`")
st.write(f"**Archivo principal:** `{__file__}`")
st.write(f"**Carpeta del script:** `{os.path.dirname(os.path.abspath(__file__))}`")

# Ruta donde DEBE estar secrets.toml
ruta_secrets = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".streamlit", "secrets.toml")
st.write(f"**Streamlit busca secrets en:** `{ruta_secrets}`")

# Verificar si existe
if os.path.exists(ruta_secrets):
    st.success(f"✅ El archivo existe en: {ruta_secrets}")
else:
    st.error(f"❌ El archivo NO existe en: {ruta_secrets}")
    st.info("Crea la carpeta `.streamlit` y el archivo `secrets.toml` en esa ubicación")






import mysql.connector
import streamlit as st

st.write("### 🧪 Test de Conexión Directa")

if st.button("Probar Conexión MySQL (sin st.connection)"):
    try:
        # AJUSTA ESTOS VALORES CON TUS DATOS REALES
        conexion = mysql.connector.connect(
            host="qanh131.dedalogestion.net",  # o tu host
            port=3306,  # tu puerto
            user="qaop422",  # tu usuario
            password="!V9e_rka9-N484Q",  # tu contraseña (si no tiene, dejar vacío)
            database="qanh131"  # tu base de datos
        )

        if conexion.is_connected():
            st.success("✅ ¡Conexión directa exitosa!")

            # Probar una consulta simple
            cursor = conexion.cursor()
            cursor.execute("SELECT DATABASE();")
            db_actual = cursor.fetchone()
            st.write(f"Base de datos actual: {db_actual[0]}")

            cursor.close()
            conexion.close()

    except mysql.connector.Error as e:
        st.error(f"❌ Error de conexión: {e}")
        st.write(f"Código de error: {e.errno}")
        st.write(f"Mensaje SQL: {e.msg}")




# Verificar si secrets existe
try:
    st.write("**Secrets disponibles:**", st.secrets.keys())

    # Verificar si existe la sección connections
    if "connections" in st.secrets:
        st.write("**Conexiones disponibles:**", st.secrets["connections"].keys())

        # Verificar configuración específica (SIN mostrar password)
        if "db_mysql" in st.secrets["connections"]:
            config = st.secrets["connections"]["db_mysql"]
            st.write("**Configuración de db_mysql:**")
            st.write(f"- Host: {config.get('host', 'NO DEFINIDO')}")
            st.write(f"- Port: {config.get('port', 'NO DEFINIDO')}")
            st.write(f"- Database: {config.get('database', 'NO DEFINIDO')}")
            st.write(f"- Username: {config.get('username', 'NO DEFINIDO')}")
            st.write(f"- Dialect: {config.get('dialect', 'NO DEFINIDO')}")
            st.write(f"- Driver: {config.get('driver', 'NO DEFINIDO')}")
    else:
        st.error("❌ No se encontró la sección [connections] en secrets.toml")

except FileNotFoundError:
    st.error("❌ No se encontró el archivo secrets.toml")
    st.info("Debe estar en: `.streamlit/secrets.toml`")
except Exception as e:
    st.error(f"Error leyendo secrets: {e}")

st.write("---")