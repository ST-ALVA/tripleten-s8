import psycopg2
import pandas as pd
import json


def execute_query(query, db_config):
    """
    Ejecuta un query SQL en PostgreSQL y retorna los resultados en un DataFrame.

    Parámetros:
        query (str): Consulta SQL.
        db_config (dict): Diccionario con los datos de conexión (host, dbname, user, password, port).

    Retorna:
        pd.DataFrame: Resultados de la consulta.
    """
    try:
        # Conectar a PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Ejecutar el query
        cursor.execute(query)
        columns = [
            desc[0] for desc in cursor.description
        ]  # Obtener nombres de columnas
        data = cursor.fetchall()

        # Convertir a DataFrame
        df = pd.DataFrame(data, columns=columns)

        # Cerrar conexión
        cursor.close()
        conn.close()

        return df
    except Exception as e:
        print(f"Error ejecutando la consulta: {e}")
        return None


# Función para crear una tabla en PostgreSQL a partir de un DataFrame
def create_table_from_df(df, table_name, db_config):
    """
    Crea una tabla en PostgreSQL basada en un DataFrame.

    Parámetros:
        df (pd.DataFrame): DataFrame con los datos.
        table_name (str): Nombre de la tabla a crear.
        db_config (dict): Diccionario con los datos de conexión.

    Retorna:
        None
    """
    try:
        # Conectar a PostgreSQL
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Construir el esquema de la tabla
        column_types = []
        for column, dtype in df.dtypes.items():
            if "int" in str(dtype):
                column_type = "INTEGER"
            elif "float" in str(dtype):
                column_type = "FLOAT"
            else:
                column_type = "TEXT"
            column_types.append(f'"{column}" {column_type}')

        columns_schema = ", ".join(column_types)
        create_table_query = (
            f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_schema});'
        )

        # Crear la tabla
        cursor.execute(create_table_query)
        conn.commit()

        # Insertar datos
        for _, row in df.iterrows():
            values = ", ".join(["%s"] * len(row))
            insert_query = f'INSERT INTO "{table_name}" VALUES ({values})'
            cursor.execute(insert_query, tuple(row))

            # hacer que se borre en caso de que exista y se cree una nueva.

        conn.commit()

        # Cerrar conexión
        cursor.close()
        conn.close()
        print(f"Tabla '{table_name}' creada exitosamente en PostgreSQL.")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")


def read_sql_file(file_path):
    """
    Lee un archivo .sql y retorna su contenido como un string.

    Parámetros:
        file_path (str): Ruta del archivo .sql

    Retorna:
        str: Contenido del archivo .sql
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            sql_query = file.read()
        return sql_query
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return None


def read_json_file(file_path):
    """
    Lee un archivo .json y retorna su contenido como un diccionario de Python.

    Parámetros:
        file_path (str): Ruta del archivo .json

    Retorna:
        dict: Contenido del archivo en formato de diccionario
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data
    except Exception as e:
        print(f"Error al leer el archivo JSON: {e}")
        return None


def limpieza_general(df):
    """
    Realiza una limpieza general de un DataFrame:
    - Normaliza nombres de columnas.
    - Elimina espacios innecesarios en strings.
    - Intenta convertir columnas tipo fecha.
    - Elimina duplicados.
    - Reporta tipos de datos y nulos.

    Retorna:
    - df_limpio: DataFrame limpio.
    """
    df = df.copy()  # Evitar modificar el original

    # 1. Nombres de columnas estandarizados
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 2. Limpiar espacios en columnas tipo texto
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # 3. Intentar convertir columnas que parecen fechas
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_datetime(df[col], errors="raise")
                print(f"📅 Columna '{col}' convertida a fecha.")
            except:
                continue

    # 4. Eliminar duplicados
    df = df.drop_duplicates()

    # 5. Reporte de tipos y nulos
    print("\n📊 Tipos de datos:")
    print(df.dtypes)
    print("\n🔍 Valores nulos por columna:")
    print(df.isnull().sum())

    print("\n✅ Limpieza general completada.")
    return df
