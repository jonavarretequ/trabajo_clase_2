import psycopg2
import json
from bs4 import BeautifulSoup
import requests

# Paso 1: Crear la base de datos y la tabla desde Python
def create_database_and_table():
    try:
        # Conexión al servidor PostgreSQL (sin especificar una base de datos)
        conn = psycopg2.connect(
            user="postgres",  # Usuario predeterminado
            password="postgres",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True  # Necesario para crear una base de datos
        cursor = conn.cursor()

        # Crear la base de datos "illinois_db" si no existe
        cursor.execute("SELECT datname FROM pg_database WHERE datname='illinois_db';")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE illinois_db;")
            print("Base de datos 'illinois_db' creada correctamente.")
        else:
            print("La base de datos 'illinois_db' ya existe.")

        # Cerrar la conexión inicial
        cursor.close()
        conn.close()

        # Conectar a la nueva base de datos
        conn = psycopg2.connect(
            database="illinois_db",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Crear la tabla "senator_tbl" si no existe
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS senator_tbl (
            id SERIAL PRIMARY KEY,
            senator VARCHAR(100) NOT NULL,
            bills VARCHAR(15),
            committees VARCHAR(50),
            district VARCHAR(15),
            party VARCHAR(15)
        );
        """)
        print("Tabla 'senator_tbl' creada correctamente.")

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al crear la base de datos o la tabla: {e}")


# Función para obtener los datos del scraping
def scrape_senator_data(format="tuple"):
    # Step 1: Make a GET request
    req = requests.get('http://www.ilga.gov/senate/default.asp')

    # Step 2: Read the content
    src = req.text

    # Step 3: Parse the content
    soup = BeautifulSoup(src, 'lxml')

    # Step 4: Extract column details
    columns_elements = soup.find_all("td", class_="detail")

    # Step 5: Organize details into rows
    rows = []
    for i in range(0, 50, 5):  # 50 elements, 5 elements per row
        row = columns_elements[i:i+5]
        row_data = [element.get_text(strip=True) for element in row]

        if format == "tuple":
            rows.append(tuple(row_data))  # Formato para la base de datos
        elif format == "json":
            rows.append({
                "senator": row_data[0],
                "bills": row_data[1],
                "committees": row_data[2],
                "district": row_data[3],
                "party": row_data[4]
            })  # Formato para JSON

    return rows


# Paso 2: Insertar datos estructurados en la tabla
def insert_senator_tbl(data):
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(
            database="illinois_db",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Insertar registros en la tabla
        cursor.executemany("""
        INSERT INTO senator_tbl (senator, bills, committees, district, party)
        VALUES (%s, %s, %s, %s, %s);
        """, data)

        # Confirmar cambios y cerrar la conexión
        conn.commit()
        print("Datos insertados correctamente.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error al insertar datos en la tabla: {e}")


# Paso 3: Guardar datos en un archivo JSON
def save_senator_tbl(data):
    try:
        # Guardar los datos en un archivo JSON
        with open("senator_tbl.json", "w") as file:
            json.dump(data, file, indent=4)

        print("Datos de los senadores guardados en 'senator_tbl.json'")

    except Exception as e:
        print(f"Error al guardar los datos en JSON: {e}")


# Ejecutar todas las funciones
if __name__ == "__main__":
    # Crear base de datos y tabla
    create_database_and_table()

    # Obtener los datos del scraping en formato de tuplas (para la base de datos)
    senator_data_tuples = scrape_senator_data(format="tuple")

    # Insertar los datos en la base de datos
    insert_senator_tbl(senator_data_tuples)

    # Obtener los datos del scraping en formato de diccionarios (para JSON)
    senator_data_json = scrape_senator_data(format="json")

    # Guardar los datos en un archivo JSON
    save_senator_tbl(senator_data_json)