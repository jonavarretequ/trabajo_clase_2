import psycopg2
import json
from datetime import datetime
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time

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
def scrape_senator_data():
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
        rows.append(tuple(row_data))

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

# Paso 3: Guardar logs no estructurados en un archivo JSON
def save_senator_tbl():
    try:
        # Simulación de logs no estructurados
        senator_tbl = [
            "[2025-02-22 10:10:00] Firewall Alert: Blocked incoming traffic from 10.0.0.1 to port 22.",
            "[2025-02-22 10:15:00] IDS Alert: Suspicious activity detected from IP 192.168.1.20."
        ]

        # Guardar logs en un archivo JSON
        with open("senator_tbl.json", "w") as file:
            json.dump(senator_tbl, file, indent=4)

        print("Logs no estructurados guardados en 'unsenator_tbl.json'")

    except Exception as e:
        print(f"Error al guardar logs no estructurados: {e}")

# Ejecutar todas las funciones
if __name__ == "__main__":

    create_database_and_table()  # Crear base de datos y tabla
    # Paso 1: Obtener los datos del scraping
    senator_data = scrape_senator_data()

    # Paso 2: Insertar los datos en la base de datos
    #insert_senator_tbl(senator_data)   # Insertar datos estructurados
    #save_senator_tbl()     # Guardar logs no estructurados
