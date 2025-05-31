from prefect import task
import mysql.connector

@task(name="load_data_to_mysql")
def load_data_to_mysql(data):
    print("Iniciando la carga de datos a MySQL...")
    db_config = {
        'host': '127.0.0.1',
        'user': 'mysql_admin',
        'password': '123456',
        'database': 'mysql'
    }
    try:
        # Conectar a la base de datos MySQL
        print("Conectando a la base de datos MySQL...")
        cnx = mysql.connector.connect(**db_config)
        # Crear un cursor para ejecutar consultas
        print("Creando el cursor...")
        cursor = cnx.cursor()
        # Crear la tabla solo si no existe
        print("Creando la tabla ofertas_laborales si no existe...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ofertas_laborales (
                id INT AUTO_INCREMENT PRIMARY KEY,
                titulo VARCHAR(255),
                empresa VARCHAR(255),
                ubicacion VARCHAR(255),
                descripcion TEXT,
                modalidad VARCHAR(255),
                link VARCHAR(500),
                fecha DATE
            )
        """)        
        # Eliminar registros existentes
        cursor.execute("DELETE FROM ofertas_laborales")
        print("Registros existentes eliminados de la tabla ofertas_laborales.")        
        add_job = ("INSERT INTO ofertas_laborales "
                   "(titulo, empresa, ubicacion, descripcion,modalidad, link, fecha) "
                   "VALUES (%s, %s, %s, %s, %s, %s, %s)")

        for job in data:
            job_data = (job['titulo'], job['empresa'], job['ubicacion'], job['descripcion'], job['modalidad'], job['link'], job['fecha'])
            print(f"Inserting job: {job_data}")
            cursor.execute(add_job, job_data)

        cnx.commit()
        print(f"Se insertaron {cursor.rowcount} ofertas laborales en la tabla ofertas_laborales.")
    except mysql.connector.Error as err:
        print(f"Error al conectar o insertar datos en MySQL: {err}")
    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Conexión a MySQL cerrada.")