from prefect import task
import mysql.connector

@task(name="load_data_to_mysql")
def load_data_to_mysql(data):
    db_config = {
        'host': 'your_mysql_host',
        'user': 'your_mysql_user',
        'password': 'your_mysql_password',
        'database': 'your_mysql_database'
    }
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor()
        add_job = ("INSERT INTO ofertas_laborales "
                   "(titulo, descripcion, ubicacion, enlace, fecha) "
                   "VALUES (%s, %s, %s, %s, %s)")

        for job in data:
            job_data = (job['titulo'], job['descripcion'], job['ubicacion'], job['enlace'], job['fecha'])
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