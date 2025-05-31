from prefect import task
from datetime import datetime

@task(name="transform_date")
def transform_date(job_listings):
    transformed_listings = []
    for job in job_listings:
        try:
            # Asume que el formato de fecha en el atributo 'datetime' es YYYY-MM-DD
            job['fecha'] = datetime.strptime(job['fecha_raw'], '%Y-%m-%d').date()
            del job['fecha_raw']  # Eliminar la columna original
            transformed_listings.append(job)
        except (ValueError, KeyError) as e:
            print(f"Error al transformar la fecha: {e} en la oferta: {job.get('titulo', 'sin título')}")
            # Decide cómo manejar los errores: omitir la oferta, usar una fecha por defecto, etc.
            pass
    return transformed_listings