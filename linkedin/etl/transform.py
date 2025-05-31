from prefect import task
from datetime import datetime

@task(name="transform_date")
def transform_date(job_listings):
    transformed_listings = []
    for job in job_listings:
        fecha = job.get('fecha', '')
        fecha_convertida = None
        # Prueba varios formatos comunes
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
            try:
                if fecha:
                    fecha_convertida = datetime.strptime(fecha, fmt).date()
                    break
            except ValueError:
                continue
        if fecha_convertida:
            job['fecha'] = fecha_convertida.strftime('%Y/%m/%d')
            transformed_listings.append(job)
        else:
            print(f"Formato de fecha no válido: {fecha}")
    return transformed_listings