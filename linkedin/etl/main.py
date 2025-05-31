from prefect import flow
from etl.extract import extract_linkedin_jobs, parse_job_listings
from etl.transform import transform_date
from etl.load import load_data_to_mysql

@flow(name="linkedin_jobs_etl")
def linkedin_jobs_etl(linkedin_url):
    soup = extract_linkedin_jobs(linkedin_url)
    if soup:
        job_listings = parse_job_listings(soup)
        if job_listings:
            transformed_data = transform_date(job_listings)
            load_data_to_mysql(transformed_data)
        else:
            print("No se encontraron ofertas de trabajo para procesar.")
    else:
        print("No se pudo extraer el contenido de la página de LinkedIn.")

if __name__ == "__main__":
    linkedin_url = "https://www.linkedin.com/jobs/search/?keywords=Python%20Developer&location=Lima%2C%20Peru&f_TPR=r86400" # Ejemplo de URL
    linkedin_jobs_etl(linkedin_url=linkedin_url)