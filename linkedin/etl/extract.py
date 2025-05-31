from prefect import task
import requests
from bs4 import BeautifulSoup
from datetime import datetime

@task(name="extract_linkedin_jobs")
def extract_linkedin_jobs(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except requests.exceptions.RequestException as e:
        print(f"Error al acceder a la URL: {e}")
        return None

@task(name="parse_job_listings")
def parse_job_listings(soup):
    job_listings = []
    # Aquí debes inspeccionar la estructura HTML de la página de LinkedIn
    # y adaptar los selectores para extraer la información deseada.
    # Este es un ejemplo genérico, necesitarás ajustarlo.
    job_elements = soup.find_all('div', class_='base-card relative w-full hover:bg-gray-lightest p-2 transition-shadow')
    print('------1')
    print(job_elements)
    for job_element in job_elements:
        title_element = job_element.find('h3', class_='base-search-card__title')
        description_element = job_element.find('p', class_='base-search-card__subtitle')
        location_element = job_element.find('span', class_='job-search-card__location')
        link_element = job_element.find('a', class_='base-card__full-link')
        date_element = job_element.find('time', class_='job-search-card__listdate')

        if title_element and description_element and location_element and link_element and date_element:
            job_listings.append({
                'titulo': title_element.text.strip(),
                'descripcion': description_element.text.strip(),
                'ubicacion': location_element.text.strip(),
                'enlace': link_element['href'],
                'fecha_raw': date_element['datetime']  # Obtener el atributo datetime
            })
    return job_listings