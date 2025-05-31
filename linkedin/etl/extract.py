from prefect import task
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time  # Importa el módulo time
import random # Importa el módulo random si vas a usar pausas aleatorias

# Lista de user-agents realistas
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Mobile/15E148 Safari/604.1',
    # Agrega más user-agents de navegadores y sistemas operativos comunes
]

@task(name="extract_linkedin_jobs")
def extract_linkedin_jobs(url):
    # Selecciona un user-agent aleatorio de la lista
    random_user_agent = random.choice(USER_AGENTS)    
    headers = {
        'User-Agent': random_user_agent
    }
    try:
        print(f"Solicitando URL: {url} con User-Agent: {random_user_agent}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lanza una excepción para códigos de error HTTP
        soup = BeautifulSoup(response.content, 'html.parser')
        # Agregar una pausa después de cada solicitud
        time.sleep(random.uniform(10, 15))  # Pausa aleatoria entre 10 y 15 segundos

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
    #job_elements = soup.find_all('div', class_='base-card relative w-full hover:bg-gray-lightest p-2 transition-shadow')
    job_elements = soup.find_all('div', class_='job-search-card')  # Clase puede variar
    for job_element in job_elements:
        # Título del trabajo
        title_tag = job_element.find('h3', class_='base-search-card__title')
        title_element = title_tag.text.strip() if title_tag else ""
        # Empresa
        company_tag = job_element.find('a', class_='hidden-nested-link')
        company_element = company_tag.text.strip() if company_tag else ""
        # Lugar de trabajo (puede ser ciudad o "Remoto")
        location_tag = job_element.find('span', class_='job-search-card__location')
        location_element = location_tag.text.strip() if location_tag else ""
        # Descripción del trabajo (puede no estar presente en todos los listados)
        link_tag = job_element.find('a', class_='base-card__full-link')
        link_element = link_tag.text.strip() if link_tag else ""
        # Fecha de publicación (clase puede variar)
        date_tag = job_element.find('time', class_='job-search-card__listdate') or \
                job_element.find('time', class_='job-search-card__listdate--new')
        date_posted = date_tag.get('datetime') if date_tag else "No disponible"
        # Modalidad (remoto/híbrido/presencial)
        job_modalidad = "Remoto" if "remote" in title_element.lower() or "remoto" in location_element.lower() else "Presencial"
        # Extraer el link del trabajo (está en un <a> externo al bloque 'base-search-card__info')
        job_card = job_element.find_parent('li')
        if job_card:
            link_tag = job_card.find('a', class_='base-card__full-link')
            job_link = link_tag.get('href') if link_tag else "Link no encontrado"
        else:
            job_link = "Link no encontrado"
        if title_element and company_element and location_element and link_element and job_modalidad and job_link and  date_posted:
            job_listings.append({
                'titulo': title_element,
                'empresa': company_element,
                'descripcion': link_element,
                'ubicacion': location_element,
                'modalidad': job_modalidad,
                'link': job_link,
                'fecha': date_posted  # Obtener el atributo datetime
            })
    return job_listings