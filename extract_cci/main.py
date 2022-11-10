import re
import math
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
from multiprocessing import cpu_count


def getttext(arg, delim=""):
    if len(arg) > 0:
        return arg[0].get_text(delim)
    return ""


def clean(string):
    return re.sub(r'\s+', ' ', string).strip()

def get_adresse(body):
    adresse = clean(getttext(body, ";"))
    splitted_adresse = adresse.strip().split(";")
    street = splitted_adresse[0]
    cp = re.sub(r'[a-zA-Z]|\s', "", splitted_adresse[1])
    city = re.sub(r'\d|\s', "", splitted_adresse[1])

    return street, cp, city

def get(array, elem):
    for i, l in enumerate(array):
        if l == elem:
            return i
    return -1

def str_if_nil(array, elem):
    idx = get(array, elem) + 1
    if idx < 0:
        return ""
    return array[idx]

def get_company_infos(body):
    nom = body.findChildren("div", id="entreprise-nom")[0].findChildren("span")[1].text
    personnes = []
    personnes_html = body.findChildren("ul", id="entreprise-personnes")[0].findChildren("li")
    for personne_html in personnes_html:
        personnes.append(personne_html.text.split(" - ")[0])

    street, cp, city = get_adresse(body.findChildren("div", id="entreprise-adresse"))
    activite = clean(getttext(body.findChildren("div", id="entreprise-activite")))
    activite = activite.replace("Activité : ", "", 1)

    tel = getttext(body.select("a[href*=tel]"))
    email = getttext(body.select("a[href*=mailto]"))
    site = getttext(body.findChildren("a", id="entreprise-internet"))
    
    a = clean(getttext(body.findChildren("div", id="entreprise-details"), ";"))
    a = a.split(";")

    effectif = str_if_nil(a, "Effectif")
    siret = str_if_nil(a, "n° SIRET")
    NAF = str_if_nil(a, "n° APE")
    chiffre_affaire = str_if_nil(a, "Chiffre d'affaire")
    annee_debut = str_if_nil(a, "Année de début de l'activité de l'établissement")
    print("; ".join([nom, ", ".join(personnes), street, cp, city, activite, tel, email, site, effectif, siret, NAF, chiffre_affaire, annee_debut]))

def get_companies(link, companies):
    for company in companies[:10]:
        try:
            page = requests.get(link + company.parent.attrs["href"])
            html_soup = BeautifulSoup(page.text, 'html.parser')
            body = html_soup.find('div', id = 'content-entreprise-middle') 
            if body != None:
                get_company_infos(body)
        except requests.exceptions.ConnectionError:
            continue
    return

def get_company_count(html_soup):
    header = html_soup.find('div', class_ = 'header-top')
    text = getttext(header.findChildren("h2"))
    value = text.split(" ")[0]
    try:
        return int(value)
    except ValueError:
        return 0

def get_activity(link):
    try:
        page = requests.get(link)
        html_soup = BeautifulSoup(page.text, 'html.parser')
        count = get_company_count(html_soup)
        # print("\t", 0)
    except requests.exceptions.ConnectionError:
        pass

    get_companies(link, html_soup.find_all('div', class_ = 'entreprise-container'))

    i = 1
    while i < math.ceil(count / 10):
        try:
            # print("\t", i)
            page = requests.get(link + "?histstate=" + str(i) + "&annuaireEntreprise_start=" + str(10*i))
            html_soup = BeautifulSoup(page.text, 'html.parser')
            get_companies(link, html_soup.find_all('div', class_ = 'entreprise-container'))
        except requests.exceptions.ConnectionError:
            continue
        i += 1
    return


if __name__ == "__main__":
    print("; ".join(["nom", "personnes", "adresse", "code postal", "ville", "activite", "tel", "email", "site web", "effectif", "siret", "NAF", "chiffre d'affaire", "annee_debut"]))

    cci = requests.get("https://www.lyon-metropole.cci.fr/jcms/annuaire-entreprises/annuaire-entreprises-d_5815.html")
    html_soup = BeautifulSoup(cci.text, 'html.parser')
    activities = html_soup.find_all('a', class_ = 'code-naf')
    with Pool(cpu_count()) as p:
        p.map(get_activity, ["https://www.lyon-metropole.cci.fr/" + activity.attrs["href"] for activity in activities])
