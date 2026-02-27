import requests
import json
import time
import os

# --- CONFIGURATION ---
API_KEY = "AzPZkCCJMdA2RJByP1rqxdGnVRXcewkh"
OUTPUT_DIR = "transit_data_export"

# Création du dossier d'export s'il n'existe pas
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# Paramètres de base (limit à 100, on gère le reste avec la pagination) 1000000000000000000
BASE_PARAMS = {
    "route_type": "2",
    "limit": "10", 
    "apikey": API_KEY
}

# 1. Les Endpoints "Globaux" (qui ne nécessitent pas d'ID spécifique)
GLOBAL_ENDPOINTS = {
    "routes": "https://transit.land/api/v2/rest/routes",
    "feeds": "https://transit.land/api/v2/rest/feeds",
    "feed_versions": "https://transit.land/api/v2/rest/feed_versions",
    "agencies": "https://transit.land/api/v2/rest/agencies",
    "stops": "https://transit.land/api/v2/rest/stops"
}

def fetch_transitland_data(name, url, params):
    """
    Récupère toutes les pages d'un endpoint via pagination et sauvegarde en JSON.
    """
    print(f"\n--- Démarrage de l'extraction : {name.upper()} ---")
    all_data =[]
    current_url = url
    current_params = params.copy()

    while current_url:
        try:
            # S'il y a des paramètres, on les passe (uniquement pour la 1ère requête)
            # Pour les pages suivantes, le lien 'next' contient déjà les paramètres et l'API KEY.
            if current_params:
                response = requests.get(current_url, params=current_params, headers=HEADERS)
            else:
                response = requests.get(current_url, headers=HEADERS)
            
            if response.status_code == 200:
                data = response.json()
                
                # Dans Transit.land, les données sont dans une clé qui porte le nom de l'endpoint (ex: data['routes'])
                # On trouve dynamiquement cette clé en excluant 'meta'
                data_keys = [k for k in data.keys() if k != 'meta']
                if data_keys:
                    main_key = data_keys[0]
                    all_data.extend(data[main_key])
                    print(f"[{name}] Page récupérée. Total provisoire : {len(all_data)} éléments")
                
                # Gestion de la PAGINATION
                if 'meta' in data and 'next' in data['meta'] and data['meta']['next']:
                    current_url = data['meta']['next']
                    current_params = None  # On annule les params car le lien 'next' les intègre déjà
                    time.sleep(0.5)  # PAUSE de 0.5s pour ne pas se faire bloquer par l'API (Rate Limit)
                else:
                    break # Plus de page 'next', on sort de la boucle
            else:
                print(f"Erreur {response.status_code} sur l'URL : {current_url}")
                try:
                    print(response.json())
                except:
                    print(response.text)
                break
                
        except Exception as e:
            print(f"Exception lors de la connexion : {e}")
            break

    # Sauvegarde dans un fichier JSON
    filepath = os.path.join(OUTPUT_DIR, f"{name}.json")
    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(all_data, file, indent=4, ensure_ascii=False)
        
    print(f"FINI : {len(all_data)} {name} enregistrés dans '{filepath}'.")
    return all_data

# ==========================================
# EXÉCUTION DU PIPELINE
# ==========================================
if __name__ == "__main__":
    
    # 1. Extraction de tous les endpoints globaux
    fetched_data = {}
    for endpoint_name, endpoint_url in GLOBAL_ENDPOINTS.items():
        fetched_data[endpoint_name] = fetch_transitland_data(endpoint_name, endpoint_url, BASE_PARAMS)
        
    
    # 2. COMMENT GÉRER LES TRIPS ET DEPARTURES ?
    # Comme il faut un ID pour chaque, voici un exemple de comment récupérer 
    # les "trips" de la TOUTE PREMIÈRE ROUTE récupérée juste au-dessus :
    
    if "routes" in fetched_data and len(fetched_data["routes"]) > 0:
        print("\n--- Exemple d'extraction dépendante (Trips) ---")
        
        # On prend l'ID de la première route trouvée (ex: 'r-u-route1')
        first_route_id = fetched_data["routes"][0].get("onestop_id")
        
        if first_route_id:
            trips_url = f"https://transit.land/api/v2/rest/routes/{first_route_id}/trips"
            print(f"Récupération des trajets (trips) pour la route : {first_route_id}")
            
            # On réutilise notre fonction magique !
            fetch_transitland_data(f"trips_route_{first_route_id}", trips_url, BASE_PARAMS)

    print("\nToutes les extractions sont terminées !")