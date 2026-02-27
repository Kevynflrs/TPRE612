import requests
import pandas as pd
import ssl
import io

ssl._create_default_https_context = ssl._create_unverified_context 

api_co2_url = "https://www.data.gouv.fr/api/1/datasets/emissions-de-co2e-perimetre-complet/"

cols_utiles_co2 = [
    "Transporteur", 
    "Origine", 
    "Origine_uic", 
    "Destination", 
    "Destination_uic", 
    "Distance entre les gares", 
    "Train - Empreinte carbone (kgCO2e)", 
    "Distance aérienne", 
    "Avion - Empreinte carbone (kgCO2e)"
]

def fetch_co2_data(api_url):
    try:
        response = requests.get(api_url)
        dataset_info = response.json()
        
        csv_url = None
        for resource in dataset_info.get('resources', []):
            if resource['format'].lower() == 'csv':
                csv_url = resource['url']
                break
        
        if csv_url:
            res = requests.get(csv_url)
            content = res.content.decode('utf-8-sig') 
            df_co2 = pd.read_csv(io.StringIO(content), sep=';')
            
            df_co2.columns = df_co2.columns.str.strip()
            
            colonnes_finales = [c for c in cols_utiles_co2 if c in df_co2.columns]
            df_co2 = df_co2[colonnes_finales]

            cols_critiques = ["Destination", "Distance entre les gares", "Train - Empreinte carbone (kgCO2e)", "Origine"]
            cols_existantes = [c for c in cols_critiques if c in df_co2.columns]
            df_co2 = df_co2.dropna(subset=cols_existantes)

            print(f"\nDataset CO2 filtré et nettoyé : {len(df_co2)} lignes restantes.")
            
            return df_co2
        else:
            print("Aucune ressource CSV trouvée.")
            return None

    except Exception as e:
        print(f"Impossible de récupérer ou filtrer l'API CO2 : {e}")
        return None

df_emissions = fetch_co2_data(api_co2_url)

if df_emissions is not None:
    print("\nColonnes conservées :", df_emissions.columns.tolist())
    print(df_emissions.head())