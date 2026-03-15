import requests
import pandas as pd
import ssl
import io

ssl._create_default_https_context = ssl._create_unverified_context 

API_URL = "https://www.data.gouv.fr/api/1/datasets/emissions-de-co2e-perimetre-complet/"
COLS_UTILES = [
    "Transporteur", "Origine", "Origine_uic", "Destination", 
    "Destination_uic", "Distance entre les gares", 
    "Train - Empreinte carbone (kgCO2e)", "Distance aérienne", 
    "Avion - Empreinte carbone (kgCO2e)"
]
COLS_OBLIGATOIRES = [
    "Destination", "Distance entre les gares", 
    "Train - Empreinte carbone (kgCO2e)", "Origine"
]

def get_co2_data():
    """
    Fonction ETL complète :
    - Extract : Récupère le CSV depuis l'API data.gouv
    - Transform : Nettoie les noms, filtre les colonnes et supprime les lignes vides
    - Load : Retourne le DataFrame prêt à l'emploi
    """
    try:
        # EXTRACT
        response = requests.get(API_URL)
        response.raise_for_status()
        dataset_info = response.json()
        
        csv_url = next((r['url'] for r in dataset_info.get('resources', []) if r['format'].lower() == 'csv'), None)
        
        if not csv_url:
            print("Erreur : Aucune ressource CSV trouvée.")
            return None

        res = requests.get(csv_url)
        content = res.content.decode('utf-8-sig') 
        df = pd.read_csv(io.StringIO(content), sep=';')

        # TRANSFORM
        df.columns = df.columns.str.strip()
        cols_presentes = [c for c in COLS_UTILES if c in df.columns]
        df = df[cols_presentes]
        cols_a_nettoyer = [c for c in COLS_OBLIGATOIRES if c in df.columns]
        df = df.dropna(subset=cols_a_nettoyer)

        print(f"Succès : ETL terminé. {len(df)} lignes traitées.")
        
        # LOAD
        return df

    except Exception as e:
        print(f"Échec de l'ETL : {e}")
        return None

df_final = get_co2_data()

if df_final is not None:
    print(df_final.head())