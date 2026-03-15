import requests
import pandas as pd
import ssl

ssl._create_default_https_context = ssl._create_unverified_context 

dataset_ids = {
    "5be56fd6634f4161bdf03fb2": "gares_europeennes",
    "5dccd2aa06e3e70687832c57": "horaires_des_gares"
}

cols_gares = [
    "id", "name", "slug","uic", "uic8_sncf", "latitude", "longitude", 
    "parent_station_id", "country", "timezone", "is_city", 
    "is_main_station", "is_airport", "sncf_id", "sncf_is_enable", 
    "trenitalia_id", "trenitalia_is_enable"
]

cols_obligatoires = [
    "id", "name", "country", "latitude", "longitude"
]

def get_data_gouv():
    dfs = {} 

    for id_ds, name in dataset_ids.items():
        try:
            url = f"https://transport.data.gouv.fr/api/datasets/{id_ds}"
            resources = requests.get(url).json().get('resources', [])
            
            for res in resources:
                if res['format'].upper() == 'CSV':
                    df = pd.read_csv(res['url'], storage_options={'verify_ssl': False}, sep=None, engine='python')
                    
                    if name == "gares_europeennes":
                        df = df[[c for c in cols_gares if c in df.columns]]
                        
                        cols_to_check = [c for c in cols_obligatoires if c in df.columns]
                        df = df.dropna(subset=cols_to_check)
                    else:
                        df = df.dropna()

                    dfs[name] = df
                    print(f"{name} : Récupéré et filtré ({len(df)} lignes)")
                    break
                
        except Exception as e:
            print(f"{name} : Impossible de récupérer les données ({e})")
    return dfs

# if "gares_europeennes" in dfs:
#     print("\n--- Aperçu du résultat final ---")
#     print(dfs["gares_europeennes"].head())