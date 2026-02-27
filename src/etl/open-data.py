import requests
import pandas as pd
import ssl

ssl._create_default_https_context = ssl._create_unverified_context #

dataset_ids = {
    "65d81858179dc96581d981db": "Gares de voyageurs",
    "59593619a3a7291dd09c8238": "Gares du réseau",
    "5be56fd6634f4161bdf03fb2": "Gares européennes",
    "5dccd2aa06e3e70687832c57": "Horaires des gares",
    "59593617a3a7291dcf9c8274": "Passages à niveau"
}

dfs = {} # dfs.get("Gares de voyageurs") --> récupe le df

for id_ds, name in dataset_ids.items():
    try:
        url = f"https://transport.data.gouv.fr/api/datasets/{id_ds}"
        resources = requests.get(url).json().get('resources', [])
        
        for res in resources:
            if res['format'].upper() == 'CSV':
                df = pd.read_csv(res['url'], storage_options={'verify_ssl': False}).dropna()
                dfs[name] = df
                print(f"[OK] {name} : Récupéré et nettoyé ({len(df)} lignes)")
                break
            
    except Exception as e:
        print(f"[ERREUR] {name} : Impossible de récupérer les données ({e})")


if dfs.get("Gares de voyageurs") is not None:
    print(" Premières lignes de 'Gares de voyageurs'")
    print(dfs.get("Gares de voyageurs").head())