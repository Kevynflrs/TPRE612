import requests
import pandas as pd

def etl_process_europa_portal(dataset_id):
    """
    Fonction qui extrait et nettoie un dataset depuis data.europa.eu
    """
    api_url = f"https://data.europa.eu/api/hub/search/datasets/{dataset_id}"
    
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            metadata = response.json()
            distributions = metadata.get('result', {}).get('distributions', [])
            
            download_url = None
            for dist in distributions:
                format_type = dist.get('format', {}).get('id', '').lower()
                if 'csv' in format_type:
                    download_url = dist.get('access_url') or dist.get('download_url')
                    break
            
            if download_url:
                print(f" Téléchargement : {download_url}")
                df = pd.read_csv(download_url, engine='python')
                
                df = df.drop_duplicates()
                
                df = df.dropna(how='all') 
                
                print(f" {len(df)} lignes traitées.")
                return df
            else:
                print(f" Aucun CSV trouvé pour {dataset_id}")
        else:
            print(f" Code {response.status_code} pour l'ID {dataset_id}")
            
    except Exception as e:
        print(f"   [ERREUR CRITIQUE] : {e}")
    return None

# 2. EXECUTION DU PROCESSUS SUR PLUSIEURS IDs
# Ajoute ici tous les IDs que tu souhaites traiter
# liste_datasets = [
#     "fdfc3d62-86dd-4104-853f-2c89e676561f",
#     "gxrjq6yzvzzjtrx2cv9vsg",
#     "qg5eeq1bhoqolcow3fojra"
# ]
liste_datasets = [
    {
        "id": "fdfc3d62-86dd-4104-853f-2c89e676561f",
        "name": "trajet_train_europe"
    },
    {
        "id": "gxrjq6yzvzzjtrx2cv9vsg",
        "name": "source_energie_train_perf"
    },
    {
        "id": "qg5eeq1bhoqolcow3fojra",
        "name": "nombre_passagers_train"
    }
]



def get_data_europa():
    resultats_datasets = {}

    for ds in liste_datasets:
        ds_id = ds["id"]
        ds_name = ds["name"]
        print(f"\nTraitement de la source : {ds_name} (ID: {ds_id})...")
        df_temp = etl_process_europa_portal(ds_id)
        
        if df_temp is not None:
            resultats_datasets[ds_name] = df_temp
            print(f"-> Source {ds_name} enregistrée.")
        else:
            print(f"-> Source {ds_name} ignorée suite à une erreur.")
    return resultats_datasets

