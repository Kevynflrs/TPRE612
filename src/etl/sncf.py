import requests
import pandas as pd

BASE_URL = (
    "https://ressources.data.sncf.com/api/explore/v2.1"
    "/catalog/datasets/emission-co2-perimetre-complet/records"
)

# Extract
def fetch_all_datas(limit: int = 100) -> list[dict]:
    """Récupère tous les enregistrements en gérant la pagination."""
    records = []
    offset = 0 # L'API utilise un système de pagination avec "limit" et "offset"

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        batch = data.get("results", [])
        records.extend(batch)

        total = data.get("total_count", 0)
        offset += limit

        print(f"  Récupéré {len(records)} / {total} enregistrements...")

        if offset >= total:
            break

    return records

records = fetch_all_datas()
df = pd.DataFrame(records)