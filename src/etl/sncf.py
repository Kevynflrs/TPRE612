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

        print(f"Récupéré {len(records)} / {total} enregistrements...")

        if offset >= total:
            break

    return records

records = fetch_all_datas()
df = pd.DataFrame(records)

# Transform
# COLS_TO_DROP = inutile pour nous
COLS_TO_DROP = [
    "autocar_longue_distance_empreinte_carbone_kgco2e",
    "voiture_electrique_2_2_pers_empreinte_carbone_kgco2e",
    "voiture_thermique_2_2_pers_empreinte_carbone_kgco2e",
]

existing_cols_to_drop = [c for c in COLS_TO_DROP if c in df.columns]
df.drop(columns=existing_cols_to_drop, inplace=True)

# rajout de la durée
VITESSES_MOYENNES = {
    "TGV":          280,
    "International": 200,
    "Intercités":   140,
    "TER":          90,
}
VITESSE_DEFAUT = 150

def calculer_duree(row: pd.Series) -> float | None:
    """
    Retourne la durée estimée en minutes à partir de la distance
    ferroviaire et de la vitesse moyenne du type de train.
    """
    distance = row.get("distance_entre_les_gares")
    transporteur = str(row.get("transporteur", "")).strip()

    if pd.isna(distance) or distance <= 0: # pas de distance valide = on calcule pas la durée
        return None

    # on cherche la vitesse moyenne à partir du transporteur
    vitesse = VITESSE_DEFAUT
    for key, val in VITESSES_MOYENNES.items():
        if key.lower() in transporteur.lower():
            vitesse = val
            break

    return round((distance / vitesse) * 60, 1) # durée en minutes arrondie


df["duree_estimee_min"] = df.apply(calculer_duree, axis=1)