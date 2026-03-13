# API REST – Dessertes Ferroviaires

Documentation technique de l'API REST exposant les données ferroviaires du data warehouse.

---

## Installation

### Prérequis
- Python 3.10+
- PostgreSQL

### Mise en place

```bash
# 1. Cloner le repo et aller dans le dossier api/
cd api/

# 2. Créer et activer un environnement virtuel
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer la connexion base de données
cp .env.example .env
# Éditer .env et renseigner DATABASE_URL

# 5. Lancer l'API
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

L'API est disponible sur `http://localhost:8000`  
Documentation Swagger : `http://localhost:8000/docs`  

---

## Structure du projet

```
api/
├── main.py               # Point d'entrée FastAPI
├── db/
│   └── database.py       # Connexion SQLAlchemy
├── models/
│   └── models.py         # Modèles ORM (tables BDD)
├── schemas/
│   └── schemas.py        # Schémas Pydantic (réponses API)
├── routers/
│   ├── trajets.py        # /trajets
│   ├── gares.py          # /gares
│   ├── trains.py         # /trains
│   ├── operateurs.py     # /operateurs
│   ├── routes.py         # /routes
│   └── stats.py          # /stats
├── requirements.txt
├── .env
└── README.md
```

---

## Endpoints

### Santé
| Méthode | Endpoint  | Description              |
|---------|-----------|--------------------------|
| GET     | `/`       | Statut de l'API          |
| GET     | `/health` | Health check             |

---

### Trajets – `/trajets`

#### `GET /trajets` – Lister les trajets

**Paramètres de filtre :**

| Paramètre        | Type    | Description                          | Exemple           |
|------------------|---------|--------------------------------------|-------------------|
| `ville_depart`   | string  | Ville de départ (partiel, insensible) | `Paris`           |
| `ville_arrivee`  | string  | Ville d'arrivée                      | `Lyon`            |
| `pays_depart`    | string  | Pays de départ                       | `France`          |
| `operateur`      | string  | Nom ou ID opérateur                  | `SNCF`            |
| `trip_headsign`  | string  | Type de train                        | `TGV`             |
| `distance_min`   | float   | Distance minimale (km)               | `100`             |
| `distance_max`   | float   | Distance maximale (km)               | `500`             |
| `duree_min`      | float   | Durée minimale (minutes)             | `30`              |
| `duree_max`      | float   | Durée maximale (minutes)             | `180`             |
| `page`           | int     | Page (défaut: 1)                     | `2`               |
| `page_size`      | int     | Résultats/page, max 100 (défaut: 20) | `50`              |

**Exemples de requêtes :**

```bash
# Trajets Paris → Lyon
GET /trajets?ville_depart=Paris&ville_arrivee=Lyon

# TGV en moins de 3h
GET /trajets?trip_headsign=TGV&duree_max=180

# Trajets SNCF de plus de 500 km, page 2
GET /trajets?operateur=SNCF&distance_min=500&page=2

# Tous les trajets (paginés)
GET /trajets?page=1&page_size=50
```

**Exemple de réponse :**

```json
{
  "total": 1240,
  "page": 1,
  "page_size": 20,
  "data": [
    {
      "fact_id": 42,
      "distance_km": 512.3,
      "duree_minutes": 117.0,
      "emissions_co2": 4.2,
      "passengers": 350.0,
      "average_speed": 262.7,
      "gare_depart": {
        "gare_id": 1,
        "name": "Paris Gare de Lyon",
        "city": "Paris",
        "country": "France",
        "latitude": 48.8448,
        "longitude": 2.3735,
        "is_main_station": true
      },
      "gare_arrivee": {
        "gare_id": 12,
        "name": "Lyon Part-Dieu",
        "city": "Lyon",
        "country": "France",
        "latitude": 45.7605,
        "longitude": 4.8596,
        "is_main_station": true
      },
      "operateur": {
        "agency_id": "SNCF",
        "agency_name": "SNCF",
        "agency_url": "https://www.sncf.com",
        "agency_country": "France"
      }
    }
  ]
}
```

#### `GET /trajets/{fact_id}` – Détail d'un trajet

```bash
GET /trajets/42
```

---

### Gares – `/gares`

```bash
# Toutes les gares françaises principales
GET /gares?country=France&is_main_station=true

# Recherche par nom
GET /gares?name=Gare+de+Lyon

# Gares d'une ville
GET /gares?city=Bruxelles
```

---

### Trains – `/trains`

```bash
# Tous les TGV
GET /trains?trip_headsign=TGV

# Trains au départ de Paris
GET /trains?origin=Paris
```

---

### Opérateurs – `/operateurs`

```bash
# Tous les opérateurs
GET /operateurs

# Opérateurs français
GET /operateurs?agency_country=France

# Rechercher SNCF
GET /operateurs?agency_name=SNCF
```

---

### Routes (lignes) – `/routes`

```bash
# Lignes depuis Paris
GET /routes?origin=Paris

# Lignes traversant la France et l'Espagne
GET /routes?countries=France
```

---

### Statistiques – `/stats`

#### `GET /stats/resume` – KPIs globaux

Parfait pour les **panels de synthèse Grafana**.

```bash
GET /stats/resume
```

```json
{
  "total_trajets": 15420,
  "total_emissions_co2_kg": 98450.50,
  "total_passengers": 5230000.0,
  "distance_moyenne_km": 387.2,
  "duree_moyenne_minutes": 142.5,
  "vitesse_moyenne_kmh": 218.3
}
```

#### `GET /stats/emissions` – CO2 par opérateur ou route

```bash
# Par opérateur (défaut)
GET /stats/emissions

# Par ligne ferroviaire
GET /stats/emissions?group_by=route

# Filtré sur Eurostar
GET /stats/emissions?operateur=Eurostar
```

#### `GET /stats/frequentation` – Passagers par gare

```bash
# Gares les plus fréquentées au départ
GET /stats/frequentation?type=depart

# Gares d'arrivée en France
GET /stats/frequentation?type=arrivee&pays=France
```

#### `GET /stats/performance` – Performance par opérateur

```bash
GET /stats/performance
GET /stats/performance?operateur=Thalys
```

---

## Intégration Grafana

L'API est compatible avec le plugin **JSON API datasource** de Grafana.

### Configuration dans Grafana

1. Installer le plugin : `grafana-cli plugins install marcusolsson-json-datasource`
2. Ajouter une datasource : `http://localhost:8000`
3. Configurer les panels avec les endpoints stats

### Exemples de panels recommandés

| Panel Grafana      | Endpoint                                  | Champ valeur             |
|--------------------|-------------------------------------------|--------------------------|
| Stat – Total trajets | `GET /stats/resume`                    | `total_trajets`          |
| Stat – CO2 total   | `GET /stats/resume`                       | `total_emissions_co2_kg` |
| Bar chart – CO2 par opérateur | `GET /stats/emissions`         | `total_emissions_co2`    |
| Bar chart – Fréquentation | `GET /stats/frequentation`       | `total_passengers`       |
| Table – Performance | `GET /stats/performance`                 | toutes les colonnes      |

---

## Variables d'environnement

| Variable       | Description                      | Exemple                                        |
|----------------|----------------------------------|------------------------------------------------|
| `DATABASE_URL` | URL de connexion PostgreSQL      | `postgresql://user:pass@localhost:5432/db_name` |

---

## Notes techniques

- Tous les filtres textuels sont **insensibles à la casse** et acceptent des **valeurs partielles**
- La pagination utilise `page` (base 1) et `page_size` (max 100)
- Les réponses respectent le format JSON standard avec `Content-Type: application/json`
- La documentation interactive Swagger est auto-générée sur `/docs`
