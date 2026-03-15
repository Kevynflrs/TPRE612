from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, literal
from typing import Optional
from src.api.db.database import get_db
from src.api.models.models import FactTrajetTrain, DimOperateur, DimGare, DimRoute

router = APIRouter()


@router.get(
    "/emissions",
    summary="Émissions CO2 par opérateur ou route",
    description="""
Statistiques d'émissions CO2 agrégées, idéal pour Grafana.

Exemples :
- `GET /stats/emissions` -> toutes les émissions par opérateur
- `GET /stats/emissions?group_by=route` -> par ligne ferroviaire
- `GET /stats/emissions?operateur=SNCF` -> filtré sur un opérateur
    """,
)
def get_emissions(
    group_by: str = Query("operateur", description="Grouper par : operateur ou route"),
    operateur: Optional[str] = Query(None, description="Filtrer par opérateur"),
    db: Session = Depends(get_db),
):
    if group_by == "route":
        query = (
            db.query(
                DimRoute.route_long_name.label("label"),
                func.sum(FactTrajetTrain.emissions_co2).label("total_emissions_co2"),
                func.avg(FactTrajetTrain.emissions_co2).label("moyenne_emissions_co2"),
                func.count(FactTrajetTrain.fact_id).label("nb_trajets"),
            )
            .join(DimRoute, FactTrajetTrain.route_id == DimRoute.route_id)
            .group_by(DimRoute.route_long_name)
            .order_by(func.sum(FactTrajetTrain.emissions_co2).desc())
        )
    else:
        query = (
            db.query(
                DimOperateur.agency_name.label("label"),
                func.sum(FactTrajetTrain.emissions_co2).label("total_emissions_co2"),
                func.avg(FactTrajetTrain.emissions_co2).label("moyenne_emissions_co2"),
                func.count(FactTrajetTrain.fact_id).label("nb_trajets"),
            )
            .join(DimOperateur, FactTrajetTrain.operator_id == DimOperateur.agency_id)
            .group_by(DimOperateur.agency_name)
            .order_by(func.sum(FactTrajetTrain.emissions_co2).desc())
        )

    if operateur:
        query = query.filter(DimOperateur.agency_name.ilike(f"%{operateur}%"))

    results = query.all()
    return [
        {
            "label": r.label,
            "total_emissions_co2": round(r.total_emissions_co2 or 0, 2),
            "moyenne_emissions_co2": round(r.moyenne_emissions_co2 or 0, 2),
            "nb_trajets": r.nb_trajets,
        }
        for r in results
    ]


@router.get(
    "/frequentation",
    summary="Fréquentation par gare",
    description="""
Nombre de passagers agrégés par gare de départ ou d'arrivée.

**Exemples :**
- `GET /stats/frequentation?type=depart`
- `GET /stats/frequentation?type=arrivee&pays=France`
    """,
)
def get_frequentation(
    type: str = Query("depart", description="Type : depart ou arrivee"),
    pays: Optional[str] = Query(None, description="Filtrer par pays"),
    db: Session = Depends(get_db),
):
    if type == "arrivee":
        gare_id_col = FactTrajetTrain.gare_arrivee_id
    else:
        gare_id_col = FactTrajetTrain.gare_depart_id

    query = (
        db.query(
            DimGare.name.label("gare"),
            DimGare.city.label("ville"),
            DimGare.country.label("pays"),
            literal(0.0).label("total_passengers"),
            func.count(FactTrajetTrain.fact_id).label("nb_trajets"),
        )
        .join(DimGare, gare_id_col == DimGare.gare_id)
        .group_by(DimGare.name, DimGare.city, DimGare.country)
        .order_by(func.count(FactTrajetTrain.fact_id).desc())
    )

    if pays:
        query = query.filter(DimGare.country.ilike(f"%{pays}%"))

    results = query.all()
    return [
        {
            "gare": r.gare,
            "ville": r.ville,
            "pays": r.pays,
            "total_passengers": round(r.total_passengers or 0, 0),
            "nb_trajets": r.nb_trajets,
        }
        for r in results
    ]


@router.get(
    "/performance",
    summary="Performance par opérateur",
    description="""
Vitesse moyenne, durée et distance par opérateur ferroviaire.

**Exemple :**
- `GET /stats/performance`
- `GET /stats/performance?operateur=Eurostar`
    """,
)
def get_performance(
    operateur: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = (
        db.query(
            DimOperateur.agency_name.label("operateur"),
            func.avg(FactTrajetTrain.average_speed).label("vitesse_moyenne"),
            func.avg(FactTrajetTrain.duree_minutes).label("duree_moyenne_minutes"),
            func.avg(FactTrajetTrain.distance_km).label("distance_moyenne_km"),
            func.count(FactTrajetTrain.fact_id).label("nb_trajets"),
        )
        .join(DimOperateur, FactTrajetTrain.operator_id == DimOperateur.agency_id)
        .group_by(DimOperateur.agency_name)
        .order_by(func.avg(FactTrajetTrain.average_speed).desc())
    )

    if operateur:
        query = query.filter(DimOperateur.agency_name.ilike(f"%{operateur}%"))

    results = query.all()
    return [
        {
            "operateur": r.operateur,
            "vitesse_moyenne_kmh": round(r.vitesse_moyenne or 0, 1),
            "duree_moyenne_minutes": round(r.duree_moyenne_minutes or 0, 1),
            "distance_moyenne_km": round(r.distance_moyenne_km or 0, 1),
            "nb_trajets": r.nb_trajets,
        }
        for r in results
    ]


@router.get(
    "/resume",
    summary="Résumé global",
    description="Chiffres clés globaux.",
)
def get_resume(db: Session = Depends(get_db)):
    result = db.query(
        func.count(FactTrajetTrain.fact_id).label("total_trajets"),
        func.sum(FactTrajetTrain.emissions_co2).label("total_emissions_co2"),
        literal(0.0).label("total_passengers"),
        func.avg(FactTrajetTrain.distance_km).label("distance_moyenne_km"),
        func.avg(FactTrajetTrain.duree_minutes).label("duree_moyenne_minutes"),
        func.avg(FactTrajetTrain.average_speed).label("vitesse_moyenne_kmh"),
    ).first()

    if result is None:
        return {
            "total_trajets": 0,
            "total_emissions_co2_kg": 0.0,
            "total_passengers": 0.0,
            "distance_moyenne_km": 0.0,
            "duree_moyenne_minutes": 0.0,
            "vitesse_moyenne_kmh": 0.0,
        }

    return {
        "total_trajets": result.total_trajets or 0,
        "total_emissions_co2_kg": round(result.total_emissions_co2 or 0, 2),
        "total_passengers": round(result.total_passengers or 0, 0),
        "distance_moyenne_km": round(result.distance_moyenne_km or 0, 1),
        "duree_moyenne_minutes": round(result.duree_moyenne_minutes or 0, 1),
        "vitesse_moyenne_kmh": round(result.vitesse_moyenne_kmh or 0, 1),
    }
