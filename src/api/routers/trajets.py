from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session, joinedload, aliased
from sqlalchemy import func
from typing import Optional, List
from src.api.db.database import get_db
from src.api.models.models import FactTrajetTrain, DimGare, DimTrain, DimOperateur, DimDate
from src.api.schemas.schemas import TrajetResponse, PaginatedResponse

router = APIRouter()


@router.get(
    "",
    response_model=PaginatedResponse,
    summary="Lister les trajets",
    description="""
Retourne la liste des trajets ferroviaires avec filtres optionnels.

Exemples de requêtes :
- `GET /trajets?ville_depart=Paris&ville_arrivee=Lyon`
- `GET /trajets?operateur=SNCF&page=1&page_size=20`
- `GET /trajets?distance_min=100&distance_max=500`
- `GET /trajets?duree_max=120`
    """,
)
def get_trajets(
    # Filtres géographiques
    ville_depart: Optional[str] = Query(None, description="Ville de départ (ex: Paris)"),
    ville_arrivee: Optional[str] = Query(None, description="Ville d'arrivée (ex: Lyon)"),
    pays_depart: Optional[str] = Query(None, description="Pays de départ (ex: France)"),
    pays_arrivee: Optional[str] = Query(None, description="Pays d'arrivée (ex: Germany)"),
    gare_depart_id: Optional[int] = Query(None, description="ID exact de la gare de départ"),
    gare_arrivee_id: Optional[int] = Query(None, description="ID exact de la gare d'arrivée"),
    # Filtres train / opérateur
    operateur: Optional[str] = Query(None, description="Nom ou ID de l'opérateur (ex: SNCF)"),
    trip_headsign: Optional[str] = Query(None, description="Type/nom du train (ex: TGV, ICE)"),
    # Filtres mesures
    distance_min: Optional[float] = Query(None, description="Distance minimale en km"),
    distance_max: Optional[float] = Query(None, description="Distance maximale en km"),
    duree_min: Optional[float] = Query(None, description="Durée minimale en minutes"),
    duree_max: Optional[float] = Query(None, description="Durée maximale en minutes"),
    # Filtre date (cherche si date_id est contenu dans le tableau date_ids)
    date_id: Optional[int] = Query(None, description="Filtre sur un date_id présent dans le tableau"),
    # Pagination
    page: int = Query(1, ge=1, description="Numéro de page"),
    page_size: int = Query(20, ge=1, description="Résultats par page"),
    db: Session = Depends(get_db),
):
    query = db.query(FactTrajetTrain).options(
        joinedload(FactTrajetTrain.gare_depart),
        joinedload(FactTrajetTrain.gare_arrivee),
        joinedload(FactTrajetTrain.train),
        joinedload(FactTrajetTrain.operateur),
        joinedload(FactTrajetTrain.route),
    )

    # Jointures pour filtres sur les gares
    gare_depart_alias = aliased(DimGare)
    gare_arrivee_alias = aliased(DimGare)
    gare_depart_joined = False
    gare_arrivee_joined = False

    if ville_depart or pays_depart:
        query = query.join(
            gare_depart_alias,
            FactTrajetTrain.gare_depart_id == gare_depart_alias.gare_id,
        )
        gare_depart_joined = True

    if ville_arrivee or pays_arrivee:
        query = query.join(
            gare_arrivee_alias,
            FactTrajetTrain.gare_arrivee_id == gare_arrivee_alias.gare_id,
        )
        gare_arrivee_joined = True

    if ville_depart and gare_depart_joined:
        query = query.filter(gare_depart_alias.city.ilike(f"%{ville_depart}%"))

    if ville_arrivee and gare_arrivee_joined:
        query = query.filter(gare_arrivee_alias.city.ilike(f"%{ville_arrivee}%"))

    if pays_depart and gare_depart_joined:
        query = query.filter(gare_depart_alias.country.ilike(f"%{pays_depart}%"))

    if pays_arrivee and gare_arrivee_joined:
        query = query.filter(gare_arrivee_alias.country.ilike(f"%{pays_arrivee}%"))

    if operateur:
        query = query.join(DimOperateur).filter(
            (DimOperateur.agency_name.ilike(f"%{operateur}%"))
            | (DimOperateur.agency_id.ilike(f"%{operateur}%"))
        )

    if trip_headsign:
        query = query.join(DimTrain).filter(
            DimTrain.trip_headsign.ilike(f"%{trip_headsign}%")
        )

    if gare_depart_id:
        query = query.filter(FactTrajetTrain.gare_depart_id == gare_depart_id)

    if gare_arrivee_id:
        query = query.filter(FactTrajetTrain.gare_arrivee_id == gare_arrivee_id)

    if distance_min is not None:
        query = query.filter(FactTrajetTrain.distance_km >= distance_min)

    if distance_max is not None:
        query = query.filter(FactTrajetTrain.distance_km <= distance_max)

    if duree_min is not None:
        query = query.filter(FactTrajetTrain.duree_minutes >= duree_min)

    if duree_max is not None:
        query = query.filter(FactTrajetTrain.duree_minutes <= duree_max)

    # Filter on array containment: date_id = ANY(date_ids)
    if date_id is not None:
        query = query.filter(FactTrajetTrain.date_ids.any(date_id))

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=[TrajetResponse.model_validate(item) for item in items],
    )


@router.get(
    "/{fact_id}",
    response_model=TrajetResponse,
    summary="Détail d'un trajet",
    description="Retourne toutes les informations d'un trajet via son identifiant unique.",
)
def get_trajet(fact_id: int, db: Session = Depends(get_db)):
    trajet = (
        db.query(FactTrajetTrain)
        .options(
            joinedload(FactTrajetTrain.gare_depart),
            joinedload(FactTrajetTrain.gare_arrivee),
            joinedload(FactTrajetTrain.train),
            joinedload(FactTrajetTrain.operateur),
            joinedload(FactTrajetTrain.route),
            # date is no longer a relationship — loaded separately below if needed
        )
        .filter(FactTrajetTrain.fact_id == fact_id)
        .first()
    )

    if not trajet:
        raise HTTPException(status_code=404, detail=f"Trajet {fact_id} introuvable")

    # Hydrate the associated DimDate rows from the date_ids array
    if trajet.date_ids:
        trajet._dates = db.query(DimDate).filter(
            DimDate.date_id.in_(trajet.date_ids)
        ).all()
    else:
        trajet._dates = []

    return trajet