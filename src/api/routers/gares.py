from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from db.database import get_db
from models.models import DimGare
from schemas.schemas import GareBase, PaginatedResponse

router = APIRouter()


@router.get(
    "",
    response_model=PaginatedResponse,
    summary="Lister les gares",
    description="""
Retourne la liste des gares avec filtres optionnels.

Exemples :
- `GET /gares?city=Paris`
- `GET /gares?country=France&is_main_station=true`
- `GET /gares?name=Gare+de+Lyon`
    """,
)
def get_gares(
    name: Optional[str] = Query(None, description="Nom de la gare"),
    city: Optional[str] = Query(None, description="Ville (ex: Paris, Lyon)"),
    country: Optional[str] = Query(None, description="Pays (ex: France, Germany)"),
    is_main_station: Optional[bool] = Query(None, description="Gare principale uniquement"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    query = db.query(DimGare)

    if name:
        query = query.filter(DimGare.name.ilike(f"%{name}%"))
    if city:
        query = query.filter(DimGare.city.ilike(f"%{city}%"))
    if country:
        query = query.filter(DimGare.country.ilike(f"%{country}%"))
    if is_main_station is not None:
        query = query.filter(DimGare.is_main_station == is_main_station)

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=[GareBase.model_validate(g) for g in items],
    )


@router.get("/{gare_id}", response_model=GareBase, summary="Détail d'une gare")
def get_gare(gare_id: int, db: Session = Depends(get_db)):
    gare = db.query(DimGare).filter(DimGare.gare_id == gare_id).first()
    if not gare:
        raise HTTPException(status_code=404, detail=f"Gare {gare_id} introuvable")
    return gare
