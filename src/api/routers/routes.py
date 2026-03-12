from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from src.api.db.database import get_db
from models.models import DimRoute
from schemas.schemas import RouteBase, PaginatedResponse

router = APIRouter()


@router.get("", response_model=PaginatedResponse, summary="Lister les lignes ferroviaires")
def get_routes(
    origin: Optional[str] = Query(None, description="Origine de la ligne"),
    destination: Optional[str] = Query(None, description="Destination de la ligne"),
    countries: Optional[str] = Query(None, description="Pays traversés"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    query = db.query(DimRoute)
    if origin:
        query = query.filter(DimRoute.origin.ilike(f"%{origin}%"))
    if destination:
        query = query.filter(DimRoute.destination.ilike(f"%{destination}%"))
    if countries:
        query = query.filter(DimRoute.countries.ilike(f"%{countries}%"))

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=[RouteBase.model_validate(r) for r in items],
    )


@router.get("/{route_id}", response_model=RouteBase, summary="Détail d'une ligne")
def get_route(route_id: int, db: Session = Depends(get_db)):
    route = db.query(DimRoute).filter(DimRoute.route_id == route_id).first()
    if not route:
        raise HTTPException(status_code=404, detail=f"Route {route_id} introuvable")
    return route
