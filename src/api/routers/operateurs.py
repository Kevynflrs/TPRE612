from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from api.db.database import get_db
from models.models import DimOperateur
from schemas.schemas import OperateurBase, PaginatedResponse

router = APIRouter()


@router.get("", response_model=PaginatedResponse, summary="Lister les opérateurs")
def get_operateurs(
    agency_name: Optional[str] = Query(None, description="Nom de l'opérateur (ex: SNCF)"),
    agency_country: Optional[str] = Query(None, description="Pays de l'opérateur"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    query = db.query(DimOperateur)
    if agency_name:
        query = query.filter(DimOperateur.agency_name.ilike(f"%{agency_name}%"))
    if agency_country:
        query = query.filter(DimOperateur.agency_country.ilike(f"%{agency_country}%"))

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=[OperateurBase.model_validate(o) for o in items],
    )


@router.get("/{agency_id}", response_model=OperateurBase, summary="Détail d'un opérateur")
def get_operateur(agency_id: str, db: Session = Depends(get_db)):
    op = db.query(DimOperateur).filter(DimOperateur.agency_id == agency_id).first()
    if not op:
        raise HTTPException(status_code=404, detail=f"Opérateur {agency_id} introuvable")
    return op
