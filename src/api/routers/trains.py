from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from src.api.db.database import get_db
from models.models import DimTrain, DimRoute
from schemas.schemas import TrainBase, PaginatedResponse

router = APIRouter()


@router.get(
    "",
    response_model=PaginatedResponse,
    summary="Lister les trains",
    description="""
Exemples :
- `GET /trains?trip_headsign=TGV`
- `GET /trains?origin=Paris`
    """,
)
def get_trains(
    trip_headsign: Optional[str] = Query(None, description="Type de train (ex: TGV, ICE, Eurostar)"),
    origin: Optional[str] = Query(None, description="Ville d'origine"),
    destination: Optional[str] = Query(None, description="Ville de destination"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    query = db.query(DimTrain)

    if trip_headsign:
        query = query.filter(DimTrain.trip_headsign.ilike(f"%{trip_headsign}%"))
    if origin:
        query = query.filter(DimTrain.trip_origin.ilike(f"%{origin}%"))
    if destination:
        query = query.join(DimRoute, DimTrain.route_id == DimRoute.route_id).filter(
            DimRoute.destination.ilike(f"%{destination}%")
        )

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    return PaginatedResponse(
        total=total,
        page=page,
        page_size=page_size,
        data=[TrainBase.model_validate(t) for t in items],
    )


@router.get("/{trip_id}", response_model=TrainBase, summary="Détail d'un train")
def get_train(trip_id: str, db: Session = Depends(get_db)):
    train = db.query(DimTrain).filter(DimTrain.trip_id == trip_id).first()
    if not train:
        raise HTTPException(status_code=404, detail=f"Train {trip_id} introuvable")
    return train
