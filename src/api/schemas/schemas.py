from pydantic import BaseModel
from typing import Optional
from datetime import date



#Gare
class GareBase(BaseModel):
    gare_id: int
    name: str
    city: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    is_main_station: Optional[bool]

    class Config:
        from_attributes = True



#Train
class TrainBase(BaseModel):
    trip_id: str
    route_id: Optional[int]
    trip_headsign: Optional[str]
    origin: Optional[str]
    destination: Optional[str]
    duration: Optional[str]
    distance: Optional[float]

    class Config:
        from_attributes = True



#Opérateur
class OperateurBase(BaseModel):
    agency_id: str
    agency_name: Optional[str]
    agency_url: Optional[str]
    agency_country: Optional[str]

    class Config:
        from_attributes = True



#Route
class RouteBase(BaseModel):
    route_id: int
    agency_id: Optional[str]
    route_long_name: Optional[str]
    origin: Optional[str]
    destination: Optional[str]
    countries: Optional[str]

    class Config:
        from_attributes = True



#Trajet
class TrajetResponse(BaseModel):
    fact_id: int
    distance_km: Optional[float]
    duree_minutes: Optional[float]
    emissions_co2: Optional[float]
    # passengers: Optional[float]
    average_speed: Optional[float]

    gare_depart: Optional[GareBase]
    gare_arrivee: Optional[GareBase]
    train: Optional[TrainBase]
    operateur: Optional[OperateurBase]
    route: Optional[RouteBase]

    class Config:
        from_attributes = True



#Stats
class StatEmissions(BaseModel):
    operateur: Optional[str]
    route: Optional[str]
    total_emissions_co2: Optional[float]
    moyenne_emissions_co2: Optional[float]
    nb_trajets: int


class StatFrequentation(BaseModel):
    gare: Optional[str]
    ville: Optional[str]
    total_passengers: Optional[float]
    nb_trajets: int


class StatPerformance(BaseModel):
    operateur: Optional[str]
    vitesse_moyenne: Optional[float]
    duree_moyenne_minutes: Optional[float]
    distance_moyenne_km: Optional[float]
    nb_trajets: int



#Pagination
class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: list
