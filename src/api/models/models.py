from sqlalchemy import Column, Integer, String, Float, Boolean, Date, ForeignKey, Time, Interval
from sqlalchemy.orm import relationship
from src.api.db.database import Base

SCHEMA = "tpre612_data_warehouse"


class DimTrain(Base):
    __tablename__ = "dim_train"
    __table_args__ = {"schema": SCHEMA}

    trip_id = Column(String, primary_key=True)
    route_id = Column(Integer)
    trip_headsign = Column(String)
    trip_origin = Column(String)
    destination_arrival_time = Column(Time)
    duration_value = Column("duration", Interval)
    distance = Column(Float)
    is_night_train = Column(Boolean, default=False)

    trajets = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.train_id", back_populates="train")
    route = relationship("DimRoute", foreign_keys=[route_id], primaryjoin="DimTrain.route_id == DimRoute.route_id", viewonly=True)

    @property
    def origin(self):
        return self.trip_origin

    @property
    def destination(self):
        return self.route.destination if self.route else None

    @property
    def duration(self):
        return str(self.duration_value) if self.duration_value is not None else None


class DimRoute(Base):
    __tablename__ = "dim_route"
    __table_args__ = {"schema": SCHEMA}

    route_id = Column(Integer, primary_key=True)
    agency_id = Column(String)
    route_long_name = Column(String)
    origin = Column(String)
    destination = Column(String)
    countries = Column(String)

    trajets = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.route_id", back_populates="route")


class DimOperateur(Base):
    __tablename__ = "dim_operateur"
    __table_args__ = {"schema": SCHEMA}

    agency_id = Column(String, primary_key=True)
    agency_name = Column(String)
    agency_url = Column(String)
    agency_timezone = Column(String)
    agency_lang = Column(String)

    trajets = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.operator_id", back_populates="operateur")

    @property
    def agency_country(self):
        return None


class DimGare(Base):
    __tablename__ = "dim_gare"
    __table_args__ = {"schema": SCHEMA}

    gare_id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)
    country = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    is_main_station = Column(Boolean)

    trajets_depart = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.gare_depart_id", back_populates="gare_depart")
    trajets_arrivee = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.gare_arrivee_id", back_populates="gare_arrivee")


class DimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": SCHEMA}

    date_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    monday = Column(Boolean)
    tuesday = Column(Boolean)
    wednesday = Column(Boolean)
    thursday = Column(Boolean)
    friday = Column(Boolean)
    saturday = Column(Boolean)
    sunday = Column(Boolean)

    trajets = relationship("FactTrajetTrain", foreign_keys="FactTrajetTrain.date_id", back_populates="date")


class DimEnergie(Base):
    __tablename__ = "dim_energie"
    __table_args__ = {"schema": SCHEMA}

    energy_id = Column(Integer, primary_key=True)
    geo = Column(String)
    vehicle = Column(String)
    energy_type = Column(String)
    year = Column(Integer)
    energy_value = Column(Float)
    # No relationship to FactTrajetTrain — no FK exists


class FactTrajetTrain(Base):
    __tablename__ = "fact_trajet_train"
    __table_args__ = {"schema": SCHEMA}

    fact_id = Column(Integer, primary_key=True, index=True)
    train_id = Column(String, ForeignKey(f"{SCHEMA}.dim_train.trip_id"))
    route_id = Column(Integer, ForeignKey(f"{SCHEMA}.dim_route.route_id"))
    operator_id = Column(String, ForeignKey(f"{SCHEMA}.dim_operateur.agency_id"))
    gare_depart_id = Column(Integer, ForeignKey(f"{SCHEMA}.dim_gare.gare_id"))
    gare_arrivee_id = Column(Integer, ForeignKey(f"{SCHEMA}.dim_gare.gare_id"))
    date_id = Column(Integer, ForeignKey(f"{SCHEMA}.dim_date.date_id"))
    distance_km = Column(Float)
    duree_minutes = Column(Float)
    emissions_co2 = Column(Float)
    passengers = Column(Float)
    average_speed = Column(Float)
    is_night_train = Column(Boolean, default=False)

    train = relationship("DimTrain", foreign_keys=[train_id], primaryjoin="FactTrajetTrain.train_id == DimTrain.trip_id", back_populates="trajets")
    route = relationship("DimRoute", foreign_keys=[route_id], primaryjoin="FactTrajetTrain.route_id == DimRoute.route_id", back_populates="trajets")
    operateur = relationship("DimOperateur", foreign_keys=[operator_id], primaryjoin="FactTrajetTrain.operator_id == DimOperateur.agency_id", back_populates="trajets")
    gare_depart = relationship("DimGare", foreign_keys=[gare_depart_id], back_populates="trajets_depart")
    gare_arrivee = relationship("DimGare", foreign_keys=[gare_arrivee_id], back_populates="trajets_arrivee")
    date = relationship("DimDate", foreign_keys=[date_id], back_populates="trajets")
    # energie relationship removed — no FK between fact_trajet_train and dim_energie