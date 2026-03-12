from sqlalchemy import Column, Integer, String, Float, Boolean, Date, ForeignKey
from sqlalchemy.orm import relationship
from db.database import Base


SCHEMA = "tpre612_data_warehouse"


class DimTrain(Base):
    __tablename__ = "dim_train"
    __table_args__ = {"schema": SCHEMA}

    trip_id = Column(String, primary_key=True)
    route_id = Column(String)
    trip_headsign = Column(String)
    origin = Column(String)
    destination = Column(String)
    duration = Column(String)
    distance = Column(Float)

    trajets = relationship("FactTrajetTrain", back_populates="train")


class DimRoute(Base):
    __tablename__ = "dim_route"
    __table_args__ = {"schema": SCHEMA}

    route_id = Column(Integer, primary_key=True)
    agency_id = Column(String)
    route_long_name = Column(String)
    origin = Column(String)
    destination = Column(String)
    countries = Column(String)

    trajets = relationship("FactTrajetTrain", back_populates="route")


class DimOperateur(Base):
    __tablename__ = "dim_operateur"
    __table_args__ = {"schema": SCHEMA}

    agency_id = Column(String, primary_key=True)
    agency_name = Column(String)
    agency_url = Column(String)
    agency_country = Column(String)

    trajets = relationship("FactTrajetTrain", back_populates="operateur")


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

    trajets_depart = relationship(
        "FactTrajetTrain",
        foreign_keys="FactTrajetTrain.gare_depart_id",
        back_populates="gare_depart",
    )
    trajets_arrivee = relationship(
        "FactTrajetTrain",
        foreign_keys="FactTrajetTrain.gare_arrivee_id",
        back_populates="gare_arrivee",
    )


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

    trajets = relationship("FactTrajetTrain", back_populates="date")


class DimEnergie(Base):
    __tablename__ = "dim_energie"
    __table_args__ = {"schema": SCHEMA}

    geo = Column(String, primary_key=True)
    energy_type = Column(String)
    vehicle = Column(String)
    year = Column(Integer)
    energy_consumption = Column(Float)

    trajets = relationship("FactTrajetTrain", back_populates="energie")


class FactTrajetTrain(Base):
    __tablename__ = "fact_trajet_train"
    __table_args__ = {"schema": SCHEMA}

    fact_id = Column(Integer, primary_key=True, index=True)
    train_id = Column(String, ForeignKey("dim_train.trip_id"))
    route_id = Column(Integer, ForeignKey("dim_route.route_id"))
    operator_id = Column(String, ForeignKey("dim_operateur.agency_id"))
    gare_depart_id = Column(Integer, ForeignKey("dim_gare.gare_id"))
    gare_arrivee_id = Column(Integer, ForeignKey("dim_gare.gare_id"))
    date_id = Column(Integer, ForeignKey("dim_date.date_id"))
    distance_km = Column(Float)
    duree_minutes = Column(Float)
    emissions_co2 = Column(Float)
    passengers = Column(Float)
    average_speed = Column(Float)

    # Relations
    train = relationship("DimTrain", back_populates="trajets")
    route = relationship("DimRoute", back_populates="trajets")
    operateur = relationship("DimOperateur", back_populates="trajets")
    gare_depart = relationship(
        "DimGare", foreign_keys=[gare_depart_id], back_populates="trajets_depart"
    )
    gare_arrivee = relationship(
        "DimGare", foreign_keys=[gare_arrivee_id], back_populates="trajets_arrivee"
    )
    date = relationship("DimDate", back_populates="trajets")
    energie = relationship("DimEnergie", back_populates="trajets")
