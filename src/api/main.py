from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import trajets, gares, trains, operateurs, routes, stats

app = FastAPI(
    title="API Dessertes Ferroviaires",
    description="""
API REST – Dessertes Ferroviaires

Cette API expose les données ferroviaires collectées et transformées dans le data warehouse.

Fonctionnalités
- Consultation des trajets avec filtres multicritères
- Recherche de gares par ville, pays
- Statistiques : émissions CO2, fréquentation, performance
- Compatible Grafana (JSON datasource)
    """,
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # À restreindre plus tard
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(trajets.router, prefix="/trajets", tags=["Trajets"])
app.include_router(gares.router, prefix="/gares", tags=["Gares"])
app.include_router(trains.router, prefix="/trains", tags=["Trains"])
app.include_router(operateurs.router, prefix="/operateurs", tags=["Opérateurs"])
app.include_router(routes.router, prefix="/routes", tags=["Routes"])
app.include_router(stats.router, prefix="/stats", tags=["Statistiques"])


@app.get("/", tags=["Santé"])
def root():
    return {
        "status": "ok",
        "message": "API Dessertes Ferroviaires v1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
    }


@app.get("/health", tags=["Santé"])
def health():
    return {"status": "healthy"}
