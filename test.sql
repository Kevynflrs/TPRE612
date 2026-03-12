-- Row counts per table
SELECT 'dim_operateur'    AS tbl, COUNT(*) FROM "tpre612_data_warehouse"."dim_operateur"
UNION ALL
SELECT 'dim_route',                COUNT(*) FROM "tpre612_data_warehouse"."dim_route"
UNION ALL
SELECT 'dim_train',                COUNT(*) FROM "tpre612_data_warehouse"."dim_train"
UNION ALL
SELECT 'dim_gare',                 COUNT(*) FROM "tpre612_data_warehouse"."dim_gare"
UNION ALL
SELECT 'dim_date',                 COUNT(*) FROM "tpre612_data_warehouse"."dim_date"
UNION ALL
SELECT 'dim_energie',              COUNT(*) FROM "tpre612_data_warehouse"."dim_energie"
UNION ALL
SELECT 'fact_trajet_train',        COUNT(*) FROM "tpre612_data_warehouse"."fact_trajet_train";

-- Quick sanity check on the fact table
SELECT o.agency_name, r.origin, r.destination,
       AVG(f.distance_km) AS avg_distance,
       AVG(f.emissions_co2) AS avg_co2
FROM "tpre612_data_warehouse"."fact_trajet_train" f
JOIN "tpre612_data_warehouse"."dim_operateur" o ON o.agency_id = f.operator_id
JOIN "tpre612_data_warehouse"."dim_route"     r ON r.route_id  = f.route_id
GROUP BY o.agency_name, r.origin, r.destination
ORDER BY avg_distance DESC
LIMIT 1100;