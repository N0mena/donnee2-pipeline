-- ============================================================
-- Schéma en étoile (Star Schema) pour l'entrepôt de données
-- sur la qualité de l'air.
-- Base de données : PostgreSQL
-- ============================================================

-- Suppression des objets existants (ordre inverse des dépendances)
DROP TABLE IF EXISTS fait_qualite_air CASCADE;
DROP TABLE IF EXISTS dim_heure CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_ville CASCADE;

-- ============================================================
-- TABLE DE DIMENSION : dim_ville
-- Stocke les informations géographiques de chaque ville.
-- ============================================================
CREATE TABLE dim_ville (
    ville_id    SERIAL PRIMARY KEY,
    ville       VARCHAR(100) NOT NULL,
    abbr        VARCHAR(5)   NOT NULL UNIQUE,
    pays        VARCHAR(5)   NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL
);

-- ============================================================
-- TABLE DE DIMENSION : dim_date
-- Stocke les composantes calendaires de chaque mesure.
-- ============================================================
CREATE TABLE dim_date (
    date_id       SERIAL PRIMARY KEY,
    date_utc      DATE NOT NULL UNIQUE,
    jour          SMALLINT NOT NULL,
    mois          SMALLINT NOT NULL,
    trimestre     SMALLINT NOT NULL,
    annee         SMALLINT NOT NULL,
    jour_semaine  SMALLINT NOT NULL,  -- 0=dimanche .. 6=samedi
    nom_jour      VARCHAR(10) NOT NULL,
    nom_mois      VARCHAR(10) NOT NULL
);

-- ============================================================
-- TABLE DE DIMENSION : dim_heure
-- Stocke les composantes horaires de chaque mesure.
-- ============================================================
CREATE TABLE dim_heure (
    heure_id          SERIAL PRIMARY KEY,
    heure_utc         TIME NOT NULL UNIQUE,
    heure             SMALLINT NOT NULL,
    minute            SMALLINT NOT NULL,
    periode_journee   VARCHAR(20) NOT NULL
    -- periode_journee : 'nuit' (00-05), 'matin' (06-11),
    --                   'apres-midi' (12-17), 'soiree' (18-23)
);

-- ============================================================
-- TABLE DE FAIT : fait_qualite_air
-- Mesures horaires de la qualité de l'air par ville.
-- Chaque ligne = une mesure (ville x date x heure).
-- ============================================================
CREATE TABLE fait_qualite_air (
    fait_id     SERIAL PRIMARY KEY,
    ville_id    INT NOT NULL REFERENCES dim_ville(ville_id),
    date_id     INT NOT NULL REFERENCES dim_date(date_id),
    heure_id    INT NOT NULL REFERENCES dim_heure(heure_id),
    aqi         SMALLINT NOT NULL,
    co          DOUBLE PRECISION,
    no          DOUBLE PRECISION,
    no2         DOUBLE PRECISION,
    o3          DOUBLE PRECISION,
    so2         DOUBLE PRECISION,
    pm2_5       DOUBLE PRECISION,
    pm10        DOUBLE PRECISION,
    nh3         DOUBLE PRECISION,
    UNIQUE (ville_id, date_id, heure_id)
);

-- ============================================================
-- INDEX pour optimiser les requêtes analytiques
-- ============================================================
CREATE INDEX idx_fait_ville   ON fait_qualite_air(ville_id);
CREATE INDEX idx_fait_date    ON fait_qualite_air(date_id);
CREATE INDEX idx_fait_heure   ON fait_qualite_air(heure_id);
CREATE INDEX idx_fait_aqi     ON fait_qualite_air(aqi);

-- ============================================================
-- VUES utiles pour l'exploration rapide
-- ============================================================
CREATE OR REPLACE VIEW vue_qualite_air_complete AS
SELECT
    f.fait_id,
    v.ville,
    v.abbr,
    v.pays,
    v.latitude,
    v.longitude,
    d.date_utc,
    d.jour,
    d.mois,
    d.trimestre,
    d.annee,
    d.nom_jour,
    d.nom_mois,
    h.heure_utc,
    h.heure,
    h.periode_journee,
    f.aqi,
    f.co,
    f.no,
    f.no2,
    f.o3,
    f.so2,
    f.pm2_5,
    f.pm10,
    f.nh3
FROM fait_qualite_air f
JOIN dim_ville  v ON f.ville_id  = v.ville_id
JOIN dim_date   d ON f.date_id   = d.date_id
JOIN dim_heure  h ON f.heure_id  = h.heure_id;
