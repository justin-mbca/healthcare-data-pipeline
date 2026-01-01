-- Example: FHIR Clinical Data Schema (PostgreSQL)
CREATE TABLE patient (
    id SERIAL PRIMARY KEY,
    fhir_id VARCHAR(64),
    name TEXT,
    birth_date DATE
);
