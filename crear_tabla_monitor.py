import os
import psycopg2

DB = os.getenv("MONITOR_DATABASE_URL") or os.getenv("DATABASE_URL")
if not DB:
    raise RuntimeError("MONITOR_DATABASE_URL (o DATABASE_URL) no está definida en el entorno")
DB = DB.replace("postgres://", "postgresql://", 1)

conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS monitor_iri (
        id SERIAL PRIMARY KEY,
        organismo TEXT,
        area TEXT,
        riesgo_financiero FLOAT,
        riesgo_contratacion FLOAT,
        riesgo_operativo FLOAT,
        riesgo_datos FLOAT,
        iri_score FLOAT,
        estado TEXT,
        fuente TEXT,
        fecha_datos DATE DEFAULT CURRENT_DATE,
        updated_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()

cur.execute("SELECT COUNT(*) FROM monitor_iri")
print("✅ Tabla monitor_iri creada. Registros:", cur.fetchone()[0])
conn.close()
