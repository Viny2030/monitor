import psycopg2

DB = "postgresql://postgres:zKiRuniKBLpYVjsRmgMKCJoIsqeygfUi@tramway.proxy.rlwy.net:32055/railway"

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