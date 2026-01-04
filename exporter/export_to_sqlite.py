import os
import gzip
import shutil
from sqlalchemy import create_engine, text, inspect
import pandas as pd

PG_USER = os.environ["POSTGRES_USER"]
PG_PWD = os.environ["POSTGRES_PASSWORD"]
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres-frontend")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "vital_signs_web")

OUT_DIR = os.environ.get("OUT_DIR", "/out")
SQLITE_BASENAME = os.environ.get("SQLITE_BASENAME", "vital_signs_web.sqlite")

sqlite_path = os.path.join(OUT_DIR, SQLITE_BASENAME)
gzip_path = sqlite_path + ".gz"

pg_url = f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
sqlite_url = f"sqlite:///{sqlite_path}"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    pg = create_engine(pg_url, pool_pre_ping=True)
    sq = create_engine(sqlite_url)

    insp = inspect(pg)
    schemas = [s for s in insp.get_schema_names() if s not in ("pg_catalog", "information_schema")]
   
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)


    with pg.connect() as conn:
        for schema in schemas:
            for table in insp.get_table_names(schema=schema):
        
                query = text(f'SELECT * FROM "{schema}"."{table}"') if schema != "public" else text(f'SELECT * FROM "{table}"')

                first = True
                for chunk in pd.read_sql(query, conn, chunksize=50000):
                    chunk.to_sql(
                        name=table if schema == "public" else f"{schema}__{table}",
                        con=sq,
                        if_exists="replace" if first else "append",
                        index=False,
                    )
                    first = False

    # Ottimizzazione SQLite
    with sq.connect() as c:
        c.execute(text("PRAGMA journal_mode=OFF;"))
        c.execute(text("PRAGMA synchronous=OFF;"))
        c.execute(text("VACUUM;"))

    # Compressione gzip
    with open(sqlite_path, "rb") as f_in, gzip.open(gzip_path, "wb", compresslevel=9) as f_out:
        shutil.copyfileobj(f_in, f_out)

    print(f"SQLite database exported and compressed to {gzip_path}")


if __name__ == "__main__":
    main()
