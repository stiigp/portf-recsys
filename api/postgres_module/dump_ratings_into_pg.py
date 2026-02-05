from postgres_module.entities.rating import Base
from postgres_module.db import engine
from sqlalchemy import text
import io

import pandas as pd

async def dump_ratings_on_startup():
    Base.metadata.create_all(bind=engine)
    
    csv_path = "dataset/ratings_clean.csv"
    chunk_size = 500000

    with engine.connect() as conn:
        has_data = conn.execute(text("SELECT 1 FROM ratings LIMIT 1")).fetchone()
        if has_data:
            print("DB already populated")
            return
        
        print("Starting dump via COPY")
        
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size):

            del chunk['timestamp']

            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            raw_conn = conn.connection.cursor()
            raw_conn.copy_from(buffer, 'ratings', sep=',', columns=['userId', 'movieId', 'rating'])
            
            conn.commit()
            print(f"Inserted more {chunk_size} registers...")

    print("Dump finished successfully!")
    