from deps import get_db_context
from postgres_module.entities.rating import Base, Rating
from postgres_module.db import engine

import pandas as pd

async def dump_ratings_on_startup():
    # isso aqui cria as tabelas vinculadas à classe Base se elas não existirem
    Base.metadata.create_all(bind=engine)
    with get_db_context() as con:
        has_data = con.query(Rating).first() is not None
        if not has_data:
            ratings = pd.read_csv("dataset/ratings_clean.csv")

            batch = []
            for row in ratings.itertuples():
                batch.append(
                    Rating(
                        userId=int(row.userId),
                        movieId=int(row.movieId),
                        rating=float(row.rating)
                    )
                )

                if len(batch) >= 1000:
                    con.bulk_save_objects(batch)
                    con.commit()
                    batch.clear()
            
            if len(batch) > 0:
                con.bulk_save_objects(batch)
                con.commit()