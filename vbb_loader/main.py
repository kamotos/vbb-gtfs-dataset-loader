# Press Shift+F10 to execute it or replace it with your code.
import os
import time
from datetime import timedelta

import pandas as pd

import models

ROOT_GTFS_PATH = os.getenv("ROOT_GTFS_PATH", "../gtfs/")
cursor = models.connection.cursor()


def import_file(table_import: models.TableImport):
    model, filename = table_import
    # Get columns types if any
    dtype = getattr(model, 'dtype', {})
    print(f"Importing {filename}..")
    start = time.time()

    for chunk in pd.read_csv(f"{ROOT_GTFS_PATH}{filename}", chunksize=100000, na_filter=False, dtype=dtype):
        columns = ", ".join(tuple(chunk))
        # We should have as many question marks as number of columns
        question_marks = ", ".join(["?"] * len(tuple(chunk)))

        # Should the model require some data preparation, do it
        if hasattr(model, 'prepare_df'):
            chunk = model.prepare_df(chunk)

        # Bulk insert
        cursor.executemany(
            f"""
                INSERT INTO {model.__tablename__} ({columns})
                    VALUES ({question_marks})
                """,
            list(chunk.itertuples(index=False, name=None))
        )

    took = timedelta(seconds=round(time.time() - start))
    print(f"Importing {filename}: Done. Took {took}.")


def load():
    for table_import in models.tables:
        import_file(table_import)


if __name__ == '__main__':
    models.create_tables()
    load()
