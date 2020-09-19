# Press Shift+F10 to execute it or replace it with your code.

import tablib
import models


def load():
    for model, filename in models.tables:

        print(f"Importing {filename}..")
        d = tablib.Dataset().load(open(f"../gtfs/{filename}").read())
        prepare = getattr(model, 'prepare', lambda o: o)
        models.session.bulk_save_objects([model(**prepare(dict(zip(d.headers, i)))) for i in d])
        print(f"Importing {filename}: Done.")


if __name__ == '__main__':
    models.create_tables()
    load()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
