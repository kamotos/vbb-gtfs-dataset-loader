from collections import namedtuple
from typing import Tuple

import sqlalchemy as sa
from crate.client.sqlalchemy import types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = sa.create_engine('crate://')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base(bind=engine)
TableImport = namedtuple('TableImport', ("table", "filename"))


class Agency(Base):
    __tablename__ = 'agencies'

    agency_id = sa.Column(sa.Integer, primary_key=True)
    agency_name = sa.Column(sa.String)
    agency_url = sa.Column(sa.String)
    agency_timezone = sa.Column(sa.String)
    agency_lang = sa.Column(sa.String(2))
    agency_phone = sa.Column(sa.String, nullable=True)


class Route(Base):
    __tablename__ = 'routes'

    route_id = sa.Column(sa.String, primary_key=True)
    agency_id = sa.Column(sa.Integer)
    route_short_name = sa.Column(sa.String)
    route_long_name = sa.Column(sa.String)
    route_type = sa.Column(sa.String)
    route_color = sa.Column(sa.String)
    route_text_color = sa.Column(sa.String)
    route_desc = sa.Column(sa.String)


class Trip(Base):
    __tablename__ = 'trips'

    route_id = sa.Column(sa.String)
    service_id = sa.Column(sa.Integer)
    trip_id = sa.Column(sa.Integer, primary_key=True)
    trip_headsign = sa.Column(sa.String)
    trip_short_name = sa.Column(sa.String)
    direction_id = sa.Column(sa.Integer, nullable=True)
    block_id = sa.Column(sa.String, nullable=True)
    shape_id = sa.Column(sa.Integer, nullable=True)
    wheelchair_accessible = sa.Column(sa.Boolean, default=0)
    bikes_allowed = sa.Column(sa.Boolean, default=0)

    @classmethod
    def prepare(cls, value: dict) -> dict:
        value['wheelchair_accessible'] = bool(value['wheelchair_accessible'])
        value['bikes_allowed'] = bool(value['bikes_allowed'])
        return value


class Shape(Base):
    __tablename__ = 'shapes'

    shape_id = sa.Column(sa.Integer, primary_key=True)
    shape_pt = sa.Column(types.Geopoint)
    shape_pt_sequence = sa.Column(sa.Integer, primary_key=True)

    @classmethod
    def prepare(cls, value: dict) -> dict:
        value['shape_pt'] = (value.pop('shape_pt_lon'), value.pop('shape_pt_lat'))
        return value


tables: Tuple[TableImport] = (
    TableImport(Agency, "agency.txt"),
    TableImport(Route, "routes.txt"),
    TableImport(Shape, "shapes.txt"),
    TableImport(Trip, "trips.txt"),
)


def create_tables():
    Base.metadata.create_all(engine, tables=[table.table.__table__ for table in tables])
