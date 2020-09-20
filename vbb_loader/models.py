"""
This module contains all the tables definitions and is used for :
- Tables creation
- Data prepartion

Each class can optionally have the following:
- prepare_df: classmethod that takes an returns a pandas.DataFrame as an argument
                It is meant to do necessary transformations before insrting in the DB
- dtype: dict that helps pandas define the types of columns.
"""
import time
from collections import namedtuple

import funcy as funcy
import pandas as pd
import requests
import sqlalchemy as sa
from crate import client
from pandas import DataFrame
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


connection = client.connect("http://db:4200/")
engine = sa.create_engine('crate://db')
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


class Shape(Base):
    __tablename__ = 'shapes'

    shape_id = sa.Column(sa.Integer, primary_key=True)
    shape_pt_lat = sa.Column(sa.DECIMAL)
    shape_pt_lon = sa.Column(sa.DECIMAL)
    shape_pt_sequence = sa.Column(sa.Integer, primary_key=True)


class Calendar(Base):
    __tablename__ = 'calendar'

    service_id = sa.Column(sa.Integer, primary_key=True)
    monday = sa.Column(sa.SmallInteger)
    tuesday = sa.Column(sa.SmallInteger)
    wednesday = sa.Column(sa.SmallInteger)
    thursday = sa.Column(sa.SmallInteger)
    friday = sa.Column(sa.SmallInteger)
    saturday = sa.Column(sa.SmallInteger)
    sunday = sa.Column(sa.SmallInteger)
    start_date = sa.Column(sa.Date)
    end_date = sa.Column(sa.Date)

    @classmethod
    def prepare_df(cls, df: DataFrame) -> DataFrame:
        df['start_date'] = pd.to_datetime(df['start_date'], format="%Y%m%d")
        df['end_date'] = pd.to_datetime(df['end_date'], format="%Y%m%d")
        return df


class CalendarDate(Base):
    __tablename__ = 'calendar_dates'

    service_id = sa.Column(sa.Integer, primary_key=True)
    date = sa.Column(sa.Date, primary_key=True)
    exception_type = sa.Column(sa.SmallInteger, primary_key=True)


class StopTime(Base):
    __tablename__ = 'stop_times'

    dtype = {
        "stop_id": 'object',
        'stop_headsign': 'object'
    }

    trip_id = sa.Column(sa.Integer, primary_key=True)
    arrival_time = sa.Column(sa.String)
    departure_time = sa.Column(sa.String, primary_key=True)
    stop_id = sa.Column(sa.String, primary_key=True)
    stop_sequence = sa.Column(sa.Integer, primary_key=True)
    pickup_type = sa.Column(sa.SmallInteger)
    drop_off_type = sa.Column(sa.SmallInteger)
    stop_headsign = sa.Column(sa.String)


class Stop(Base):
    __tablename__ = 'stops'

    stop_id = sa.Column(sa.String, primary_key=True)
    stop_code = sa.Column(sa.String)
    stop_name = sa.Column(sa.String)
    stop_desc = sa.Column(sa.String)
    stop_lat = sa.Column(sa.DECIMAL)
    stop_lon = sa.Column(sa.DECIMAL)
    location_type = sa.Column(sa.SmallInteger)
    parent_station = sa.Column(sa.String)
    wheelchair_boarding = sa.Column(sa.SmallInteger, default=0)
    platform_code = sa.Column(sa.String)
    zone_id = sa.Column(sa.String)

    @classmethod
    def prepare_df(cls, df: DataFrame) -> DataFrame:
        df.wheelchair_boarding.replace("", "0", inplace=True)
        return df


tables = (
    TableImport(Agency, "agency.txt"),
    TableImport(CalendarDate, "calendar_dates.txt"),
    TableImport(Calendar, "calendar.txt"),
    TableImport(Route, "routes.txt"),
    TableImport(Shape, "shapes.txt"),
    TableImport(Stop, "stops.txt"),
    TableImport(StopTime, "stop_times.txt"),
)


@funcy.silent
def is_db_ready() -> bool:
    return requests.head("http://db:4200").ok


@funcy.retry(10)
def create_tables():
    while not is_db_ready():
        time.sleep(1)
    Base.metadata.create_all(engine, tables=[table.table.__table__ for table in tables])
