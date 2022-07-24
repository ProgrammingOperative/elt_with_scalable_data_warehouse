from decimal import Decimal
import sqlalchemy
import uuid
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Date


class Connection(object):

    """
    This class connects Python to the PostgreSQL database using SQLAlchemy.
    SQLAlchemy is the Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL.
    """
    def __init__(self, db_connection):
        """
        The constructor for the Connection class. 
        It creates an SQLAlchemy database Engine to map Python models to PPostgreSQL database
        Params:
            db_connection: str
                Database connection string
        """
        engine = create_engine(db_connection)
        self.engine = engine

    def get_session(self):
        """
        Gets the PostgreSQL database session
        """
        Session = sessionmaker(bind=self.engine)
        return Session()

    def get_engine(self):
        """
        Returns the active database engine
        """
        return self.engine

Base = declarative_base()


def init_db(db_connection):
    engine = create_engine(db_connection)
    Base.metadata.create_all(bind=engine)


class TrafficFlow(Base):
    """
    Describes the traffic_flow table in the database
    """
    __tablename__ = 'traffic_flow'
    
    id = Column(Integer, primary_key=True)
    track_id = Column(Integer)
    vehicle_types = Column(String)
    traveled_d = Column(Float)
    avg_speed = Column(Float)
    trajectory = Column(String)

    def __init__(self, id, track_id, vehicle_types, traveled_d, avg_speed, trajectory):
        """
        Constructor to the TrafficFlow class
        It initializes all the properties of the class
        """
        self.id = id
        self.track_id = track_id
        self.vehicle_types = vehicle_types
        self.traveled_d = traveled_d
        self.avg_speed = avg_speed
        self.trajectory = trajectory