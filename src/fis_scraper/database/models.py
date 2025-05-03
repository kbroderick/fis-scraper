from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Enum, CHAR
from sqlalchemy.orm import relationship
from .connection import Base
import enum

class Discipline(enum.Enum):
    SL = "Slalom"
    GS = "Giant Slalom"
    SG = "Super-G"
    DH = "Downhill"

class Gender(enum.Enum):
    M = "Male"
    F = "Female"

class Athlete(Base):
    __tablename__ = 'athletes'

    id = Column(Integer, primary_key=True)
    fis_id = Column(Integer, unique=True, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    nation_code = Column(CHAR(3), nullable=False)  # 3-letter country code
    gender = Column(Enum(Gender), nullable=False)
    birth_date = Column(Date)
    birth_year = Column(Integer)  # For quick filtering/analysis
    ski_club = Column(String)
    national_code = Column(String)  # National federation code
    
    results = relationship("RaceResult", back_populates="athlete")
    points = relationship("AthletePoints", back_populates="athlete")

class RaceResult(Base):
    __tablename__ = 'race_results'

    id = Column(Integer, primary_key=True)
    athlete_id = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    race_date = Column(Date, nullable=False)
    discipline = Column(Enum(Discipline), nullable=False)
    points = Column(Float)
    rank = Column(Integer)
    race_name = Column(String)
    location = Column(String)
    
    athlete = relationship("Athlete", back_populates="results")

class PointsList(Base):
    __tablename__ = 'points_lists'

    id = Column(Integer, primary_key=True)
    publication_date = Column(Date, nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date, nullable=False)
    season = Column(String, nullable=False)
    
    athlete_points = relationship("AthletePoints", back_populates="points_list")

class AthletePoints(Base):
    __tablename__ = 'athlete_points'

    id = Column(Integer, primary_key=True)
    athlete_id = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    points_list_id = Column(Integer, ForeignKey('points_lists.id'), nullable=False)
    sl_points = Column(Float)
    gs_points = Column(Float)
    sg_points = Column(Float)
    dh_points = Column(Float)
    sl_rank = Column(Integer)  # World rank position in Slalom
    gs_rank = Column(Integer)  # World rank position in Giant Slalom
    sg_rank = Column(Integer)  # World rank position in Super-G
    dh_rank = Column(Integer)  # World rank position in Downhill
    
    athlete = relationship("Athlete", back_populates="points")
    points_list = relationship("PointsList", back_populates="athlete_points") 