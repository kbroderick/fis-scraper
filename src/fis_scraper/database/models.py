from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Enum, CHAR
from sqlalchemy.orm import relationship
from .connection import Base
import enum
from typing import Optional, List

class Discipline(enum.Enum):
    """Enumeration of alpine skiing disciplines."""
    SL = "Slalom"
    GS = "Giant Slalom"
    SG = "Super-G"
    DH = "Downhill"

class Gender(enum.Enum):
    """Enumeration of athlete genders."""
    M = "Male"
    F = "Female"

class Athlete(Base):
    """Database model representing an alpine skiing athlete.
    
    Attributes:
        id (int): Primary key
        fis_id (int): FIS (International Ski Federation) ID
        name (str): Athlete's full name
        country (str): Athlete's country of origin
        nation_code (str): 3-letter country code
        gender (Gender): Athlete's gender
        birth_date (Optional[date]): Athlete's date of birth
        birth_year (Optional[int]): Athlete's birth year for quick filtering
        ski_club (Optional[str]): Athlete's ski club
        national_code (Optional[str]): National federation code
        results (List[RaceResult]): List of race results for this athlete
        points (List[AthletePoints]): List of points records for this athlete
    """
    __tablename__ = 'athletes'

    id: int = Column(Integer, primary_key=True)
    fis_id: int = Column(Integer, unique=True, nullable=False)
    name: str = Column(String, nullable=False)
    country: str = Column(String, nullable=False)
    nation_code: str = Column(CHAR(3), nullable=False)  # 3-letter country code
    gender: Gender = Column(Enum(Gender), nullable=False)
    birth_date: Optional[Date] = Column(Date)
    birth_year: Optional[int] = Column(Integer)  # For quick filtering/analysis
    ski_club: Optional[str] = Column(String)
    national_code: Optional[str] = Column(String)  # National federation code
    
    results: List["RaceResult"] = relationship("RaceResult", back_populates="athlete")
    points: List["AthletePoints"] = relationship("AthletePoints", back_populates="athlete")

class RaceResult(Base):
    """Database model representing a single race result.
    
    Attributes:
        id (int): Primary key
        athlete_id (int): Foreign key to Athlete
        race_date (date): Date of the race
        discipline (Discipline): Race discipline
        points (Optional[float]): FIS points earned
        rank (Optional[int]): Race finish position
        race_name (Optional[str]): Name of the race
        location (Optional[str]): Race location
        athlete (Athlete): Related Athlete object
    """
    __tablename__ = 'race_results'

    id: int = Column(Integer, primary_key=True)
    athlete_id: int = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    race_date: Date = Column(Date, nullable=False)
    discipline: Discipline = Column(Enum(Discipline), nullable=False)
    points: Optional[float] = Column(Float)
    rank: Optional[int] = Column(Integer)
    race_name: Optional[str] = Column(String)
    location: Optional[str] = Column(String)
    
    athlete: Athlete = relationship("Athlete", back_populates="results")

class PointsList(Base):
    """Database model representing a FIS points list publication.
    
    Attributes:
        id (int): Primary key
        publication_date (date): Date when points list was published
        valid_from (date): Start date of validity period
        valid_to (date): End date of validity period
        season (str): Season identifier (e.g., "2023/24")
        athlete_points (List[AthletePoints]): List of athlete points records
    """
    __tablename__ = 'points_lists'

    id: int = Column(Integer, primary_key=True)
    publication_date: Date = Column(Date, nullable=False)
    valid_from: Date = Column(Date, nullable=False)
    valid_to: Date = Column(Date, nullable=False)
    season: str = Column(String, nullable=False)
    
    athlete_points: List["AthletePoints"] = relationship("AthletePoints", back_populates="points_list")

class AthletePoints(Base):
    """Database model representing an athlete's FIS points and rankings.
    
    Attributes:
        id (int): Primary key
        athlete_id (int): Foreign key to Athlete
        points_list_id (int): Foreign key to PointsList
        sl_points (Optional[float]): Slalom points
        gs_points (Optional[float]): Giant Slalom points
        sg_points (Optional[float]): Super-G points
        dh_points (Optional[float]): Downhill points
        sl_rank (Optional[int]): World rank position in Slalom
        gs_rank (Optional[int]): World rank position in Giant Slalom
        sg_rank (Optional[int]): World rank position in Super-G
        dh_rank (Optional[int]): World rank position in Downhill
        athlete (Athlete): Related Athlete object
        points_list (PointsList): Related PointsList object
    """
    __tablename__ = 'athlete_points'

    id: int = Column(Integer, primary_key=True)
    athlete_id: int = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    points_list_id: int = Column(Integer, ForeignKey('points_lists.id'), nullable=False)
    sl_points: Optional[float] = Column(Float)
    gs_points: Optional[float] = Column(Float)
    sg_points: Optional[float] = Column(Float)
    dh_points: Optional[float] = Column(Float)
    sl_rank: Optional[int] = Column(Integer)  # World rank position in Slalom
    gs_rank: Optional[int] = Column(Integer)  # World rank position in Giant Slalom
    sg_rank: Optional[int] = Column(Integer)  # World rank position in Super-G
    dh_rank: Optional[int] = Column(Integer)  # World rank position in Downhill
    
    athlete: Athlete = relationship("Athlete", back_populates="points")
    points_list: PointsList = relationship("PointsList", back_populates="athlete_points") 