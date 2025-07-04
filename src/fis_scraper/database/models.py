from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Enum, CHAR
from sqlalchemy.orm import relationship, Mapped
from .connection import Base
import enum
from typing import Optional, List

class Discipline(enum.Enum):
    """Enumeration of alpine skiing disciplines."""
    SL = "Slalom"
    GS = "Giant Slalom"
    SG = "Super-G"
    DH = "Downhill"
    AC = "Alpine Combined"

class Gender(enum.Enum):
    """Enumeration of athlete genders."""
    M = "Male"
    F = "Female"

class Athlete(Base):
    """Database model representing an alpine skiing athlete.
    
    Attributes:
        id (int): Primary key
        fis_id (int): FIS (International Ski Federation) ID
        fis_db_id (Optional[int]): FIS DB ID (e.g. 123456)
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

    id: Mapped[int] = Column(Integer, primary_key=True)
    fis_id: Mapped[int] = Column(Integer, unique=True, nullable=False)
    fis_db_id: Mapped[Optional[int]] = Column(Integer)
    last_name: Mapped[str] = Column(String, nullable=False)
    first_name: Mapped[str] = Column(String, nullable=False)
    nation_code: Mapped[str] = Column(CHAR(3), nullable=False)  # 3-letter country code
    gender: Mapped[Gender] = Column(Enum(Gender), nullable=False)
    birth_date: Mapped[Optional[Date]] = Column(Date)
    birth_year: Mapped[Optional[int]] = Column(Integer)
    ski_club: Mapped[Optional[str]] = Column(String)
    national_code: Mapped[Optional[str]] = Column(String)  # National federation skier ID
    
    race_results: Mapped[List["RaceResult"]] = relationship("RaceResult", back_populates="athlete")
    points: Mapped[List["AthletePoints"]] = relationship("AthletePoints", back_populates="athlete")

class Race(Base):
    """Database model representing a FIS race event.
    
    Attributes:
        id (int): Primary key
        fis_db_id (Optional[int]): FIS race ID (e.g., 124886)
        race_codex (Optional[str]): FIS competition codex (e.g., "1970")
        race_date (date): Date of the race
        discipline (Discipline): Race discipline
        race_name (Optional[str]): Name of the race
        location (Optional[str]): Race location
        win_time (Optional[float]): Winner's time in seconds
        penalty (Optional[float]): Calculated penalty value
        race_category (Optional[str]): FIS race category
        total_starters (Optional[int]): Total number of starters
        total_finishers (Optional[int]): Total number of finishers
        start_altitude (Optional[int]): Start altitude in meters
        finish_altitude (Optional[int]): Finish altitude in meters
        length (Optional[int]): Course length in meters
        gates (Optional[int]): Number of gates
        turning_gates (Optional[int]): Number of turning gates
        homologation (Optional[str]): Homologation number
        results (List[RaceResult]): List of race results for this race
    """
    __tablename__ = 'races'

    id: Mapped[int] = Column(Integer, primary_key=True)
    fis_db_id: Mapped[Optional[int]] = Column(Integer)  # FIS race ID
    race_codex: Mapped[Optional[str]] = Column(String)  # FIS competition codex
    race_date: Mapped[Date] = Column(Date, nullable=False)
    discipline: Mapped[Discipline] = Column(Enum(Discipline), nullable=False)
    race_name: Mapped[Optional[str]] = Column(String)
    location: Mapped[Optional[str]] = Column(String)
    nation: Mapped[Optional[str]] = Column(String)
    win_time: Mapped[Optional[float]] = Column(Float, nullable=True)  # Winner's time in seconds
    penalty: Mapped[Optional[float]] = Column(Float)  # Calculated penalty value
    race_category: Mapped[Optional[str]] = Column(String)  # FIS race category
    total_starters: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Total starters
    total_finishers: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Total finishers
    start_altitude: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Start altitude in meters
    finish_altitude: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Finish altitude in meters
    length: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Course length in meters
    gates1: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Number of gates
    gates2: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Number of gates, run 2
    turning_gates1: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Number of turning gates, run 1
    turning_gates2: Mapped[Optional[int]] = Column(Integer, nullable=True)  # Number of turning gates, run 2
    homologation: Mapped[Optional[str]] = Column(String, nullable=True)  # Homologation number
    
    race_results: Mapped[List["RaceResult"]] = relationship("RaceResult", back_populates="race")

class RaceResult(Base):
    """Database model representing a single athlete's race result.
    
    Attributes:
        id (int): Primary key
        race_id (int): Foreign key to Race
        athlete_id (int): Foreign key to Athlete
        points (Optional[float]): FIS points earned
        rank (Optional[int]): Race finish position
        racer_time (Optional[float]): Racer's time in seconds
        race_points (Optional[float]): Race points for finish position
        result (Optional[str]): Letter Result (DNF1, DNS, etc.)
        race (Race): Related Race object
        athlete (Athlete): Related Athlete object
    """
    __tablename__ = 'race_results'

    id: Mapped[int] = Column(Integer, primary_key=True)
    race_id: Mapped[int] = Column(Integer, ForeignKey('races.id'), nullable=False)
    athlete_id: Mapped[int] = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    points: Mapped[Optional[float]] = Column(Float)
    rank: Mapped[Optional[int]] = Column(Integer)
    racer_time: Mapped[Optional[float]] = Column(Float, nullable=True)  # Racer's time in seconds
    result: Mapped[Optional[str]] = Column(String, nullable=True)  # Letter Result (DNF1, DNS, etc.)
    run1_time: Mapped[Optional[float]] = Column(Float, nullable=True)  # First run time in seconds
    run2_time: Mapped[Optional[float]] = Column(Float, nullable=True)  # Second run time in seconds
    
    race: Mapped["Race"] = relationship("Race", back_populates="race_results")
    athlete: Mapped["Athlete"] = relationship("Athlete", back_populates="race_results")

class PointsList(Base):
    """Database model representing a FIS points list publication.
    
    Attributes:
        id (int): Primary key
        valid_from (date): Start date of validity period
        valid_to (date): End date of validity period
        season (str): Season identifier (e.g., "2023/24")
        listid (int): FIS list ID (sequential, e.g. last 24/25 is 413)
        athlete_points (List[AthletePoints]): List of athlete points records
        name (str): Name of the points list
    """
    __tablename__ = 'points_lists'

    id: Mapped[int] = Column(Integer, primary_key=True)
    valid_from: Mapped[Date] = Column(Date, nullable=False)
    valid_to: Mapped[Date] = Column(Date, nullable=False)
    season: Mapped[str] = Column(String, nullable=False)
    listid: Mapped[int] = Column(Integer, nullable=False)
    name: Mapped[str] = Column(String, nullable=False)
    athlete_points: Mapped[List["AthletePoints"]] = relationship("AthletePoints", back_populates="points_list")

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

    id: Mapped[int] = Column(Integer, primary_key=True)
    athlete_id: Mapped[int] = Column(Integer, ForeignKey('athletes.id'), nullable=False)
    points_list_id: Mapped[int] = Column(Integer, ForeignKey('points_lists.id'), nullable=False)
    sl_points: Mapped[Optional[float]] = Column(Float)
    gs_points: Mapped[Optional[float]] = Column(Float)
    sg_points: Mapped[Optional[float]] = Column(Float)
    dh_points: Mapped[Optional[float]] = Column(Float)
    ac_points: Mapped[Optional[float]] = Column(Float)
    sl_rank: Mapped[Optional[int]] = Column(Integer)  # World rank position in Slalom
    gs_rank: Mapped[Optional[int]] = Column(Integer)  # World rank position in Giant Slalom
    sg_rank: Mapped[Optional[int]] = Column(Integer)  # World rank position in Super-G
    dh_rank: Mapped[Optional[int]] = Column(Integer)  # World rank position in Downhill
    ac_rank: Mapped[Optional[int]] = Column(Integer)  # World rank position in Alpine Combined
    calculated_date: Mapped[Optional[Date]] = Column(Date)
    
    athlete: Mapped["Athlete"] = relationship("Athlete", back_populates="points")
    points_list: Mapped["PointsList"] = relationship("PointsList", back_populates="athlete_points")