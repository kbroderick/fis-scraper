import pytest
from src.fis_scraper.scrapers.points_list import PointsListScraper
from src.fis_scraper.database.models import PointsList, Athlete, AthletePoints, Gender
from datetime import datetime, date

@pytest.fixture
def scraper():
    return PointsListScraper()

def test_parse_dates(scraper):
    # Test valid date string
    date_str = "01-01-2023 - 31-12-2023"  # Format: DD-MM-YYYY - DD-MM-YYYY
    valid_from, valid_to = scraper._parse_dates(date_str)
    assert valid_from == date(2023, 1, 1)
    assert valid_to == date(2023, 12, 31)
    
    # Test invalid date string
    date_str = "invalid"
    valid_from, valid_to = scraper._parse_dates(date_str)
    assert valid_from is None
    assert valid_to is None

def test_extract_season(scraper):
    # Test valid season string
    name = "1st FIS points list 2023/24"
    season = scraper._extract_season(name)
    assert season == "2023/24"
    
    # Test invalid season string
    name = "Invalid name"
    season = scraper._extract_season(name)
    assert season is None

def test_save_points_list(scraper):
    # Create test data
    points_list_data = {
        'name': 'FIS Points List 2023/24',
        'valid_from': date(2023, 1, 1),
        'valid_to': date(2023, 12, 31),
        'season': '2023/24'
    }
    
    # Create test DataFrame
    import pandas as pd
    df = pd.DataFrame({
        'FIS Code': [6532813],
        'Name': ['Test Athlete'],
        'Nation': ['USA'],
        'Nation Code': ['USA'],
        'Gender': ['M'],
        'Birth Date': ['1990-01-01'],
        'Ski Club': ['Test Club'],
        'National Code': ['6342814'],
        'SL': [50.0],
        'GS': [60.0],
        'SG': [70.0],
        'DH': [80.0],
        'SLpos': [100],
        'GSpos': [120],
        'SGpos': [140],
        'DHpos': [160]
    })
    
    # Save points list
    scraper._save_points_list(points_list_data, df)
    
    # Verify database entries
    points_list = scraper.session.query(PointsList).first()
    assert points_list is not None
    assert points_list.valid_from == date(2023, 1, 1)
    assert points_list.valid_to == date(2023, 12, 31)
    assert points_list.season == '2023/24'
    
    athlete = scraper.session.query(Athlete).first()
    assert athlete is not None
    assert athlete.fis_id == 6532813
    assert athlete.name == 'Test Athlete'
    assert athlete.country == 'USA'
    assert athlete.nation_code == 'USA'
    assert athlete.gender == Gender.M
    assert athlete.birth_date == date(1990, 1, 1)
    assert athlete.birth_year == 1990
    assert athlete.ski_club == 'Test Club'
    assert athlete.national_code == '6342814'
    
    athlete_points = scraper.session.query(AthletePoints).first()
    assert athlete_points is not None
    assert athlete_points.sl_points == 50.0
    assert athlete_points.gs_points == 60.0
    assert athlete_points.sg_points == 70.0
    assert athlete_points.dh_points == 80.0
    assert athlete_points.sl_rank == 100
    assert athlete_points.gs_rank == 120
    assert athlete_points.sg_rank == 140
    assert athlete_points.dh_rank == 160
    
    # Clean up
    scraper.session.query(AthletePoints).delete()
    scraper.session.query(Athlete).delete()
    scraper.session.query(PointsList).delete()
    scraper.session.commit() 