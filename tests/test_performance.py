import pytest
from fis_scraper.analysis.performance import PerformanceAnalyzer
from fis_scraper.database.models import Athlete, RaceResult, AthletePoints, PointsList, Discipline, Gender
from datetime import datetime, date, timedelta
import pandas as pd

@pytest.fixture
def analyzer():
    return PerformanceAnalyzer()

@pytest.fixture
def test_athlete(analyzer):
    # Create a test athlete
    athlete = Athlete(
        fis_id=123456,
        name="Test Athlete",
        country="USA",
        nation_code="USA",
        gender=Gender.M
    )
    analyzer.session.add(athlete)
    analyzer.session.commit()
    return athlete

@pytest.fixture
def test_points_list(analyzer):
    # Create a test points list
    points_list = PointsList(
        publication_date=date(2023, 1, 1),
        valid_from=date(2023, 1, 1),
        valid_to=date(2023, 12, 31),
        season="2023/24"
    )
    analyzer.session.add(points_list)
    analyzer.session.commit()
    return points_list

def test_calculate_trend(analyzer):
    # Test increasing trend
    series = [1, 2, 3, 4, 5]
    trend = analyzer._calculate_trend(series)
    assert trend > 0
    
    # Test decreasing trend
    series = [5, 4, 3, 2, 1]
    trend = analyzer._calculate_trend(series)
    assert trend < 0
    
    # Test flat trend
    series = [1, 1, 1, 1, 1]
    trend = analyzer._calculate_trend(series)
    assert abs(trend) < 1e-10  # Allow for floating-point imprecision

def test_analyze_race_results(analyzer, test_athlete):
    # Create test race results
    results = [
        RaceResult(
            athlete_id=test_athlete.id,
            race_date=date(2023, 1, 1),
            discipline=Discipline.SL,
            points=50.0,
            rank=5,
            race_name="Test Race 1",
            location="Location 1"
        ),
        RaceResult(
            athlete_id=test_athlete.id,
            race_date=date(2023, 2, 1),
            discipline=Discipline.GS,
            points=40.0,
            rank=3,
            race_name="Test Race 2",
            location="Location 2"
        )
    ]
    analyzer.session.add_all(results)
    analyzer.session.commit()
    
    # Analyze results
    analysis = analyzer._analyze_race_results(pd.DataFrame([{
        'date': r.race_date,
        'discipline': r.discipline.name,
        'points': r.points,
        'rank': r.rank
    } for r in results]))
    
    # Verify analysis
    assert analysis['total_races'] == 2
    assert analysis['average_rank'] == 4.0
    assert analysis['best_rank'] == 3
    assert analysis['worst_rank'] == 5
    assert analysis['average_points'] == 45.0
    assert analysis['best_points'] == 40.0
    assert analysis['worst_points'] == 50.0
    
    # Clean up
    analyzer.session.query(RaceResult).delete()
    analyzer.session.query(Athlete).delete()
    analyzer.session.commit()

def test_analyze_points_trends(analyzer, test_athlete, test_points_list):
    # Create test athlete points
    athlete_points = AthletePoints(
        athlete_id=test_athlete.id,
        points_list_id=test_points_list.id,
        sl_points=50.0,
        gs_points=60.0,
        sg_points=70.0,
        dh_points=80.0,
        sl_rank=100,
        gs_rank=120,
        sg_rank=140,
        dh_rank=160
    )
    analyzer.session.add(athlete_points)
    analyzer.session.commit()
    
    # Analyze points trends
    analysis = analyzer._analyze_points_trends(pd.DataFrame([{
        'date': test_points_list.valid_from,
        'sl_points': athlete_points.sl_points,
        'gs_points': athlete_points.gs_points,
        'sg_points': athlete_points.sg_points,
        'dh_points': athlete_points.dh_points,
        'sl_rank': athlete_points.sl_rank,
        'gs_rank': athlete_points.gs_rank,
        'sg_rank': athlete_points.sg_rank,
        'dh_rank': athlete_points.dh_rank
    }]))
    
    # Verify analysis
    assert analysis['sl']['current_points'] == 50.0
    assert analysis['gs']['current_points'] == 60.0
    assert analysis['sg']['current_points'] == 70.0
    assert analysis['dh']['current_points'] == 80.0
    assert analysis['sl']['current_rank'] == 100
    assert analysis['gs']['current_rank'] == 120
    assert analysis['sg']['current_rank'] == 140
    assert analysis['dh']['current_rank'] == 160
    
    # Clean up
    analyzer.session.query(AthletePoints).delete()
    analyzer.session.query(PointsList).delete()
    analyzer.session.query(Athlete).delete()
    analyzer.session.commit() 