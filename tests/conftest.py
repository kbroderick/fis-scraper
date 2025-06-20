import pytest
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

from src.fis_scraper.database.models import PointsList, Athlete, AthletePoints

def pytest_configure(config):
    config.option.capture = "tee-sys"

@pytest.fixture(scope='function', autouse=False)
def check_db_status(request, scraper):
    RED = '\033[91m'
    RESET = '\033[0m' # Resets the color back to default
    starting_lists = scraper.session.query(PointsList).count()
    starting_athletes = scraper.session.query(Athlete).count()
    starting_athlete_points = scraper.session.query(AthletePoints).count()
    yield
    ending_lists = scraper.session.query(PointsList).count()
    ending_athletes = scraper.session.query(Athlete).count()
    ending_athlete_points = scraper.session.query(AthletePoints).count()
    print(f"\n{request.function.__name__}:")
    if ending_lists > starting_lists:
        print(f"{RED}PointsList: {ending_lists - starting_lists}{RESET}")
    if ending_athletes > starting_athletes:
        print(f"{RED}Athlete: {ending_athletes - starting_athletes}{RESET}")
    if ending_athlete_points > starting_athlete_points:
        print(f"{RED}AthletePoints: {ending_athlete_points - starting_athlete_points}{RESET}")
    print("--- After test execution ---") 
