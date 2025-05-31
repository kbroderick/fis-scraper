import pytest
from fis_scraper.scrapers.athlete_results import AthleteResultsScraper
from fis_scraper.database.models import Athlete, RaceResult, Discipline, Gender
from datetime import date
from bs4 import BeautifulSoup

@pytest.fixture
def scraper():
    return AthleteResultsScraper()

@pytest.fixture
def test_athlete(scraper):
    # Create a test athlete
    athlete = Athlete(
        fis_id=12345,
        name="Test Athlete",
        country="USA",
        nation_code="USA",
        gender=Gender.M,
        birth_date=date(1990, 1, 1),
        birth_year=1990,
        ski_club="Test Club",
        national_code="4978003"
    )
    scraper.session.add(athlete)
    scraper.session.commit()
    return athlete

def test_parse_discipline(scraper):
    # Test valid disciplines
    assert scraper._parse_discipline('SL') == Discipline.SL
    assert scraper._parse_discipline('GS') == Discipline.GS
    assert scraper._parse_discipline('SG') == Discipline.SG
    assert scraper._parse_discipline('DH') == Discipline.DH
    
    # Test invalid discipline
    assert scraper._parse_discipline('Invalid') is None

def test_parse_athlete_details(scraper):
    # Create test HTML
    html = """
    <div class="athlete-info">
        <table>
            <tr><td>Nation</td><td>US123</td></tr>
            <tr><td>Gender</td><td>M</td></tr>
            <tr><td>Birth Date</td><td>01.01.1990</td></tr>
            <tr><td>Ski Club</td><td>Test Club</td></tr>
            <tr><td>National Code</td><td>4978003</td></tr>
        </table>
    </div>
    """
    soup = BeautifulSoup(html, 'html.parser')
    details = scraper._parse_athlete_details(soup)
    
    assert details['nation_code'] == '4978003'
    assert details['gender'] == Gender.M
    assert details['birth_date'] == date(1990, 1, 1)
    assert details['birth_year'] == 1990
    assert details['ski_club'] == 'Test Club'

def test_parse_result_row(scraper, test_athlete):
    # Create test cells
    html = """
    <tr>
        <td>01.01.2023</td>
        <td>SL</td>
        <td>Test Race</td>
        <td>Test Location</td>
        <td>5</td>
        <td>50.0</td>
    </tr>
    """
    soup = BeautifulSoup(html, 'html.parser')
    cells = soup.find('tr').find_all('td')
    
    # Parse result row
    result = scraper._parse_result_row(cells, test_athlete.fis_id)
    
    # Verify result
    assert result is not None
    assert result.race_date == date(2023, 1, 1)
    assert result.discipline == Discipline.SL
    assert result.race_name == "Test Race"
    assert result.location == "Test Location"
    assert result.rank == 5
    assert result.points == 50.0
    assert result.athlete_id == test_athlete.id
    
    # Clean up
    scraper.session.query(RaceResult).delete()
    scraper.session.query(Athlete).delete()
    scraper.session.commit()

def test_parse_result_row_invalid(scraper):
    # Test with invalid cells
    html = "<tr><td>Invalid</td></tr>"
    soup = BeautifulSoup(html, 'html.parser')
    cells = soup.find('tr').find_all('td')
    
    result = scraper._parse_result_row(cells, 12345)
    assert result is None 