import pprint
import pytest
from datetime import date
from bs4 import BeautifulSoup
from urllib import response
from unittest.mock import MagicMock

from fis_scraper.database.models import Gender, Discipline
from fis_scraper.scrapers.athlete_results import AthleteResultsScraper

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

def test_parse_result_row_dnf(scraper):
    # Create test cells
    html = """
        <a class="table-row" href="https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&amp;competitorid=210989&amp;raceid=125607" target="_self">
            <div class="container g-xs-24">
                <div class="g-xs-4 g-sm-4 g-md-4 g-lg-4 justify-left">30-03-2025</div>
                <div class="g-md g-lg justify-left hidden-sm-down">Waterville Valley</div>
                <div class="g-xs-10 g-sm-8 hidden-md-up justify-left flex-xs-wrap flex-sm-wrap">
                    <div class="g-xs-24 justify-left clip-xs">Waterville Valley</div>
                    <div class="g-xs-24 justify-left clip-xs gray">Slalom</div>
                </div>
                <div class="g-xs-4 g-sm-3 g-md-2 g-lg-2 justify-left flex-xs-wrap flex-sm-wrap">
                    <div class="g-xs-24 justify-left">
                        <div class="country country_flag country_with-info">
                            <span class="country__flag">
                                <span class="flag-USA flag"></span>
                            </span>
                            <span class="country__name-short">USA</span>
                        </div>
                    </div>
                    <div class="g-xs-24 justify-left hidden-sm-up">FIS</div>
                </div>
                <div class="g-sm-3 g-md-5 g-lg-5 justify-left hidden-xs hidden-md-up">FIS</div>
                <div class="g-md-5 g-lg-5 justify-left hidden-sm-down">FIS</div>
                <div class="g-md-3 g-lg-3 justify-left hidden-sm-down">Slalom</div>
                <div class="g-xs-6 g-sm-6 g-md-6 g-lg-6 justify-right flex-xs-wrap">
                    <div class="g-xs-24 g-sm g-md g-lg justify-right">DNF1</div>
                    <div class="g-xs-24 g-sm-8 g-md-8 g-lg-8 justify-right"></div>
                            
                    <!--<div class="g-sm-8 g-md-8 g-lg-8 justify-right hidden-xs">//</div>-->
                    <!--<div class="g-sm-8 g-md-8 g-lg-8 justify-right hidden-xs"></div>-->
                    <div class="g-xs-24 g-sm-8 g-md-8 g-lg-8 justify-right"></div>
                </div>
            </div>
        </a>
        """
    soup = BeautifulSoup(html, 'html.parser')
    cells = soup.find('a')
    
    # Parse result row
    result = scraper._parse_result_row(cells)
    
    # Verify result
    assert result is not None
    assert result['race_date'] == date(2025, 3, 30)
    assert result['discipline'] == Discipline.SL
    assert result['location'] == "Waterville Valley"
    assert result['rank'] == None
    assert result['result'] == "DNF1"
    assert result['points'] == None
    assert result['race_category'] == 'FIS'
    assert result['competitorid'] == 210989
    assert result['fis_race_id'] == 125607
    assert result['cup_points'] == None
    assert result['nation'] == 'USA'
    assert result['race_url'] == "https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&competitorid=210989&raceid=125607"

def test_parse_result_row_points(scraper):
    # Create test cells
    html = """
        <a class="table-row" href="https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&competitorid=210989&raceid=124884" target="_self">
        <div class="container g-xs-24">
            <div class="g-xs-4 g-sm-4 g-md-4 g-lg-4 justify-left">18-03-2025</div>
            <div class="g-md g-lg justify-left hidden-sm-down">Sugarloaf</div>
            <div class="g-xs-10 g-sm-8 hidden-md-up justify-left flex-xs-wrap flex-sm-wrap">
                <div class="g-xs-24 justify-left clip-xs">Sugarloaf</div>
                <div class="g-xs-24 justify-left clip-xs gray">Giant Slalom</div>
            </div>
            <div class="g-xs-4 g-sm-3 g-md-2 g-lg-2 justify-left flex-xs-wrap flex-sm-wrap">
                <div class="g-xs-24 justify-left">
                    <div class="country country_flag country_with-info">
                        <span class="country__flag"><span class="flag-USA flag"></span></span>
                        <span class="country__name-short">USA</span>
                    </div>
                </div>
                <div class="g-xs-24 justify-left hidden-sm-up">NAC</div>
            </div>
            <div class="g-sm-3 g-md-5 g-lg-5 justify-left hidden-xs hidden-md-up">NAC</div>
            <div class="g-md-5 g-lg-5 justify-left hidden-sm-down">Nor-Am Cup</div>
            <div class="g-md-3 g-lg-3 justify-left hidden-sm-down">Giant Slalom</div>
            <div class="g-xs-6 g-sm-6 g-md-6 g-lg-6 justify-right flex-xs-wrap">
                <div class="g-xs-24 g-sm g-md g-lg justify-right">21</div>
                <div class="g-xs-24 g-sm-8 g-md-8 g-lg-8 justify-right">34.96</div>

                <!--<div class="g-sm-8 g-md-8 g-lg-8 justify-right hidden-xs">//</div>-->
                <!--<div class="g-sm-8 g-md-8 g-lg-8 justify-right hidden-xs"></div>-->
                <div class="g-xs-24 g-sm-8 g-md-8 g-lg-8 justify-right">10</div>
            </div>
        </div>
        </a>
    """
    soup = BeautifulSoup(html, 'html.parser')
    cells = soup.find('a')
    
    # Parse result row
    result = scraper._parse_result_row(cells)
    
    # Verify result
    assert result is not None
    assert result['race_date'] == date(2025, 3, 18)
    assert result['discipline'] == Discipline.GS
    assert result['location'] == 'Sugarloaf'
    assert result['cup_points'] == 10
    assert result['rank'] == 21
    assert result['points'] == 34.96
    assert result['race_category'] == 'NAC'
    assert result['competitorid'] == 210989
    assert result['fis_race_id'] == 124884
    assert result['nation'] == 'USA'
    assert result['race_url'] == "https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&competitorid=210989&raceid=124884"

def test_parse_result_row_invalid(scraper):
    # Test with invalid cells
    html = "<a class='table-row' href='https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&competitorid=210989&raceid=124884' target='_self'><div class='container g-xs-24'><div class='g-xs-4 g-sm-4 g-md-4 g-lg-4 justify-left'>30-03-2025</div></div></a>"
    soup = BeautifulSoup(html, 'html.parser')
    cells = soup.find('a')
    
    result = scraper._parse_result_row(cells)
    assert result is None 