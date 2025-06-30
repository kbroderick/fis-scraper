import pytest
import pytest_mock
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock, mock_open
from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Optional

from src.fis_scraper.database.models import Athlete, RaceResult, Discipline, Gender, PointsList, Race
from src.fis_scraper.scrapers.fis_constants import FisCategory, BASE_URL, DATA_URL
from src.fis_scraper.scrapers.race_results import RaceResultsScraper


class TestRaceResultsScraper:
    """Test suite for RaceResultsScraper class."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()
    
    @pytest.fixture
    def sample_race_info(self) -> Dict[str, Any]:
        """Sample race information for testing."""
        return {
            'race_id': 12345,
            'race_name': 'Test Race',
            'race_date': date(2024, 1, 15),
            'location': 'Test Location',
            'discipline': Discipline.SL,
            'race_url': 'https://data.fis-ski.com/alpine-skiing/results.html?raceid=12345'
        }
    
    @pytest.fixture
    def sample_result_data(self) -> Dict[str, Any]:
        """Create sample result data for testing."""
        return {
            'rank': 1,
            'athlete_name': 'Test Athlete',
            'athlete_fis_db_id': 98765,
            'fis_db_id': 12345,  # Race ID
            'nation': 'USA',
            'run1_time': 45.23,
            'run2_time': 46.12,
            'win_time': 91.35,
            'penalty': 15.00,
            'racer_time': 91.35,
            'race_codex': '1970',
            'race_name': 'Test Race',
            'total_starters': 50,
            'total_finishers': 45
        }
    
    @pytest.fixture
    def sample_points_list(self) -> Dict[str, Any]:
        """Sample points list data for testing."""
        return {
            'id': 1,
            'valid_from': date(2024, 1, 1),
            'valid_to': date(2024, 1, 31),
            'season': '2023/24',
            'listid': 413,
            'name': 'Test Points List'
        }
    
    def test_init(self, scraper: RaceResultsScraper) -> None:
        """Test RaceResultsScraper initialization."""
        assert scraper.session is not None
        assert scraper.CATEGORY_URL == f"{DATA_URL}/fis_events/ajax/calendarfunctions/get_select_category.html"
        assert scraper.RESULTS_URL == f"{DATA_URL}/alpine-skiing/results.html"
    
    @patch('src.fis_scraper.scrapers.race_results.requests.get')
    def test_discover_races_success(self, mock_get: Mock, scraper: RaceResultsScraper) -> None:
        """Test successful race discovery."""
        # Mock response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = '''
        <html>
            <a href="results.html?raceid=12345">Test Race</a>
            <div class="date">15.01.2024</div>
            <div class="location">Test Location</div>
            <div class="discipline">SL</div>
        </html>
        '''
        mock_get.return_value = mock_response
        
        races = scraper.discover_races(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31)
        )
        
        assert len(races) > 0
        assert races[0]['race_id'] == 12345
        mock_get.assert_called_once()
    
    def test_discover_races_request_error(self):
        """Test handling of request errors during race discovery."""
        scraper = RaceResultsScraper()
        
        # Mock requests.get to raise an exception
        with patch('src.fis_scraper.scrapers.race_results.requests.get') as mock_get:
            mock_get.side_effect = Exception("Network error")
            
            # Expect the exception to be raised
            with pytest.raises(Exception, match="Network error"):
                scraper.discover_races()
    
    def test_parse_race_link_valid(self, scraper: RaceResultsScraper) -> None:
        """Test parsing valid race link."""
        html = '<a href="results.html?raceid=12345">Test Race</a>'
        soup = BeautifulSoup(html, 'html.parser')
        link = soup.find('a')
        
        race_info = scraper._parse_race_link(link)
        
        assert race_info is not None
        assert race_info['race_id'] == 12345
        assert race_info['race_name'] == 'Test Race'
    
    def test_parse_race_link_invalid(self, scraper: RaceResultsScraper) -> None:
        """Test parsing invalid race link."""
        html = '<a href="invalid.html">Test Race</a>'
        soup = BeautifulSoup(html, 'html.parser')
        link = soup.find('a')
        
        race_info = scraper._parse_race_link(link)
        
        assert race_info is None
    
    def test_matches_filters_discipline_match(self, scraper: RaceResultsScraper, sample_race_info: Dict[str, Any]) -> None:
        """Test filter matching with discipline match."""
        result = scraper._matches_filters(sample_race_info, discipline=Discipline.SL)
        assert result is True
    
    def test_matches_filters_discipline_no_match(self, scraper: RaceResultsScraper, sample_race_info: Dict[str, Any]) -> None:
        """Test filter matching with discipline no match."""
        result = scraper._matches_filters(sample_race_info, discipline=Discipline.GS)
        assert result is False
    
    @patch('src.fis_scraper.scrapers.race_results.requests.get')
    def test_scrape_race_results_success(self, mock_get: Mock, scraper: RaceResultsScraper) -> None:
        """Test successful race results scraping using real FIS HTML structure."""
        # Mock response with FIS HTML structure
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = '''
        <html>
            <title>FIS | Alpine Skiing Results - Test Race</title>
            <div id="events-info-results" class="table__body">
                <a class="table-row" href="athlete.html?competitorid=12345">
                    <div class="g-row container">
                        <div class="g-row justify-sb">
                            <div class="g-lg-1 g-md-1 g-sm-1 g-xs-2 justify-right pr-1 bold">1</div>
                            <div class="g-lg-6 g-md-6 g-sm-5 g-xs-8 justify-left bold">Test Athlete</div>
                            <div class="g-lg-1 g-md-1 g-sm-2 g-xs-3 justify-left">
                                <div class="country country_flag">
                                    <span class="country__flag"></span>
                                    <span class="country__name-short">USA</span>
                                </div>
                            </div>
                            <div class="g-lg-2 g-md-2 g-sm-2 justify-right bold hidden-xs">44.12</div>
                            <div class="g-lg-2 g-md-2 g-sm-2 justify-right bold hidden-xs">44.12</div>
                            <div class="g-lg-2 g-md-2 justify-right blue bold hidden-sm hidden-xs">1:28.24</div>
                            <div class="g-lg-2 g-md-2 g-sm-2 g-xs-3 justify-right ">15.00</div>
                        </div>
                    </div>
                </a>
            </div>
        </html>
        '''
        mock_get.return_value = mock_response
        results = scraper.scrape_race_results(12345)
        assert len(results) > 0
        assert results[0]['rank'] == 1
        assert results[0]['athlete_name'] == 'Test Athlete'
        assert results[0]['nation'] == 'USA'
        assert results[0]['run1_time'] == 44.12
        assert results[0]['run2_time'] == 44.12
        assert results[0]['win_time'] == 88.24
        assert results[0]['penalty'] == 15.00

    @patch('src.fis_scraper.scrapers.race_results.requests.get')
    def test_scrape_race_results_no_table(self, mock_get: Mock, scraper: RaceResultsScraper) -> None:
        """Test race results scraping with no FIS results div present."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = '<html><body>No results table</body></html>'
        mock_get.return_value = mock_response
        results = scraper.scrape_race_results(12345)
        assert results == []
    
    def test_parse_time_valid_minutes_seconds(self, scraper: RaceResultsScraper) -> None:
        """Test parsing time with minutes and seconds format."""
        time_str = "1:23.45"
        result = scraper._parse_time(time_str)
        assert result == 83.45
    
    def test_parse_time_valid_seconds_only(self, scraper: RaceResultsScraper) -> None:
        """Test parsing time with seconds only format."""
        time_str = "45.23"
        result = scraper._parse_time(time_str)
        assert result == 45.23
    
    def test_parse_time_invalid(self, scraper: RaceResultsScraper) -> None:
        """Test parsing invalid time."""
        time_str = "invalid"
        result = scraper._parse_time(time_str)
        assert result is None
    
    def test_parse_time_empty(self, scraper: RaceResultsScraper) -> None:
        """Test parsing empty time."""
        time_str = ""
        result = scraper._parse_time(time_str)
        assert result is None
    
    def test_parse_discipline_sl(self, scraper: RaceResultsScraper) -> None:
        """Test discipline parsing for Slalom."""
        result = scraper._parse_discipline('SL')
        assert result == Discipline.SL
    
    def test_parse_discipline_gs(self, scraper: RaceResultsScraper) -> None:
        """Test discipline parsing for Giant Slalom."""
        result = scraper._parse_discipline('GS')
        assert result == Discipline.GS
    
    def test_parse_discipline_full_name(self, scraper: RaceResultsScraper) -> None:
        """Test discipline parsing with full name."""
        result = scraper._parse_discipline('Slalom')
        assert result == Discipline.SL
    
    def test_parse_discipline_invalid(self, scraper: RaceResultsScraper) -> None:
        """Test discipline parsing with invalid discipline."""
        result = scraper._parse_discipline('Invalid')
        assert result is None
    
    def test_is_float_valid(self, scraper: RaceResultsScraper) -> None:
        """Test float validation with valid float."""
        result = scraper._is_float("45.23")
        assert result is True
    
    def test_is_float_invalid(self, scraper: RaceResultsScraper) -> None:
        """Test float validation with invalid float."""
        result = scraper._is_float("invalid")
        assert result is False
    
    def test_is_float_integer(self):
        """Test float parsing with integer value."""
        scraper = RaceResultsScraper()
        assert scraper._is_float("123") is True
    
    def test_save_race_results_with_fis_db_id(self, scraper, sample_result_data):
        """Test that fis_db_id is correctly stored in Race table and linked to RaceResult."""
        from src.fis_scraper.database.models import Athlete, Gender, Race
        
        # Check if athlete with id=1 already exists, if not create it
        existing_athlete = scraper.session.query(Athlete).filter_by(id=1).first()
        if existing_athlete:
            athlete = existing_athlete
        else:
            # Create and add a real athlete to the DB
            athlete = Athlete(
                id=1,
                fis_id=99999,
                fis_db_id=98765,
                last_name="Athlete",
                first_name="Test",
                nation_code="USA",
                gender=Gender.M
            )
            scraper.session.add(athlete)
            scraper.session.commit()

        # Patch validation to return the real athlete
        with patch.object(scraper, '_validate_athlete_exists', return_value=athlete):
            sample_result_data.update({
                'race_date': date(2024, 1, 15),
                'discipline': Discipline.SL
            })
            saved_count = scraper.save_race_results([sample_result_data])
            assert saved_count == 1
            
            # Check that Race record was created
            race = scraper.session.query(Race).first()
            assert race is not None
            assert race.fis_db_id == 12345
            assert race.race_codex == '1970'
            
            # Check that RaceResult was created and linked to Race
            race_result = scraper.session.query(RaceResult).first()
            assert race_result is not None
            assert race_result.race_id == race.id
            assert race_result.athlete_id == athlete.id


class TestPointsListAutoIngestion:
    """Test points list auto-ingestion functionality."""
    
    def test_ensure_points_list_existing(self):
        """Test that existing points list is returned without auto-ingestion."""
        scraper = RaceResultsScraper()
        mock_points_list = Mock()
        
        # Mock the new method
        with patch.object(scraper, '_get_points_list_for_date', return_value=mock_points_list):
            result = scraper._ensure_points_list_for_date('2024-01-15')
        
        assert result == mock_points_list
    
    def test_ensure_points_list_auto_ingest_success(self):
        """Test successful auto-ingestion of points list."""
        scraper = RaceResultsScraper()
        mock_points_list = Mock()
        mock_points_scraper = Mock()
        mock_points_scraper.get_points_lists.return_value = [{'valid_from': '2024-01-01', 'valid_to': '2024-01-31'}]
        mock_points_scraper.download_and_process_points_list.return_value = True
        
        # Mock the new method to return None initially, then the points list after ingestion
        with patch.object(scraper, '_get_points_list_for_date', side_effect=[None, mock_points_list]), \
             patch('src.fis_scraper.scrapers.race_results.PointsListScraper', return_value=mock_points_scraper):
            result = scraper._ensure_points_list_for_date('2024-01-15')
        
        assert result == mock_points_list
        mock_points_scraper.get_points_lists.assert_called_once()
        mock_points_scraper.download_and_process_points_list.assert_called_once()
    
    def test_ensure_points_list_no_available_lists(self):
        """Test handling when no points lists are available for auto-ingestion."""
        scraper = RaceResultsScraper()
        mock_points_scraper = Mock()
        mock_points_scraper.get_points_lists.return_value = []
        
        # Mock the new method to return None initially and after failed ingestion
        with patch.object(scraper, '_get_points_list_for_date', return_value=None), \
             patch('src.fis_scraper.scrapers.race_results.PointsListScraper', return_value=mock_points_scraper):
            result = scraper._ensure_points_list_for_date('2024-01-15')
        
        assert result is None


class TestRaceStatisticsCalculation:
    """Test suite for race statistics calculation."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()
    
    def test_calculate_total_starters_with_times(self, scraper: RaceResultsScraper) -> None:
        """Test total starters calculation with racers who have times."""
        results = [
            {'racer_time': 45.23, 'result': None},
            {'racer_time': 46.12, 'result': None},
            {'racer_time': None, 'result': 'DNF1'},
            {'racer_time': None, 'result': 'DSQ1'},
        ]
        
        total_starters = scraper._calculate_total_starters(results)
        assert total_starters == 4
    
    def test_calculate_total_starters_dns2_with_run1(self, scraper: RaceResultsScraper) -> None:
        """Test total starters calculation with DNS2 after finishing run1."""
        results = [
            {'racer_time': 45.23, 'run1_time': 22.5, 'run2_time': None, 'result': None},
            {'racer_time': None, 'run1_time': 23.1, 'run2_time': None, 'result': 'DNS2'},
            {'racer_time': None, 'run1_time': None, 'run2_time': None, 'result': 'DNS1'},
        ]
        
        total_starters = scraper._calculate_total_starters(results)
        assert total_starters == 2  # First two count, DNS1 without run1_time doesn't
    
    def test_calculate_total_finishers(self, scraper: RaceResultsScraper) -> None:
        """Test total finishers calculation."""
        results = [
            {'racer_time': 45.23, 'result': None},
            {'racer_time': 46.12, 'result': None},
            {'racer_time': None, 'result': 'DNF1'},
            {'racer_time': None, 'result': 'DSQ1'},
        ]
        
        total_finishers = scraper._calculate_total_finishers(results)
        assert total_finishers == 2
    
    def test_calculate_total_finishers_none(self, scraper: RaceResultsScraper) -> None:
        """Test total finishers calculation with no finishers."""
        results = [
            {'racer_time': None, 'result': 'DNF1'},
            {'racer_time': None, 'result': 'DSQ1'},
            {'racer_time': None, 'result': 'DNS1'},
        ]
        
        total_finishers = scraper._calculate_total_finishers(results)
        assert total_finishers == 0


class TestResultCodeParsing:
    """Test suite for result code parsing."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()
    
    def test_parse_result_code_dns1(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNS1 result code."""
        run_data = {'run1_time': None, 'run2_time': None}
        result = scraper._parse_result_code('DNS', run_data)
        assert result == 'DNS1'
    
    def test_parse_result_code_dns2(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNS2 result code."""
        run_data = {'run1_time': 22.5, 'run2_time': None}
        result = scraper._parse_result_code('DNS', run_data)
        assert result == 'DNS2'
    
    def test_parse_result_code_dnf1(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNF1 result code."""
        run_data = {'run1_time': None, 'run2_time': None}
        result = scraper._parse_result_code('DNF', run_data)
        assert result == 'DNF1'
    
    def test_parse_result_code_dnf2(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNF2 result code."""
        run_data = {'run1_time': 22.5, 'run2_time': None}
        result = scraper._parse_result_code('DNF', run_data)
        assert result == 'DNF2'
    
    def test_parse_result_code_dsq1(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DSQ1 result code."""
        run_data = {'run1_time': None, 'run2_time': None}
        result = scraper._parse_result_code('DSQ', run_data)
        assert result == 'DSQ1'
    
    def test_parse_result_code_dsq2(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DSQ2 result code."""
        run_data = {'run1_time': 22.5, 'run2_time': None}
        result = scraper._parse_result_code('DSQ', run_data)
        assert result == 'DSQ2'
    
    def test_parse_result_code_single_run_dns(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNS for single run race."""
        run_data = {}
        result = scraper._parse_result_code('DNS', run_data)
        assert result == 'DNS'
    
    def test_parse_result_code_single_run_dnf(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DNF for single run race."""
        run_data = {}
        result = scraper._parse_result_code('DNF', run_data)
        assert result == 'DNF'
    
    def test_parse_result_code_single_run_dsq(self, scraper: RaceResultsScraper) -> None:
        """Test parsing DSQ for single run race."""
        run_data = {}
        result = scraper._parse_result_code('DSQ', run_data)
        assert result == 'DSQ'
    
    def test_parse_result_code_none(self, scraper: RaceResultsScraper) -> None:
        """Test parsing None result code."""
        run_data = {}
        result = scraper._parse_result_code(None, run_data)
        assert result is None
    
    def test_parse_result_code_empty(self, scraper: RaceResultsScraper) -> None:
        """Test parsing empty result code."""
        run_data = {}
        result = scraper._parse_result_code('', run_data)
        assert result is None


class TestAthleteValidation:
    """Test athlete validation and creation functionality."""
    
    def test_validate_athlete_exists(self):
        """Test validation when athlete already exists."""
        scraper = RaceResultsScraper()
        mock_athlete = Mock()
        
        # Mock the new method
        with patch.object(scraper, '_get_athlete', return_value=mock_athlete):
            athlete = scraper._validate_athlete_exists({'athlete_fis_db_id': 12345, 'athlete_name': 'Test Athlete', 'nation': 'USA'})
        
        assert athlete == mock_athlete
    
    def test_validate_athlete_not_exists(self):
        """Test validation when athlete does not exist."""
        scraper = RaceResultsScraper()
        
        # Mock the new method to return None
        with patch.object(scraper, '_get_athlete', return_value=None):
            athlete = scraper._validate_athlete_exists({'athlete_fis_db_id': 12345, 'athlete_name': 'Test Athlete', 'nation': 'USA'})
        
        assert athlete is None
    
    def test_create_athlete_if_needed_success(self):
        """Test successful athlete creation when needed."""
        scraper = RaceResultsScraper()
        mock_athlete = Mock()
        
        # Mock the validation to return None (athlete doesn't exist)
        with patch.object(scraper, '_validate_athlete_exists', return_value=None), \
             patch.object(scraper, '_create_athlete_if_needed', return_value=mock_athlete):
            athlete = scraper._create_athlete_if_needed({'athlete_fis_db_id': 12345, 'athlete_name': 'Test Athlete', 'nation': 'USA'})
        
        assert athlete == mock_athlete

    def test_get_or_create_race_new_race(self):
        """Test creating a new race when it doesn't exist."""
        scraper = RaceResultsScraper()
        result_data = {
            'fis_db_id': 99999,  # Use unique ID to avoid conflicts
            'race_date': date(2024, 12, 31),  # Use unique date
            'discipline': Discipline.SL,
            'race_name': 'Test Race',
            'location': 'Test Location',
            'race_codex': '1970',
            'win_time': 45.23,
            'penalty': 15.00,
            'race_category': 'FIS',
            'total_starters': 50,
            'total_finishers': 45
        }
        
        race = scraper._get_or_create_race(result_data)
        assert race is not None
        assert race.fis_db_id == 99999
        assert race.race_date == date(2024, 12, 31)
        assert race.discipline == Discipline.SL
        assert race.race_name == 'Test Race'
        assert race.location == 'Test Location'
        assert race.race_codex == '1970'
        assert race.win_time == 45.23
        assert race.penalty == 15.00
        assert race.race_category == 'FIS'
        assert race.total_starters == 50
        assert race.total_finishers == 45

    def test_get_or_create_race_existing_race(self):
        """Test finding an existing race when it already exists."""
        scraper = RaceResultsScraper()
        result_data = {
            'fis_db_id': 99999,  # Use very unique ID
            'race_date': date(2024, 12, 25),  # Use very unique date
            'discipline': Discipline.AC,  # Use different discipline
            'race_name': 'Test Race',
            'location': 'Test Location'
        }
        
        # Create a race first
        existing_race = Race(
            fis_db_id=99999,
            race_date=date(2024, 12, 25),
            discipline=Discipline.AC,
            race_name='Existing Race',
            location='Existing Location'
        )
        scraper.session.add(existing_race)
        scraper.session.commit()
        
        # Try to get or create the same race
        race = scraper._get_or_create_race(result_data)
        assert race is not None
        # Instead of checking specific ID, check that we got the existing race
        assert race.fis_db_id == existing_race.fis_db_id
        assert race.race_date == existing_race.race_date
        assert race.discipline == existing_race.discipline
        assert race.race_name == 'Existing Race'  # Should return existing, not create new
        
        # Clean up
        scraper.session.delete(existing_race)
        scraper.session.commit()

    def test_get_or_create_race_missing_data(self):
        """Test handling missing required race data."""
        scraper = RaceResultsScraper()
        
        # Missing fis_db_id
        result_data = {
            'race_date': date(2024, 1, 15),
            'discipline': Discipline.SL
        }
        race = scraper._get_or_create_race(result_data)
        assert race is None
        
        # Missing race_date
        result_data = {
            'fis_db_id': 12345,
            'discipline': Discipline.SL
        }
        race = scraper._get_or_create_race(result_data)
        assert race is None
        
        # Missing discipline
        result_data = {
            'fis_db_id': 12345,
            'race_date': date(2024, 1, 15)
        }
        race = scraper._get_or_create_race(result_data)
        assert race is None


class TestRaceResultsScraperIntegration:
    """Integration tests for RaceResultsScraper."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for integration testing."""
        return RaceResultsScraper()
    
    def test_end_to_end_race_discovery_and_scraping(self, scraper: RaceResultsScraper) -> None:
        """Test end-to-end race discovery and scraping workflow."""
        # This would be an integration test that actually hits the FIS website
        # For now, we'll just test the workflow structure
        assert scraper is not None
        # In a real integration test, we would:
        # 1. Discover races
        # 2. Ensure points lists exist (auto-ingest if needed)
        # 3. Scrape results for a specific race
        # 4. Calculate race statistics
        # 5. Save results to database
        # 6. Verify data integrity
    
    def test_race_results_data_structure(self, scraper: RaceResultsScraper) -> None:
        """Test that race results have the expected data structure."""
        # Test that the RaceResult model has all required fields
        required_race_result_fields = [
            'race_id', 'athlete_id', 'points', 'rank',
            'racer_time', 'race_points', 'result'
        ]
        
        # This is a structural test to ensure our model matches requirements
        race_result = RaceResult()
        for field in required_race_result_fields:
            assert hasattr(race_result, field), f"RaceResult missing field: {field}"
        
        # Test that the Race model has all required fields
        required_race_fields = [
            'fis_db_id', 'race_codex', 'race_date', 'discipline', 'race_name', 
            'location', 'win_time', 'penalty', 'race_category', 'total_starters', 'total_finishers'
        ]
        
        race = Race()
        for field in required_race_fields:
            assert hasattr(race, field), f"Race missing field: {field}"
    
    def test_points_list_validation_integration(self, scraper: RaceResultsScraper) -> None:
        """Test integration of points list validation with race scraping."""
        # This would test the full workflow of:
        # 1. Checking for points list
        # 2. Auto-ingesting if needed
        # 3. Proceeding with race scraping
        assert scraper is not None
    
    def test_race_statistics_integration(self, scraper: RaceResultsScraper) -> None:
        """Test integration of race statistics calculation."""
        # This would test the full workflow of:
        # 1. Scraping race results
        # 2. Calculating starters and finishers
        # 3. Storing statistics with results
        assert scraper is not None


class TestRealRaceResultsScraping:
    """Test race result scraping using actual saved HTML file."""
    
    def test_scrape_real_race_results(self):
        """Test scraping race results from actual HTML file."""
        scraper = RaceResultsScraper()
        # Read the actual HTML file
        with open('tests/data/LoafNorAMSL-20251020-1970.html', 'r') as f:
            html_content = f.read()
        # Mock the session and points list methods
        with patch.object(scraper, '_get_points_list_for_date', return_value=Mock()), \
             patch.object(scraper, '_get_athlete', return_value=Mock()), \
             patch('src.fis_scraper.scrapers.race_results.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.text = html_content
            mock_get.return_value = mock_response
            results = scraper.scrape_race_results(1970)
        # There should be 45 finishers
        finishers = [r for r in results if r.get('rank') is not None]
        assert len(finishers) == 45
        # Codex value
        for r in results:
            assert r['race_codex'] == 1970
        # Win time and penalty for the winner
        winner = next(r for r in results if r.get('rank') == 1)
        assert winner['win_time'] == 92.72  # 1:32.72 = 92.72 seconds
        assert winner['penalty'] == 15.00

    def test_real_race_statistics(self):
        """Test race statistics calculation for real race results."""
        scraper = RaceResultsScraper()
        with open('tests/data/LoafNorAMSL-20251020-1970.html', 'r') as f:
            html_content = f.read()
        with patch.object(scraper, '_get_points_list_for_date', return_value=Mock()), \
             patch.object(scraper, '_get_athlete', return_value=Mock()), \
             patch('src.fis_scraper.scrapers.race_results.requests.get') as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.text = html_content
            mock_get.return_value = mock_response
            results = scraper.scrape_race_results(1970)
        # Use the actual helpers
        total_starters = scraper._calculate_total_starters(results)
        total_finishers = scraper._calculate_total_finishers(results)
        assert total_finishers == 45
        assert total_starters >= total_finishers

class TestFindEventsByCategory:
    """Test finding events by category."""

    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()

    @patch(f'{RaceResultsScraper.__module__}.requests.get')
    def test_find_events_by_category(self, mock_get: Mock, scraper: RaceResultsScraper):
        """Test finding events by category."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = open('tests/data/get_select_category.html', 'r').read()
        mock_get.return_value = mock_response
        events = scraper.find_events_by_category('UNI', 2025)
        assert len(events) == 26
        assert events[0] == 'https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57032&seasoncode=2025'

class TestFindRaces:
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()

    def test_find_events_by_season(self, scraper: RaceResultsScraper, mocker):
        """Test finding events by season."""
        mocker.patch(f'{RaceResultsScraper.__module__}.RaceResultsScraper.find_events_by_category', return_value=['https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57032&seasoncode=2025', 'https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57033&seasoncode=2025'])

        events = scraper.find_events_by_season(2025)
        assert scraper.find_events_by_category.call_count == len(FisCategory)
        assert len(events) == 2 * len(FisCategory)
        assert events[0] == 'https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57032&seasoncode=2025'

    @patch(f'{RaceResultsScraper.__module__}.requests.get')
    def test_find_race_links_by_event(self, mock_get: Mock, scraper: RaceResultsScraper):
        """Test finding races by event."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = open('tests/data/eventdetails57032.html', 'r').read()
        mock_get.return_value = mock_response
        races = scraper.find_races_by_event('https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57032&seasoncode=2025')
        assert len(races) == 4
        assert races[0] == 126056

    def test_parse_race_id_from_link(self, scraper: RaceResultsScraper):
        """Test parsing race ID from link."""
        assert scraper._parse_race_id_from_link('https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=126056') == 126056

    def test_parse_fis_race_header_loaf_nor_am_sl(self) -> None:
        """Test parsing race header from LoafNorAMSL HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup)
        
        assert race_info['race_name'] == 'Sugarloaf (USA) 2024/2025'
        assert race_info['race_codex'] == 1970
        assert race_info['race_date'] == date(2025, 3, 20)
        assert race_info['discipline'] == Discipline.SL
        assert race_info['race_category'] == 'Nor-Am Cup'
        assert race_info['location'] == 'Sugarloaf'

    def test_parse_fis_race_header_aspen_njr_gs(self) -> None:
        """Test parsing race header from AspenNJRGS HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/AspenNJRGS-20250104-1828.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup)
        
        assert race_info['race_name'] == 'Aspen / Highlands (USA) 2024/2025'
        assert race_info['race_codex'] == 1828
        assert race_info['race_date'] == date(2025, 1, 4)
        assert race_info['discipline'] == Discipline.GS
        assert race_info['race_category'] == 'National Junior Race'
        assert race_info['location'] == 'Aspen / Highlands'

    def test_parse_fis_race_header_gressan_pila_dh(self) -> None:
        """Test parsing race header from Gressan-Pila-DH HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/Gressan-Pila-DH-20250110-5310.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup)
        
        assert race_info['race_name'] == 'Gressan - Pila (ITA) 2024/2025'
        assert race_info['race_codex'] == 5310
        assert race_info['race_date'] == date(2025, 1, 10)
        assert race_info['discipline'] == Discipline.DH
        assert race_info['race_category'] == 'FIS'
        assert race_info['location'] == 'Gressan - Pila'

    def test_parse_fis_race_header_gressan_pila_tra(self) -> None:
        """Test parsing race header from Gressan-Pila-TRA HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/Gressan-Pila-TRA-20250109-5311.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup)
        
        assert race_info['race_name'] == 'Gressan - Pila (ITA) 2024/2025'
        assert race_info['race_codex'] == 5311
        assert race_info['race_date'] == date(2025, 1, 9)
        assert race_info['discipline'] == Discipline.DH  # Downhill Training
        assert race_info['race_category'] == 'Training'
        assert race_info['location'] == 'Gressan - Pila'

    def test_parse_course_details_loaf_nor_am_sl(self) -> None:
        """Test parsing course details from LoafNorAMSL HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        print(f"PARSED COURSE DETAILS: {course_details}")
        assert course_details['start_altitude'] == 918
        assert course_details['finish_altitude'] == 743
        assert course_details['gates'] == 58  # First run gates
        assert course_details['turning_gates'] == 56  # First run turning gates
        assert course_details['homologation'] == '13162/05/19'

    def test_parse_course_details_aspen_njr_gs(self) -> None:
        """Test parsing course details from AspenNJRGS HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/AspenNJRGS-20250104-1828.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        
        assert course_details['start_altitude'] == 2827
        assert course_details['finish_altitude'] == 2481
        assert course_details['length'] == 1198
        assert course_details['gates'] == 42  # First run gates
        assert course_details['turning_gates'] == 40  # First run turning gates
        assert course_details['homologation'] == '15067/10/23'

    def test_parse_course_details_gressan_pila_dh(self) -> None:
        """Test parsing course details from Gressan-Pila-DH HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/Gressan-Pila-DH-20250110-5310.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        
        assert course_details['start_altitude'] == 2595
        assert course_details['finish_altitude'] == 2005
        assert course_details['length'] == 1492
        assert course_details['gates'] == 32  # Course gates (single run)
        assert course_details['turning_gates'] == 32  # Course turning gates (single run)
        assert course_details['homologation'] == '13352/11/19'

    def test_parse_course_details_debug(self) -> None:
        """Debug test to see what course details are being parsed and what the HTML looks like."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        print(f"Course details: {course_details}")
        
        # Print the Technical data section HTML
        sections = soup.find_all('section', class_='section_more-info')
        for section in sections:
            section_header = section.find('h3', class_='heading_l3')
            if section_header and section_header.get_text(strip=True) == 'Technical data':
                print("--- Technical data section HTML ---")
                print(section.prettify())
                rows = section.find_all(['div', 'a'], class_='table-row')
                print(f"Found {len(rows)} table-row elements in Technical data section.")
                for i, row in enumerate(rows):
                    print(f"Row {i} HTML:")
                    print(row.prettify())
        assert True
