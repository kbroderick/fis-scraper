from asyncio.log import logger
import pytest
import pytest_mock
import pprint
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock, mock_open
from bs4 import BeautifulSoup, Tag
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import column, Integer
import freezegun

import src
from src.fis_scraper.database.models import Athlete, AthletePoints, RaceResult, Discipline, Gender, PointsList, Race
from src.fis_scraper.scrapers.fis_constants import FisCategory, BASE_URL, DATA_URL
from src.fis_scraper.scrapers.points_list_scraper import PointsListScraper
from src.fis_scraper.scrapers.race_results_scraper import RaceResultsScraper


class TestRaceResultsScraper:
    """Test suite for RaceResultsScraper class."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()
      
    def test_init(self, scraper: RaceResultsScraper) -> None:
        """Test RaceResultsScraper initialization."""
        assert scraper.session is not None
        assert scraper.RESULTS_URL == f"{DATA_URL}/alpine-skiing/results.html"
    
    def test_scrape_race_results(self, scraper: RaceResultsScraper):
        """
        Test scraping race results from actual HTML file, verify winner,
        additional finisher, DNF2 example, race info.
        """
        # Read the actual HTML file
        with open('tests/data/html/races/LoafNorAMSL-20251020-1970.html', 'r') as f:
            html_content = f.read()
        with patch.object(scraper, '_get_points_list_for_date', return_value=Mock()), \
             patch.object(scraper, '_get_race_results_page') as mock_get:
            mock_get.return_value = BeautifulSoup(html_content, 'html.parser')

            race_details, results = scraper.scrape_race_results(124886)

            assert mock_get.call_count == 1
            assert mock_get.call_args[0][0] == 124886

        # There should be 45 finishers
        finishers = [r for r in results if r.get('rank') is not None]
        assert len(finishers) == 45
        assert len(results) == 97

        assert race_details['race_codex'] == 1970
        assert race_details['win_time'] == 92.72  # 1:32.72 = 92.72 seconds
        assert race_details['penalty'] == 15.00
        assert race_details['total_starters'] == 97
        assert race_details['total_finishers'] == 45

        assert results[0]['racer_time'] == 92.72
        assert results[0]['result'] == None
        assert results[0]['points'] == 15.00
        assert results[0]['athlete_name'] == 'VIDOVIC Matej'
        assert results[0]['nation'] == 'CRO'
        assert results[0]['run1_time'] == 46.77
        assert results[0]['run2_time'] == 45.95

        assert results[1]['racer_time'] == 92.72 + 0.36
        assert results[1]['result'] == None
        assert results[1]['points'] == 17.83

        assert results[45]['racer_time'] == None
        assert results[45]['result'] == 'DNF2'
        assert results[45]['points'] == None

    def test_scrape_race_results_no_table(self, mocker, scraper: RaceResultsScraper) -> None:
        """Test race results scraping for future race with no FIS results div present."""
        with open('tests/data/html/races/ElColorado-GS-0154-127132-no-results.html', 'r') as f:
            html_content = f.read()
        with patch.object(scraper, '_get_points_list_for_date', return_value=Mock()), \
             patch.object(scraper, '_get_race_results_page') as mock_get:
            mock_get.return_value = BeautifulSoup(html_content, 'html.parser')

            race_info, results = scraper.scrape_race_results(127132)

            assert mock_get.call_count == 1
            assert mock_get.call_args[0][0] == 127132

        assert race_info == { 
                            'race_codex': 154,
                            'race_date': date(2025, 7, 24),
                            'discipline': Discipline.GS,
                            'fis_db_id': 127132,
                            'homologation': '14984/06/23',
                            'race_category': 'FIS',
                            'location': 'El Colorado',
                             'nation': 'CHI' }
        assert results == []


    def test_parse_fis_race_header_loaf_nor_am_sl(self) -> None:
        """Test parsing race header from LoafNorAMSL HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup, 124886)
        
        assert race_info['race_name'] == 'Sugarloaf (USA) 2024/2025'
        assert race_info['race_codex'] == 1970
        assert race_info['race_date'] == date(2025, 3, 20)
        assert race_info['discipline'] == Discipline.SL
        assert race_info['race_category'] == 'Nor-Am Cup'
        assert race_info['location'] == 'Sugarloaf'

    def test_parse_fis_race_header_aspen_njr_gs(self) -> None:
        """Test parsing race header from AspenNJRGS HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/AspenNJRGS-20250104-1828.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup, 126799)
        
        assert race_info['race_name'] == 'Aspen / Highlands (USA) 2024/2025'
        assert race_info['race_codex'] == 1828
        assert race_info['race_date'] == date(2025, 1, 4)
        assert race_info['discipline'] == Discipline.GS
        assert race_info['race_category'] == 'National Junior Race'
        assert race_info['location'] == 'Aspen / Highlands'

    def test_parse_fis_race_header_gressan_pila_dh(self) -> None:
        """Test parsing race header from Gressan-Pila-DH HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/Gressan-Pila-DH-20250110-5310.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup, 124709)
        
        assert race_info['race_name'] == 'Gressan - Pila (ITA) 2024/2025'
        assert race_info['race_codex'] == 5310
        assert race_info['race_date'] == date(2025, 1, 10)
        assert race_info['discipline'] == Discipline.DH
        assert race_info['race_category'] == 'FIS'
        assert race_info['location'] == 'Gressan - Pila'

    def test_parse_fis_race_header_gressan_pila_tra(self) -> None:
        """Test parsing race header from Gressan-Pila-TRA HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/Gressan-Pila-TRA-20250109-5311.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        race_info = scraper._parse_fis_race_header(soup, 124708)
        
        assert race_info['race_name'] == 'Gressan - Pila (ITA) 2024/2025'
        assert race_info['race_codex'] == 5311
        assert race_info['race_date'] == date(2025, 1, 9)
        assert race_info['discipline'] == Discipline.DH  # Downhill Training
        assert race_info['race_category'] == 'Training'
        assert race_info['location'] == 'Gressan - Pila'

    def test_parse_course_details_loaf_nor_am_sl(self) -> None:
        """Test parsing course details from LoafNorAMSL HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        assert course_details['start_altitude'] == 918
        assert course_details['finish_altitude'] == 743
        assert course_details['gates1'] == 58  # First run gates
        assert course_details['turning_gates1'] == 56  # First run turning gates
        assert course_details['gates2'] == 54  # Second run gates
        assert course_details['turning_gates2'] == 53  # Second run turning gates
        assert course_details['homologation'] == '13162/05/19'

    def test_parse_course_details_loaf_nor_am_dh(self) -> None:
        """Test parsing course details from LoafNorAMDH HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/LoafNorAMDH-20250312-1963-124875.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        assert course_details['start_altitude'] == 1146
        assert course_details['finish_altitude'] == 618
        assert course_details['gates1'] == 30
        assert course_details['homologation'] == '15602/12/24'

    def test_parse_course_details_loaf_nor_am_sg(self) -> None:
        """ Test parsing course details from LoafNorAMSG HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/LoafNorAMSG-20250313-1966-124881.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        assert course_details['start_altitude'] == 1146
        assert course_details['finish_altitude'] == 618
        assert course_details['gates1'] == 38
        assert course_details['turning_gates1'] == 35
        assert course_details['homologation'] == '15603/12/24'

    def test_parse_course_details_aspen_njr_gs(self) -> None:
        """Test parsing course details from AspenNJRGS HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/AspenNJRGS-20250104-1828.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        
        assert course_details['start_altitude'] == 2827
        assert course_details['finish_altitude'] == 2481
        assert course_details['length'] == 1198
        assert course_details['gates1'] == 42  # First run gates
        assert course_details['turning_gates1'] == 40  # First run turning gates
        assert course_details['gates2'] == 42  # Second run gates
        assert course_details['turning_gates2'] == 40  # Second run turning gates
        assert course_details['homologation'] == '15067/10/23'

    def test_parse_course_details_gressan_pila_dh(self) -> None:
        """Test parsing course details from Gressan-Pila-DH HTML file."""
        scraper = RaceResultsScraper()
        
        with open('tests/data/html/races/Gressan-Pila-DH-20250110-5310.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        course_details = scraper._parse_course_details(soup)
        
        assert course_details['start_altitude'] == 2595
        assert course_details['finish_altitude'] == 2005
        assert course_details['length'] == 1492
        assert course_details['gates1'] == 32  # Course gates (single run)
        assert course_details['turning_gates1'] == 32  # Course turning gates (single run)
        assert course_details['homologation'] == '13352/11/19'

    def test_parse_fis_table_row_loaf_nor_am_sl(self) -> None:
        """Test parsing FIS table row from LoafNorAMSL HTML file."""
        scraper = RaceResultsScraper()
        
        # actual HTML from FIS website / LoafNorAMSL-20251020-1970.html
        html_content = """
            <a class="table-row" href="https://www.fis-ski.com/DB/general/athlete-biography.html?sectorcode=al&amp;competitorid=146640" target="_self">
                    <div class="g-row container">
                        <div class="g-row justify-sb">
                            <div class="g-lg-1 g-md-1 g-sm-1 g-xs-2 justify-right pr-1 bold">1</div>
                            <div class="g-lg-1 g-md-1 g-sm-1 justify-center hidden-sm-down gray">3</div>
                            <div class="pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray">380334</div>
                            <div class="g-lg-6 g-md-6 g-sm-5 g-xs-8 justify-left bold">VIDOVIC Matej</div>

                            <!-- <div class="g-lg-1 g-md-1 g-sm-2 g-xs-3 justify-left">--><!--</div>-->
                            <div class="g-lg-1 g-md-1 hidden-sm-down justify-left">1993</div>

                            <div class="g-lg-1 g-md-1 g-sm-2 g-xs-3 justify-left">
                                <div class="country country_flag">
                                    <span class="country__flag">
                                        <span class="flag flag-CRO"></span>
                                    </span>
                                    <span class="country__name-short">CRO</span>
                                </div>
                            </div>

                            <div class="g-lg-2 g-md-2 g-sm-2 justify-right bold hidden-xs">46.77</div>
                            <div class="g-lg-2 g-md-2 g-sm-2 justify-right bold hidden-xs">45.95</div>
                                                            
                            <div class="g-lg-2 g-md-2 justify-right blue bold hidden-sm hidden-xs">1:32.72</div>
                            <div class="g-lg-2 g-md-2 g-sm-3 g-xs-5 justify-right bold"><span class="hidden-md-up visible-sm">1:32.72</span></div>
                                                            
                            <div class="g-lg-2 g-md-2 g-sm-2 g-xs-3 justify-right ">15.00</div>
                            <div class="g-lg-2 g-md-2 g-sm-2 justify-right hidden-xs">100</div>
                        </div>
                    </div>
                </a>
        """

        soup = BeautifulSoup(html_content, 'html.parser')
        row = soup.find('a', class_='table-row')
        result = scraper._parse_fis_table_row(row, 124886)
        assert result['athlete_name'] == 'VIDOVIC Matej'
        assert result['rank'] == 1
        assert result['racer_time'] == 92.72
        assert result['run1_time'] == 46.77
        assert result['run2_time'] == 45.95
        assert result['result'] == None
        assert result['points'] == 15.00
        assert result['nation'] == 'CRO'

    def test_parse_fis_table_row_gressan_pila_dh(self) -> None:
        """Test parsing FIS table row from Gressan-Pila-DH HTML file."""
        scraper = RaceResultsScraper()
        
        # actual HTML from FIS website / Gressan-Pila-DH-20250110-5310.html
        html_content = """
            <a class="table-row" href="https://www.fis-ski.com/DB/general/athlete-biography.html?sectorcode=al&amp;competitorid=231510" target="_self">
              <div class="g-row container">
                <div class="g-row justify-sb">
                  <div class="g-lg-1 g-md-1 g-sm-1 g-xs-2 justify-right pr-1 bold">1</div>
                  <div class="g-lg-1 g-md-1 g-sm-1 justify-center hidden-sm-down gray">8</div>
                  <div class="pr-1 g-lg-2 g-md-2 g-sm-2 hidden-xs justify-right gray">6295804</div>
                  <div class="g-lg-12 g-md-12 g-sm-11 g-xs-8 justify-left bold">DAL VECCHIO Elena</div>

                  <!-- <div class="g-lg-1 g-md-1 g-sm-2 g-xs-3 justify-left">--><!--</div>-->
                  <div class="g-lg-1 g-md-1 hidden-sm-down justify-left">2002</div>

                  <div class="g-lg-1 g-md-1 g-sm-2 g-xs-3 justify-left">
                    <div class="country country_flag">
                      <span class="country__flag">
                        <span class="flag flag-ITA"></span>
                      </span>
                      <span class="country__name-short">ITA</span>
                    </div>
                  </div>

                                                        
                  <div class="g-lg-2 g-md-2 justify-right blue bold hidden-sm hidden-xs">1:18.86</div>
                  <div class="g-lg-2 g-md-2 g-sm-3 g-xs-5 justify-right bold"><span class="hidden-md-up visible-sm">1:18.86</span></div>
                                                           
                  <div class="g-lg-2 g-md-2 g-sm-2 g-xs-3 justify-right ">33.63</div>
                </div>
              </div>
            </a>
            """
        soup = BeautifulSoup(html_content, 'html.parser')
        row = soup.find('a', class_='table-row')
        result = scraper._parse_fis_table_row(row, 124886)
        pprint.pprint(result)
        assert result['athlete_name'] == 'DAL VECCHIO Elena'
        assert result['nation'] == 'ITA'
        assert result['rank'] == 1
        assert result['racer_time'] == 78.86
        assert result['run1_time'] == 78.86
        assert result['run2_time'] == None
        assert result['result'] == None
        assert result['points'] == 33.63

    def test_get_non_finishers(self, scraper: RaceResultsScraper):
        """Test getting non-finishers from HTML file."""
        with open('tests/data/html/races/LoafNorAMSL-20251020-1970.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        fis_results_div = soup.find('div', id='events-info-results', class_='table__body')
        non_finishers = scraper._get_non_finishers(fis_results_div.parent, 124886)

        assert len(non_finishers) == 52
        assert non_finishers[0]['athlete_name'] == 'SHEARS Bredy'
        assert non_finishers[0]['nation'] == 'CAN'
        assert non_finishers[0]['rank'] == None
        assert non_finishers[0]['racer_time'] == None
        assert non_finishers[0]['run1_time'] == 54.67
        assert non_finishers[0]['run2_time'] == None
        assert non_finishers[0]['result'] == 'DNF2'
        assert non_finishers[0]['points'] == None

    def test_get_race_results_page(self, scraper: RaceResultsScraper):
        """Test getting race results page."""
        with open('tests/data/html/races/ElColorado-GS-0154-127132-no-results.html', 'r', encoding='utf-8') as f:
            html_content = f.read()

        with patch('src.fis_scraper.scrapers.race_results_scraper.requests.get') as mock_get:
            mock_get.return_value = Mock()
            mock_get.return_value.raise_for_status.return_value = None
            mock_get.return_value.text = html_content
            assert scraper._get_race_results_page(127132) is not None
            assert mock_get.call_count == 1
            assert mock_get.call_args[0][0] == 'https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=127132'

class TestRaceResultsScraperDatabase:
    """Test suite for race results scraper database functionality."""

    @pytest.fixture
    def sample_results_soup(self) -> BeautifulSoup:
        """Create sample result data for testing."""
        with open('tests/data/html/races/LoafAbbrev-20250320-1970-124886.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        return BeautifulSoup(html_content, 'html.parser')

    @pytest.fixture
    def sample_parsed_results(self, scraper: RaceResultsScraper, sample_results_soup: BeautifulSoup) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Create sample parsed results for testing."""
        with patch.object(scraper, '_get_points_list_for_date', return_value=Mock()), \
             patch.object(scraper, '_get_race_results_page') as mock_get:
            mock_get.return_value = sample_results_soup
            race_info, results = scraper.scrape_race_results(124886)
        return race_info, results
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()
    
    @pytest.fixture(scope='class', autouse=True)
    def create_points_list(self):
        # we need a points list for import
        pls = PointsListScraper()
        with patch.object(PointsListScraper,
                          '_get_filelocation_for_points_list',
                          return_value='tests/data/points_lists/FAL_2025412-abbrev.csv'):
            res = pls.download_and_process_points_list(
                {'sectorcode': 'AL', 'seasoncode': '2025', 'listid': '412',
                 'name': '21st FIS points list 2024/25',
                 'valid_from': date(2025, 3, 12),
                 'valid_to': date(2025, 3, 22)}) # forcing 20 March, dates fictional
            if not res:
                raise Exception("Failed to load points list")
        print('Points list imported from FAL_2025412-abbrev.csv')
        yield

        points_lists = pls.session.query(PointsList).filter_by(listid=412)
        points_list_ids = [pl.id for pl in points_lists]
        athlete_points = \
            pls.session.query(AthletePoints).filter(AthletePoints.points_list_id.in_(points_list_ids))
        athlete_ids = [ap.athlete_id for ap in athlete_points]
        athlete_points.delete()
        pls.session.query(PointsList).filter(PointsList.id.in_(points_list_ids)).delete()
        pls.session.query(Athlete).filter(Athlete.id.in_(athlete_ids)).delete()
        pls.session.commit()

    def test_save_race_results(self, scraper: RaceResultsScraper,
                               sample_parsed_results: Tuple[Dict[str, Any], List[Dict[str, Any]]]):
        """Test saving race info and results to DB."""

        race_info, results = sample_parsed_results
        race = scraper._get_or_create_race(race_info)
        assert race is not None
        assert race.fis_db_id == 124886
        assert race.race_codex == 1970
        assert race.total_starters == len(results)
        assert race.nation == 'USA'

        count = scraper._save_race_results(race, results)
        assert count == len(results)

        assert scraper.session.query(RaceResult).filter_by(race_id=race.id).count() == len(results)

        scraper.session.query(RaceResult).filter_by(race_id=race.id).delete()
        scraper.session.query(Race).filter_by(id=race.id).delete()
        scraper.session.commit()

        assert scraper.session.query(Race).filter_by(fis_db_id=race_info['fis_db_id']).count() == 0
        assert scraper.session.query(RaceResult).filter_by(race_id=race.id).count() == 0

    def test_record_race_new_race(self, scraper: RaceResultsScraper,
                                  sample_parsed_results: Tuple[Dict[str, Any], List[Dict[str, Any]]]):
        """Test recording a new race."""
        race_info, results = sample_parsed_results
        count = scraper.record_race(race_info, results)
        assert count == len(results)

        race = scraper.session.query(Race).filter_by(fis_db_id=race_info['fis_db_id']).first()  
        assert race is not None
        assert scraper.session.query(RaceResult).filter_by(race_id=race.id).count() == len(results)

        scraper.session.query(RaceResult).filter_by(race_id=race.id).delete()
        scraper.session.query(Race).filter_by(id=race.id).delete()
        scraper.session.commit()

        assert scraper.session.query(Race).filter_by(fis_db_id=race_info['fis_db_id']).count() == 0
        assert scraper.session.query(RaceResult).filter_by(race_id=race.id).count() == 0


    def test_record_race_no_results(self, scraper: RaceResultsScraper,
                                   sample_parsed_results: Tuple[Dict[str, Any], List[Dict[str, Any]]]):
        """Test recording a race with no results."""
        race_info, _ = sample_parsed_results
        count = scraper.record_race(race_info, [])
        assert count == 0
        race = scraper.session.query(Race).filter_by(fis_db_id=race_info['fis_db_id']).first()
        assert race is not None
        assert scraper.session.query(RaceResult).filter_by(race_id=race.id).count() == 0

        scraper.session.query(Race).filter_by(id=race.id).delete()
        scraper.session.commit()

    def test_get_or_create_race_existing_race(self):
        """Test finding an existing race when it already exists."""
        scraper = RaceResultsScraper()
        race_info = {
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
        race = scraper._get_or_create_race(race_info)
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
        race_info = {
            'race_date': date(2024, 1, 15),
            'discipline': Discipline.SL
        }
        race = scraper._get_or_create_race(race_info)
        assert race is None
        
        # Missing race_date
        race_info = {
            'fis_db_id': 12345,
            'discipline': Discipline.SL
        }
        race = scraper._get_or_create_race(race_info)
        assert race is None
        
        # Missing discipline
        race_info = {
            'fis_db_id': 12345,
            'race_date': date(2024, 1, 15)
        }
        race = scraper._get_or_create_race(race_info)
        assert race is None


        
class TestRaceResultsScraperHelpers:
    """Test suite for race results scraper helpers."""
    
    @pytest.fixture
    def scraper(self) -> RaceResultsScraper:
        """Create a RaceResultsScraper instance for testing."""
        return RaceResultsScraper()

    #### HELPERS ####
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

    def test_get_result_status(self, scraper: RaceResultsScraper) -> None:
        """Test result status parsing."""
        assert scraper._get_result_status("Disqualified 1st Run") == "DSQ1"
        assert scraper._get_result_status("Did Not Start 2nd Run") == "DNS2"
        assert scraper._get_result_status("Did Not Finish 1st Run") == "DNF1"
        assert scraper._get_result_status("") is None
        assert scraper._get_result_status("Did Not Finish") == "DNF"

    def test_parse_race_id_from_link(self, scraper: RaceResultsScraper):
        """Test parsing race ID from link."""
        assert scraper._parse_race_id_from_link('https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=126056') == 126056

    @pytest.mark.freeze_time
    def test_get_current_season(self, scraper: RaceResultsScraper, freezer):
        """Test getting current season."""
        freezer.move_to("2025-01-01")
        assert scraper.get_current_season() == 2025
        freezer.move_to("2024-07-01")
        assert scraper.get_current_season() == 2025
        freezer.move_to("2024-06-30")
        assert scraper.get_current_season() == 2024

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
             patch('src.fis_scraper.scrapers.race_results_scraper.PointsListScraper', return_value=mock_points_scraper):
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
             patch('src.fis_scraper.scrapers.race_results_scraper.PointsListScraper', return_value=mock_points_scraper):
            result = scraper._ensure_points_list_for_date('2024-01-15')
        
        assert result is None

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
            'racer_time', 'result', 'run1_time', 'run2_time'
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
        mock_response.text = open('tests/data/html/get_select_category.html', 'r').read()
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
        mock_response.text = open('tests/data/html/eventdetails57032.html', 'r').read()
        mock_get.return_value = mock_response
        races = scraper.find_races_by_event('https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=57032&seasoncode=2025')
        assert len(races) == 4
        assert races[0] == 126056