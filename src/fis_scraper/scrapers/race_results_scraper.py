import argparse
import logging
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Union, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup, Tag
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .fis_constants import DATA_URL, BASE_URL, FisSector, FisCategory
from ..database.connection import get_session
from ..database.models import Athlete, RaceResult, Discipline, Gender, PointsList, Race
from .points_list_scraper import PointsListScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RaceResultsScraper:
    """Scraper for FIS race results from the FIS website.
    
    This class handles discovering and scraping race results, including
    parsing individual racer results, times, positions, and calculated
    points. Automatically ingests points lists when needed.
    """
    
    #CATEGORY_URL: str = f"{DATA_URL}//fis_events/ajax/calendarfunctions/get_select_category.html"
    CALENDAR_URL: str = "https://www.fis-ski.com/DB/alpine-skiing/calendar-results.html"
    RESULTS_URL: str = "https://data.fis-ski.com/alpine-skiing/results.html"
    #MYSTERIOUSLY_WORKING_URL: str = "https://data.fis-ski.com//fis_events/ajax/calendarfunctions/get_select_category.html?sectorcode=AL"
    

    def __init__(self, session: Optional[Session] = None) -> None:
        """Initialize the RaceResultsScraper with a database session (injectable for testing)."""
        self.session: Session = session or get_session()

    def find_events_by_season(self, season: int) -> List[str]:
        """Find race events by season."""
        events = []
        for category in FisCategory:
            events.extend(self.find_events_by_category(category.value, season))
        return events
    
    def find_events_by_category(self, category: str, season: int) -> List[str]:
        """
        Find race events by category and season.

        Args:
            category: The category to search for (e.g. 'WC', 'EC', 'UNI', etc.)
            season: The season to search for (e.g. 2024, 2025, etc.)

        Returns:
            List[str]: List of race (event) URLs
        """
        params = {
            'eventselection': '',
            'place': '',
            'sectorcode': 'AL',
            'seasoncode': season,
            'categorycode': category,
            'disciplinecode': '',
            'gendercode': '',
            'racedate': '',
            'racecodex': '',
            'nationcode': '',
            'seasonmonth': f'X-{season}',
            'saveselection': '-1',
            'seasonselection': ''
        }
        logger.debug(f"Using category URL: {self.CALENDAR_URL} with params: {params}")
        response = requests.get(self.CALENDAR_URL, params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        race_links = soup.find_all('a', {'class': 'pl-xs-0_6 pr-xs-0 g-sm-2 g-xs-3 justify-sm-center hidden-md-up bold'})
        return [link.get('href') for link in race_links]
        
    def process_events(self, events: List[str], discover_only: bool = False) -> Tuple[int, int, int]:
        """Process a list of events.
        
        Args:
            events: List of event URLs
            discover_only: Only discover races, don't scrape results

        Returns:
            Tuple[int, int, int]: Number of races recorded, number of races with no additional results, number of errors
        """
        recorded_races = 0
        no_additional_results = 0
        errors = 0

        for event in events:
            race_ids = self.find_races_by_event(event)
            for race_id in race_ids:
                if discover_only:
                    print(f"Race ID: {race_id}, link: {self._race_link_from_id(race_id)}")
                else:
                    race = self.session.query(Race).filter(Race.fis_db_id == race_id).first()
                    if race and race.race_results:
                        logger.debug(f"Race {race_id} already recorded with {len(race.race_results)} results; skipping.")
                        continue
                    elif race and not race.race_results:
                        logger.debug(f"Race {race_id} found in database but no results; scraping.")
                    else:
                        logger.debug(f"Race {race_id} not found in database; scraping.")

                    race_info, results = self.scrape_race_results(race_id)
                    if results:
                        saved_count = self.record_race(race_info, results)
                        if saved_count > 0:
                            logger.debug(f"Saved {saved_count} results for race {race_id}")
                            recorded_races += 1
                        elif saved_count == 0:
                            logger.info(f"No results found for race {race_id}--already recorded or no results found.")
                            no_additional_results += 1
                        else:
                            logger.error(f"Error saving results for race {race_id}: {saved_count}")
                            errors += 1
                    elif race_info.get('discipline') is None:
                        # NOTE: special case from scrape_race_results() where discipline is not found
                        #       by _parse_fis_race_header() / _parse_discipline() and we short-
                        #       circuit out of the loop. Other parsing failures could end up in
                        #       the else clause below.
                        logger.error(f"Discipline could not be parsed for {race_id} / {self._race_link_from_id(race_id)}")
                        errors += 1
                    else:
                        logger.warning(f"No results found for race {race_id}")
                        no_additional_results += 1

        logger.debug(f"Recorded {recorded_races} races, {no_additional_results} races with no additional results, {errors} errors")
        return recorded_races, no_additional_results, errors

    def find_races_by_event(self, event_url: str) -> List[int]:
        """
        Find races by event.
        
        Args:
            event_url: URL of the event
        
        Returns:
            List[int]: List of race IDs (FIS DB IDs for races in event)
        """
        response = requests.get(event_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        race_links = soup.find_all('a', {'class': 'g-lg-2 g-md-3 g-sm-2 g-xs-4 px-md-1 px-lg-1 pl-xs-1 justify-left'})
        links = [link.get('href') for link in race_links]
        return [self._parse_race_id_from_link(link) for link in links]

    def scrape_race_results(self, race_id: int) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Scrape complete results for a specific race. Verifies
        appropriate PointsList is available for the race date; does not
        check for existing record of race in database.

        If race has not yet occurred, will return empty results list.
        
        Args:
            race_id: FIS race ID
        
        Returns:
            Tuple[Dict[str, Any], List[Dict[str, Any]]]: Race header
                info and list of racer results
        """
        results: List[Dict[str, Any]] = []
        race_info: Dict[str, Any] = {}

        try:
            # Get race results page
            soup = self._get_race_results_page(race_id)
            if not soup:
                logger.error(f"No race results page found for race {race_id}")
                return race_info, results
            
            race_info = self._parse_fis_race_header(soup, race_id)
            if race_info.get('discipline') is None:
                logger.error(f"Discipline could not be parsed for {race_id} / {self._race_link_from_id(race_id)}")
                return race_info, results

            # NOTE: fis_results_div contains only finishers; its sibling
            #       DIVs contain non-finish results (DNF, DNS, etc.)
            fis_results_div = soup.find('div', id='events-info-results', class_='table__body')
            if not fis_results_div:
                logger.warning(f"No results table found for race {race_id}")
                return race_info, results
            rows = fis_results_div.find_all('a', class_='table-row')

            if race_info.get('race_date'):
                points_list = self._ensure_points_list_for_date(race_info['race_date'])
                if not points_list:
                    logger.error(f"Cannot scrape race {race_id}: No valid points list for date {race_info['race_date']}")
                    return race_info, results

            for row in rows:
                result = self._parse_fis_table_row(row, race_id)
                if result:
                    results.append(result)
            if results: # if we have results, we can calculate finishers and starters
                winner_info = self._get_winner_info(rows[0], race_id)
                race_info.update(winner_info)
                race_info['total_finishers'] = len(results)
                results.extend(self._get_non_finishers(fis_results_div.parent, race_id))
                race_info['total_starters'] = self._calculate_total_starters(results)
                logger.debug(f"Scraped {len(results)} results for race {race_id}")
        
            return race_info, results

        except requests.RequestException as e:
            logger.error(f"Error scraping race {race_id}: {e}")
        return race_info, results

    @staticmethod
    def get_current_season() -> int:
        """Get the current FIS season; 1 July starts new season."""
        date = datetime.now()
        if date.month >= 7:
            return date.year + 1
        else:
            return date.year
    
    def record_race(self, race_info: Dict[str, Any], results: List[Dict[str, Any]]) -> int:
        """
        Record a race and its results.
        
        Args:
            race_info: Race information dictionary
            results: List of race result dictionaries; if empty, race is
            recorded but no results are saved.
            
        Returns:
            int: Number of results saved
                -1: No race found or created
                0: Race found but no results saved (results already recorded or no results found)
                >0: Number of results saved
        """
        race = self._get_or_create_race(race_info)
        if race and len(race.race_results) > 0:
            if len(race.race_results) == len(results):
                logger.info(f"Race {race_info['fis_db_id']} already has {len(results)} results; continuing.")
            else:
                logger.error(f"Race {race_info['fis_db_id']} already has {len(race.race_results)} results; found {len(results)} new results; review and update manually.")
            return 0
        if race and results:
            return self._save_race_results(race, results)
        elif race:
            logger.debug(f"No results found for raceid {race_info['fis_db_id']}")
            return 0
        else:
            logger.warning(f"No race found or created for raceid {race_info['fis_db_id']}")
            return -1

    def _parse_race_id_from_link(self, link: str) -> int:
        """Parse race ID from a race link."""
        match = re.search(r'raceid=(\d+)', link)
        if match:
            return int(match.group(1))
        return None
    
    def _parse_race_link(self, link: Tag) -> Optional[Dict[str, Any]]:
        """Parse race information from a race link as it appears on per-
        athlete results page.
        
        Args:
            link: BeautifulSoup Tag containing race link
            
        Returns:
            Optional[Dict[str, Any]]: Race information dictionary
        """
        try:
            href = link.get('href', '')
            race_id_match = re.search(r'raceid=(\d+)', href)
            if not race_id_match:
                return None
                
            race_id = int(race_id_match.group(1))
            
            # Extract race details from link text and surrounding elements
            race_name = link.get_text(strip=True)
            
            # Look for date information
            date_element = link.find_parent().find_previous_sibling('div', class_='date')
            race_date = None
            if date_element:
                try:
                    date_str = date_element.get_text(strip=True)
                    race_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                except ValueError:
                    pass
            
            # Look for location information
            location_element = link.find_parent().find('div', class_='location')
            location = location_element.get_text(strip=True) if location_element else None
            
            # Look for discipline information
            discipline_element = link.find_parent().find('div', class_='discipline')
            discipline_str = discipline_element.get_text(strip=True) if discipline_element else None
            discipline = self._parse_discipline(discipline_str) if discipline_str else None
            
            return {
                'race_id': race_id,
                'race_name': race_name,
                'race_date': race_date,
                'location': location,
                'discipline': discipline,
                'race_url': urljoin(DATA_URL, href)
            }
            
        except (ValueError, AttributeError) as e:
            logger.error(f"Error parsing race link: {e}")
            return None
    
    def _get_points_list_for_date(self, race_date: date) -> Optional[PointsList]:
        """Return a points list valid for the given date, or None."""
        return self.session.query(PointsList).filter(
            PointsList.valid_from <= race_date,
            PointsList.valid_to >= race_date
        ).first()

    def _ensure_points_list_for_date(self, race_date: date) -> Optional[PointsList]:
        """Ensure a points list exists for the given race date, auto-ingest if needed."""
        points_list = self._get_points_list_for_date(race_date)
        if points_list:
            return points_list
        logger.info(f"No points list found for {race_date}, attempting to discover available lists...")
        points_scraper = PointsListScraper()
        available_lists = points_scraper.get_points_lists()
        valid_lists = [
            lst for lst in available_lists 
            if lst['valid_from'] and lst['valid_to'] and 
            lst['valid_from'] <= race_date <= lst['valid_to']
        ]
        if not valid_lists:
            logger.error(f"No available points lists found for race date {race_date}")
            return None
        logger.debug(f"Auto-ingesting points list for {race_date}")
        success = points_scraper.download_and_process_points_list(valid_lists[0])
        if success:
            return self._get_points_list_for_date(race_date)
        return None
    
    def _race_link_from_id(self, race_id: int) -> str:
        """Get the race link from the race ID."""
        return f"{BASE_URL}/general/results.html?sectorcode=AL&raceid={race_id}"

    def _get_race_results_page(self, race_id: int) -> Optional[BeautifulSoup]:
        """
        Get the race results page for a given race ID.

        Args:
            race_id: FIS race ID

        Returns:
            Optional[BeautifulSoup]: BeautifulSoup object of race page,
            or None if error.
        """
        response = requests.get(self._race_link_from_id(race_id))
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')

    def _parse_fis_race_header(self, soup: BeautifulSoup, race_id: int) -> Dict[str, Any]:
        """Parse race header information from real FIS HTML structure.
        
        Args:
            soup: BeautifulSoup object of race page
            race_id: FIS race ID
        Returns:
            Dict[str, Any]: Race header information
        """
        race_info: Dict[str, Any] = {
            'fis_db_id': race_id
        }
        
        # Race name and location from <title>
        title = soup.find('title')
        if title and 'Results -' in title.text:
            parts = title.text.split('Results -')
            if len(parts) > 1:
                race_info['race_name'] = parts[1].strip()
        
        # Codex from JS or span
        codex = None
        script = soup.find('script', string=lambda t: t and 'competitionCodex' in t)
        if script:
            m = re.search(r'competitionCodex":(\d+)', script.text)
            if m:
                codex = int(m.group(1))
        if not codex:
            codex_span = soup.find('span', string=lambda t: t and 'CODEX:' in t)
            if codex_span:
                m = re.search(r'CODEX:\s*(\d{4})', codex_span.text)
                if m:
                    codex = m.group(1)
        if codex:
            race_info['race_codex'] = int(codex)
        
        # Parse date from event header
        date_element = soup.find('div', class_='timezone-date')
        if date_element and date_element.get('data-date'):
            try:
                date_str = date_element.get('data-date')
                race_info['race_date'] = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                pass
        
        # Parse discipline from event header
        discipline_element = soup.find('div', class_='event-header__kind')
        if discipline_element:
            discipline_text = discipline_element.get_text(strip=True)
            # Extract discipline from text like "Men's Giant Slalom" or "Women's Slalom"
            discipline_match = re.search(r"((?:Men's|Women's))\s+(.+)", discipline_text)
            if discipline_match:
                gender_str = discipline_match.group(1).strip()
                race_info['gender'] = self._parse_gender(gender_str)
                discipline_str = discipline_match.group(2).strip()
                race_info['discipline'] = self._parse_discipline(discipline_str)
        
        # Parse race category from event header subtitle
        category_element = soup.find('div', class_='event-header__subtitle')
        if category_element:
            race_info['race_category'] = category_element.get_text(strip=True)
        
        # Parse location from event header name
        location_element = soup.find('h1', class_='heading')
        if location_element:
            location_text = location_element.get_text(strip=True)
            # Extract location from text like "Aspen / Highlands (USA)" or "Sugarloaf (USA)"
            location_match = re.search(r"(.+?)\s*\(([A-Z]{3})\)", location_text)
            if location_match:
                race_info['location'] = location_match.group(1).strip()
                race_info['nation'] = location_match.group(2).strip()
        
        # Parse course details
        course_details = self._parse_course_details(soup)
        race_info.update(course_details)
        
        return race_info
    
    def _parse_course_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse course details from the FIS HTML structure.
        
        Args:
            soup: BeautifulSoup object of race page
        
        Returns:
            Dict[str, Any]: Course details including altitude, length, gates, homologation
        """
        course_info: Dict[str, Any] = {}
        
        def find_label_value(row, label_class, value_class, value_exact=True):
            if not value_exact:
                # For run sections: get all justify-left divs, first is label, second is value (skip bold)
                justify_left_divs = [div for div in row.find_all('div', recursive=True) if 'justify-left' in div.get('class', [])]
                label_div = None
                value_div = None
                for div in justify_left_divs:
                    if 'bold' in div.get('class', []):
                        label_div = div
                    elif value_div is None:
                        value_div = div
                return label_div, value_div
            else:
                label_div = None
                value_div = None
                for div in row.find_all('div', recursive=True):
                    classes = div.get('class', [])
                    if label_class in ' '.join(classes):
                        label_div = div
                    if value_class in ' '.join(classes):
                        value_div = div
                return label_div, value_div

        # Find the "More Information" section that contains course details
        sections = soup.find_all('section', class_='section_more-info')
        
        for section in sections:
            section_header = section.find('h3', class_='heading_l3')
            if not section_header:
                continue
            header_text = section_header.get_text(strip=True)
            # Parse altitude and length information from "Technical data" section
            if header_text == 'Technical data':
                rows = section.find_all(['div', 'a'], class_='table-row')
                for row in rows:
                    label_div, value_div = find_label_value(row, 'justify-left bold', 'justify-right', value_exact=True)
                    if label_div and value_div:
                        label = label_div.get_text(strip=True)
                        value = value_div.get_text(strip=True)
                        import re
                        if label == 'Start Altitude':
                            match = re.search(r'(\d+)', value)
                            if match:
                                course_info['start_altitude'] = int(match.group(1))
                        elif label == 'Finish Altitude':
                            match = re.search(r'(\d+)', value)
                            if match:
                                course_info['finish_altitude'] = int(match.group(1))
                        elif label == 'Length':
                            match = re.search(r'(\d+)', value)
                            if match:
                                course_info['length'] = int(match.group(1))
                        elif label == 'Homologation Number':
                            course_info['homologation'] = value.strip()
            # Look for gates information in run sections (1st Run (tech) or Course (speed)
            elif any(run_type in header_text for run_type in ['1st Run', 'Course']):
                rows = section.find_all(['div', 'a'], class_='table-row')
                for row in rows:
                    label_div, value_div = find_label_value(row, 'justify-left bold', 'justify-left', value_exact=False)
                    if label_div and value_div:
                        label = label_div.get_text(strip=True)
                        value = value_div.get_text(strip=True)
                        if label == 'Number of Gates':
                            if value.isdigit() and 'gates1' not in course_info:
                                course_info['gates1'] = int(value)
                        elif label == 'Turning Gates':
                            if value.isdigit() and 'turning_gates1' not in course_info:
                                course_info['turning_gates1'] = int(value)
            elif '2nd Run' in header_text:
                rows = section.find_all(['div', 'a'], class_='table-row')
                for row in rows:
                    label_div, value_div = find_label_value(row, 'justify-left bold', 'justify-left', value_exact=False)
                    if label_div and value_div:
                        label = label_div.get_text(strip=True)
                        value = value_div.get_text(strip=True)
                        if label == 'Number of Gates':
                            if value.isdigit() and 'gates2' not in course_info:
                                course_info['gates2'] = int(value)
                        elif label == 'Turning Gates':
                            if value.isdigit() and 'turning_gates2' not in course_info:
                                course_info['turning_gates2'] = int(value)
        return course_info

    def _parse_fis_table_row(self, row: Tag, race_id: int, result_status: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Parse a single FIS result row from <a class="table-row">.
        
        Args:
            row: BeautifulSoup Tag for the row
            race_id: FIS race ID
        
        Returns:
            Optional[Dict[str, Any]]: Parsed result data--
                'rank': rank (or None)
                'athlete_name': athlete_name (SURNAME Firstname)
                'athlete_fis_db_id': athlete_fis_db_id,
                'fis_db_id': race_id,  # Store the race ID here
                'nation': nation of athlete (3-letter code eg. USA, CRO)
                'run1_time': float time for 1st run, seconds (or None)
                'run2_time': float time for 2nd run, seconds (or None)
                'racer_time': float race time, seconds (or None)
                'points': FIS points result for racer (or None)
                'result': result_status (DNF1, DNF2, etc or None)
        """
        try:
            # Find the outer container: <div class='g-row container'>
            container = None
            for div in row.find_all('div'):
                classes = div.get('class', [])
                if 'g-row' in classes and 'container' in classes:
                    container = div
                    break
            if not container:
                return None
            # Find the inner row: <div class='g-row justify-sb'>
            inner_row = None
            for div in container.find_all('div', recursive=False):
                classes = div.get('class', [])
                if 'g-row' in classes and 'justify-sb' in classes:
                    inner_row = div
                    break
            if not inner_row:
                return None
            divs = inner_row.find_all('div', recursive=False)
            # Defensive: fallback to all divs if not found
            if not divs:
                divs = inner_row.find_all('div')
            # Rank
            rank = None
            if len(divs) > 0:
                rank_text = divs[0].get_text(strip=True)
                if rank_text.isdigit():
                    rank = int(rank_text)
            # Name
            athlete_name = None
            name_divs = row.select('div.justify-left.bold')
            if name_divs:
                athlete_name = name_divs[0].get_text(strip=True)
            # Nation
            nation = None
            nation_span = row.select_one('span.country__name-short')
            if nation_span:
                nation = nation_span.get_text(strip=True)
            # FIS DB ID (athlete's ID)
            athlete_fis_db_id = None
            href = row.get('href', '')
            m = re.search(r'competitorid=(\d+)', href)
            if m:
                athlete_fis_db_id = int(m.group(1))

            # Times: look for individual run times and combined time
            time_divs = row.select('div.justify-right.bold.hidden-xs')
            if len(time_divs) > 0:
                run1_time = self._parse_time(time_divs[0].get_text(strip=True))
                racer_time = self._parse_time(time_divs[-1].get_text(strip=True))
            else:
                run1_time = None
                racer_time = None
            if len(time_divs) > 1:
                run2_time = self._parse_time(time_divs[1].get_text(strip=True))
            else:
                run2_time = None

            points = None
            if not result_status:
                points_div = row.find('div', class_="g-lg-2 g-md-2 g-sm-2 g-xs-3 justify-right")
                if points_div and self._is_float(points_div.get_text(strip=True)):
                    points = float(points_div.get_text(strip=True))

            result = {
                'rank': rank,
                'athlete_name': athlete_name,
                'athlete_fis_db_id': athlete_fis_db_id,
                'fis_db_id': race_id,  # Store the race ID here
                'nation': nation,
                'run1_time': run1_time,
                'run2_time': run2_time,
                'racer_time': racer_time,
                'points': points,
                'result': result_status
            }
            return result
        except Exception as e:
            logger.error(f"Error parsing FIS table row: {e}")
            return None
        
    def _get_non_finishers(self, soup: BeautifulSoup, race_id: int) -> List[Dict[str, Any]]:
        """
        Returns non-finishing racers listed as participants in given race.
        
        Args:
            soup: BeautifulSoup Tag containing all div.table__body elets
            race_id: FIS race ID
        
        Returns:
            List[Dict[str, Any]]: List of non-finishing racers
        """
        non_finishers = []
        for div in soup.find_all('div', class_='table__body'):
            if div.get('id') == 'events-info-results':
                continue
            status = self._get_result_status(div.find_previous_sibling().get_text(strip=True))
            for row in div.find_all('a', class_='table-row'):
                result = self._parse_fis_table_row(row, race_id, result_status = status)
                if result:
                    non_finishers.append(result)
        return non_finishers

    def _get_result_status(self, text: str) -> Optional[str]:
        """
        Get result status from text.
        Args:
            text: Text to parse from results, eg "Disqualified 1st Run"
        
        Returns:
            Optional[str]: Result status, eg "DSQ1", "DNF2", "DNS2"
        """
        run = ""
        text = text.lower()
        if "2nd run" in text:
            run = 2
        elif "1st run" in text:
            run = 1
        
        if 'disqualified' in text:
            return f"DSQ{run}"
        elif 'did not finish' in text:
            return f"DNF{run}"
        elif 'did not start' in text:
            return f"DNS{run}"
        elif 'did not qualify' in text:
            return f"DNQ{run}"
        elif 'not permitted to start' in text:
            return f"NPS{run}"
        return None

    def _get_winner_info(self, row: Tag, race_id: int) -> Dict[str, Any]:
        """
        Get winner information from first row of results table.
        
        Args:
            soup: BeautifulSoup object of race page
        
        Returns:
            Dict[str, Any]: Winner information--win_time, penalty
        """
        winner_info = self._parse_fis_table_row(row, race_id)
        return { 'win_time': winner_info['racer_time'],
                 'penalty': winner_info['points'] }

    def _calculate_total_starters(self, results: List[Dict[str, Any]]) -> int:
        """Calculate total starters based on FIS rules.
        
        Args:
            results: List of race result dictionaries
            
        Returns:
            int: Total number of starters
        """
        starters = len(results)
        for result in results:
            if result['result'] == 'DNS1':
                starters -= 1
        
        return starters
      
    def _parse_time(self, time_str: str) -> Optional[float]:
        """Parse time string to float seconds.
        
        Args:
            time_str: Time string in format "MM:SS.sss" or "SS.sss"
            
        Returns:
            Optional[float]: Time in seconds
        """
        if not time_str or time_str == '-':
            return None
            
        try:
            if ':' in time_str:
                minutes, seconds = time_str.split(':')
                return float(minutes) * 60 + float(seconds)
            else:
                return float(time_str)
        except ValueError:
            return None
    
    def _parse_gender(self, gender_str: str) -> Optional[Gender]:
        """Parse gender string to Gender enum.
        
        Args:
            gender_str: Gender string: should contain 'women', 'men', or
             'mixed'
            
        Returns:
            Optional[Gender]: Gender enum value
        """
        if not gender_str:
            return None
        
        if 'women' in gender_str.lower():
            return Gender.F
        elif 'men' in gender_str.lower():
            return Gender.M
        elif 'mixed' in gender_str.lower():
            return Gender.A
        return None
    
    def _parse_discipline(self, discipline_str: str) -> Optional[Discipline]:
        """Parse discipline string to Discipline enum.
        
        Args:
            discipline_str: Discipline string
            
        Returns:
            Optional[Discipline]: Discipline enum value
        """
        if not discipline_str:
            return None
        
        # Clean the discipline string
        discipline_str = discipline_str.strip().upper()
        
        # Handle training variations by extracting the base discipline
        if 'TRAINING' in discipline_str:
            # Extract the base discipline from "Downhill Training", "Giant Slalom Training", etc.
            base_discipline = discipline_str.replace(' TRAINING', '').strip()
            discipline_str = base_discipline
            
        discipline_map = {
            'SL': Discipline.SL,
            'GS': Discipline.GS,
            'SG': Discipline.SG,
            'DH': Discipline.DH,
            'AC': Discipline.AC,
            'SLALOM': Discipline.SL,
            'GIANT SLALOM': Discipline.GS,
            'SUPER-G': Discipline.SG,
            'DOWNHILL': Discipline.DH,
            'ALPINE COMBINED': Discipline.AC,
            'SUPER G': Discipline.SG
        }
        
        discipline = discipline_map.get(discipline_str)
        if not discipline:
            logger.error(f"Discipline {discipline_str} not found in map")
        return discipline
    
    def _is_float(self, value: str) -> bool:
        """Check if string can be converted to float.
        
        Args:
            value: String to check
            
        Returns:
            bool: True if string can be converted to float
        """
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _get_fis_athlete(self, fis_db_id: int) -> Optional[Athlete]:
        """Return an athlete by FIS DB ID or name/nation, or None."""
        return self.session.query(Athlete).filter_by(fis_db_id=fis_db_id).first()
        
    def _get_or_create_race(self, race_info: Dict[str, Any]) -> Optional[Race]:
        """Get or create a Race record from race-info data.
        
        Args:
            race_info: Race data containing race information
            
        Returns:
            Optional[Race]: Race object if found or created, None otherwise
        """
        fis_db_id = race_info.get('fis_db_id')
        race_date = race_info.get('race_date')
        discipline = race_info.get('discipline')
        
        if not all([fis_db_id, race_date, discipline]):
            logger.error(f"Missing required race data: fis_db_id={fis_db_id}, race_date={race_date}, discipline={discipline}")
            logger.debug(f"Race info: {race_info}")
            return None
        
        # Try to find existing race
        race = self.session.query(Race).filter_by(
            fis_db_id=fis_db_id,
            race_date=race_date,
            discipline=discipline
        ).first()
        
        if race:
            return race
        
        # Create new race record
        try:
            race = Race(
                fis_db_id=fis_db_id,
                race_codex=race_info.get('race_codex'),
                race_date=race_date,
                discipline=discipline,
                race_name=race_info.get('race_name'),
                location=race_info.get('location'),
                win_time=race_info.get('win_time'),
                penalty=race_info.get('penalty'),
                race_category=race_info.get('race_category'),
                total_starters=race_info.get('total_starters'),
                total_finishers=race_info.get('total_finishers'),
                start_altitude=race_info.get('start_altitude'),
                finish_altitude=race_info.get('finish_altitude'),
                length=race_info.get('length'),
                gates1=race_info.get('gates1'),
                gates2=race_info.get('gates2'),
                turning_gates1=race_info.get('turning_gates1'),
                turning_gates2=race_info.get('turning_gates2'),
                homologation=race_info.get('homologation'),
                nation=race_info.get('nation'),
                gender=race_info.get('gender')
            )
            
            self.session.add(race)
            self.session.flush()  # Get the ID
            logger.info(f"Created new race: {race_info.get('race_name')} (FIS DB ID: {fis_db_id})")
            return race
            
        except Exception as e:
            logger.error(f"Error creating race: {e}")
            return None
    
    def _save_race_results(self, race: Race, results: List[Dict[str, Any]]) -> int:
        """Save race results to database.
        
        Args:
            race: Race object, already persisted (has id)
            results: List of race result dictionaries
            
        Returns:
            int: Number of results saved
        """
        if not results:
            return 0
        
        saved_count = 0
        
        for result_data in results:
            try:
                # Validate athlete exists (should exist from points list ingestion)
                athlete = self._get_fis_athlete(result_data.get('athlete_fis_db_id'))
                if not athlete:
                    logger.error(f"FIS Raceid {race.fis_db_id}: Could not find athlete with FIS DB ID {result_data.get('athlete_fis_db_id')}"
                                 f" for {result_data.get('athlete_name')}")
                    continue
                
                # Create race result linked to the race
                race_result = RaceResult(
                    race_id=race.id,
                    athlete_id=athlete.id,
                    points=result_data.get('points'),
                    rank=result_data.get('rank'),
                    racer_time=result_data.get('racer_time'),
                    result=result_data.get('result'),
                    run1_time=result_data.get('run1_time'),
                    run2_time=result_data.get('run2_time')
                )
                
                self.session.add(race_result)
                saved_count += 1
                
            except IntegrityError as e:
                logger.error(f"Integrity error saving result: {e}")
                self.session.rollback()
            except Exception as e:
                logger.error(f"Error saving result: {e}")
                self.session.rollback()
        
        try:
            self.session.commit()
            logger.info(f"Successfully saved {saved_count} race results for race {race.fis_db_id}")
        except Exception as e:
            logger.error(f"Error committing results: {e}")
            self.session.rollback()
            saved_count = 0
            
        return saved_count
    
    @staticmethod
    def add_arguments_to_parser(parser: argparse.ArgumentParser) -> None:
        """Add arguments to parser.
        
        Args:
            parser: ArgumentParser to add arguments to
        """
        parser.add_argument('--race-category', type=str, choices=[c.value for c in FisCategory],
                           help='Filter by race category')
        parser.add_argument('--season', type=int,
                            help='Season to scrape (e.g., 2025); defaults to current season.')
        parser.add_argument('--race-id', type=int, help='Scrape specific race by ID')
        parser.add_argument('--discover-only', action='store_true', 
                           help='Only discover races, don\'t scrape results')
        parser.add_argument('--verbose', action='store_true',
                           help='Verbose logging')
        parser.add_argument('--very-verbose', action='store_true',
                           help='Very verbose logging')

def _get_argument_parser() -> argparse.ArgumentParser:
    """Get command line argument parser for race results scraper.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(description='Scrape FIS race results')
    RaceResultsScraper.add_arguments_to_parser(parser)
    return parser

def main(race_category: Optional[str] = None,
         race_id: Optional[int] = None,
         season: Optional[int] = None,
         discover_only: bool = False,
         verbose: bool = False,
         very_verbose: bool = False) -> None:
    """Main function for race results scraping.
    
    Args:
        race_category: Race category filter
        race_id: Specific race ID to scrape
        discover_only: Only discover races, don't scrape results
        verbose: Enable verbose logging
    """
    if verbose:
        logger.setLevel(logging.INFO)
    elif very_verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)
    
    scraper = RaceResultsScraper()

    if not season:
        logger.info(f"No season provided, using current (season {scraper.get_current_season()})")
        season = scraper.get_current_season()

    if race_category:
        events = scraper.find_events_by_category(race_category, season)
        recorded_races, no_additional_results, errors = scraper.process_events(events, discover_only)
        print(f"Recorded {recorded_races} races, {no_additional_results} races with no additional results, {errors} errors")
        return

    if race_id:
        # Scrape specific race
        logger.info(f"Scraping race {race_id}")
        race_info, results = scraper.scrape_race_results(race_id)
        if race_info and results:
            saved_count = scraper.record_race(race_info, results)
            logger.info(f"Saved {saved_count} results for race {race_id}")
        else:
            logger.warning(f"No results found for race {race_id}")
    else:
        # Discover and scrape races
        logger.info(f"Discovering all events for season {season}")
        events = scraper.find_events_by_season(season)
        logger.info(f"Found {len(events)} events")
        recorded_races, no_additional_results, errors = scraper.process_events(events, discover_only)
        print(f"Recorded {recorded_races} races, {no_additional_results} races with no additional results, {errors} errors")
        return

if __name__ == "__main__":
    parser = _get_argument_parser()
    args = parser.parse_args()
    
    main(
        race_category=args.race_category,
        race_id=args.race_id,
        season=args.season,
        discover_only=args.discover_only,
        verbose=args.verbose,
        very_verbose=args.very_verbose
    ) 