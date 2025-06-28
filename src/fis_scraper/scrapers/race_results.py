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

from .fis_constants import DATA_URL, FisSector, FisCategory
from ..database.connection import get_session
from ..database.models import Athlete, RaceResult, Discipline, Gender, PointsList
from .points_list import PointsListScraper

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
    
    CATEGORY_URL: str = f"{DATA_URL}/fis_events/ajax/calendarfunctions/get_select_category.html"
    RESULTS_URL: str = "https://data.fis-ski.com/alpine-skiing/results.html"
    
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
            'category': category,
            'season': season
        }
        response = requests.get(self.CATEGORY_URL, params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        race_links = soup.find_all('a', {'class': 'pr-1 g-lg-1 g-md-1 g-sm-2 hidden-xs justify-left'})
        return [link.get('href') for link in race_links]
    
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
    
    def _parse_race_id_from_link(self, link: str) -> int:
        """Parse race ID from a race link."""
        match = re.search(r'raceid=(\d+)', link)
        if match:
            return int(match.group(1))
        return None

    def discover_races(self, 
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None,
                      discipline: Optional[Discipline] = None,
                      gender: Optional[Gender] = None,
                      race_category: Optional[str] = None) -> List[Dict[str, Any]]:
        """AI-generated and BROKEN.
        Need to decide fix vs discard.
        Discover available races based on filters.
        
        Args:
            start_date: Start date for race discovery
            end_date: End date for race discovery
            discipline: Filter by specific discipline
            gender: Filter by gender
            race_category: Filter by race category (WC, EC, FIS, etc.)
            
        Returns:
            List[Dict[str, Any]]: List of race information dictionaries
        """
        races = []
        
        # Build search parameters
        params = {
            'sectorcode': 'AL',
            'limit': 1000
        }
        
        if start_date:
            params['date_from'] = start_date.strftime('%d.%m.%Y')
        if end_date:
            params['date_to'] = end_date.strftime('%d.%m.%Y')
        if discipline:
            params['discipline'] = discipline.value
        if gender:
            params['gender'] = gender.value
                  
        try:
            response = requests.get(self.RESULTS_URL, params=params)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            race_links = soup.find_all('a', href=re.compile(r'raceid='))
            
            for link in race_links:
                race_info = self._parse_race_link(link)
                if race_info and self._matches_filters(race_info, discipline, gender, race_category):
                    races.append(race_info)
                    
        except requests.RequestException as e:
            logger.error(f"Error discovering races: {e}")
            
        return races
    
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
    
    def _matches_filters(self, 
                        race_info: Dict[str, Any],
                        discipline: Optional[Discipline] = None,
                        gender: Optional[Gender] = None,
                        race_category: Optional[str] = None) -> bool:
        """Check if race matches the specified filters.
        
        Args:
            race_info: Race information dictionary
            discipline: Discipline filter
            gender: Gender filter
            race_category: Race category filter
            
        Returns:
            bool: True if race matches all filters
        """
        if discipline and race_info.get('discipline') != discipline:
            return False
        # Additional filter logic can be added here
        return True
    
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
        logger.info(f"Auto-ingesting points list for {race_date}")
        success = points_scraper.download_and_process_points_list(valid_lists[0])
        if success:
            return self._get_points_list_for_date(race_date)
        return None
    
    def scrape_race_results(self, race_id: int) -> List[Dict[str, Any]]:
        """Scrape complete results for a specific race using the real FIS HTML structure.
        
        Args:
            race_id: FIS race ID
        
        Returns:
            List[Dict[str, Any]]: List of racer results
        """
        results: List[Dict[str, Any]] = []
        try:
            # Get race results page
            race_url = f"{DATA_URL}/alpine-skiing/results.html?raceid={race_id}"
            response = requests.get(race_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Only support real FIS HTML structure
            fis_results_div = soup.find('div', id='events-info-results', class_='table__body')
            if not fis_results_div:
                logger.warning(f"No results table found for race {race_id}")
                return results
            rows = fis_results_div.find_all('a', class_='table-row')
            race_info = self._parse_fis_race_header(soup)
            if race_info.get('race_date'):
                points_list = self._ensure_points_list_for_date(race_info['race_date'])
                if not points_list:
                    logger.error(f"Cannot scrape race {race_id}: No valid points list for date {race_info['race_date']}")
                    return results
            for row in rows:
                result = self._parse_fis_table_row(row, race_info, race_id)
                if result:
                    results.append(result)
            if results:
                total_starters = self._calculate_total_starters(results)
                total_finishers = self._calculate_total_finishers(results)
                for result in results:
                    result['total_starters'] = total_starters
                    result['total_finishers'] = total_finishers
        except requests.RequestException as e:
            logger.error(f"Error scraping race {race_id}: {e}")
        return results

    def _parse_fis_race_header(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse race header information from real FIS HTML structure.
        
        Args:
            soup: BeautifulSoup object of race page
        
        Returns:
            Dict[str, Any]: Race header information
        """
        race_info: Dict[str, Any] = {}
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
            import re
            m = re.search(r'competitionCodex":(\d+)', script.text)
            if m:
                codex = int(m.group(1))
        if not codex:
            codex_span = soup.find('span', string=lambda t: t and 'CODEX:' in t)
            if codex_span:
                m = re.search(r'CODEX:\s*(\d+)', codex_span.text)
                if m:
                    codex = int(m.group(1))
        if codex:
            race_info['race_codex'] = codex
        # TODO: Parse date, discipline, etc. if available
        # For now, leave as None or parse from elsewhere if needed
        return race_info

    def _parse_fis_table_row(self, row: Tag, race_info: Dict[str, Any], race_id: int) -> Optional[Dict[str, Any]]:
        """Parse a single FIS result row from <a class="table-row">, supporting real FIS HTML structure.
        
        Args:
            row: BeautifulSoup Tag for the row
            race_info: Race header info
            race_id: FIS race ID
        
        Returns:
            Optional[Dict[str, Any]]: Parsed result data
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
            for d in divs:
                if 'g-lg-6' in d.get('class', []) and 'bold' in d.get('class', []):
                    athlete_name = d.get_text(strip=True)
                    break
            # Nation
            nation = None
            for d in divs:
                nation_span = d.find('span', class_='country__name-short')
                if nation_span:
                    nation = nation_span.get_text(strip=True)
                    break
            # FIS DB ID (athlete's ID)
            href = row.get('href', '')
            athlete_fis_db_id = None
            import re
            m = re.search(r'competitorid=(\d+)', href)
            if m:
                athlete_fis_db_id = int(m.group(1))
            # Times: look for all divs with class justify-right bold hidden-xs
            time_divs = [d for d in divs if 'justify-right' in d.get('class', []) and 'bold' in d.get('class', []) and 'hidden-xs' in d.get('class', [])]
            run1_time = self._parse_time(time_divs[0].get_text(strip=True)) if len(time_divs) > 0 else None
            run2_time = self._parse_time(time_divs[1].get_text(strip=True)) if len(time_divs) > 1 else None
            # Win time (total time): blue bold
            win_time = None
            for d in divs:
                if 'justify-right' in d.get('class', []) and 'blue' in d.get('class', []) and 'bold' in d.get('class', []) and 'hidden-sm' in d.get('class', []) and 'hidden-xs' in d.get('class', []):
                    win_time = self._parse_time(d.get_text(strip=True))
                    break
            # Penalty (FIS Points): find the correct justify-right div (not bold, not gray, not hidden-xs)
            penalty = None
            for d in divs:
                classes = d.get('class', [])
                if (
                    'justify-right' in classes
                    and 'bold' not in classes
                    and 'gray' not in classes
                    and 'hidden-xs' not in classes
                ):
                    penalty_text = d.get_text(strip=True)
                    if self._is_float(penalty_text):
                        penalty = float(penalty_text)
                        break
            # Compose racer_time for statistics
            racer_time = None
            if win_time is not None:
                racer_time = win_time
            elif run1_time is not None and run2_time is not None:
                racer_time = run1_time + run2_time
            elif run1_time is not None:
                racer_time = run1_time
            result = {
                'rank': rank,
                'athlete_name': athlete_name,
                'athlete_fis_db_id': athlete_fis_db_id,
                'fis_db_id': race_id,  # Store the race ID here
                'nation': nation,
                'run1_time': run1_time,
                'run2_time': run2_time,
                'win_time': win_time,
                'penalty': penalty,
                'racer_time': racer_time,
                **race_info
            }
            return result
        except Exception as e:
            logger.error(f"Error parsing FIS table row: {e}")
            return None
    
    def _parse_race_header(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Parse race header information.
        
        Args:
            soup: BeautifulSoup object of race page
            
        Returns:
            Dict[str, Any]: Race header information
        """
        race_info = {}
        
        # Extract race name
        title_element = soup.find('h1', class_='race-title')
        if title_element:
            race_info['race_name'] = title_element.get_text(strip=True)
        
        # Extract race date
        date_element = soup.find('div', class_='race-date')
        if date_element:
            try:
                date_str = date_element.get_text(strip=True)
                race_info['race_date'] = datetime.strptime(date_str, '%d.%m.%Y').date()
            except ValueError:
                pass
        
        # Extract location
        location_element = soup.find('div', class_='race-location')
        if location_element:
            race_info['location'] = location_element.get_text(strip=True)
        
        # Extract discipline
        discipline_element = soup.find('div', class_='race-discipline')
        if discipline_element:
            discipline_str = discipline_element.get_text(strip=True)
            race_info['discipline'] = self._parse_discipline(discipline_str)
        
        # Extract race category
        category_element = soup.find('div', class_='race-category')
        if category_element:
            race_info['race_category'] = category_element.get_text(strip=True)
        
        return race_info
    
    def _parse_result_row(self, row: Tag, race_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single result row.
        
        Args:
            row: BeautifulSoup Tag containing result row
            race_info: Race header information
            
        Returns:
            Optional[Dict[str, Any]]: Parsed result data
        """
        try:
            cells = row.find_all('td')
            if len(cells) < 6:  # Minimum expected columns
                return None
            
            # Parse rank/position
            rank_cell = cells[0]
            rank_text = rank_cell.get_text(strip=True)
            
            # Handle DNF, DNS, DSQ, etc.
            result = None
            rank = None
            if rank_text.isdigit():
                rank = int(rank_text)
            else:
                result = rank_text
                rank = None
            
            # Parse athlete information
            athlete_cell = cells[1]
            athlete_link = athlete_cell.find('a')
            if not athlete_link:
                return None
                
            athlete_name = athlete_link.get_text(strip=True)
            athlete_url = athlete_link.get('href', '')
            
            # Extract FIS DB ID from athlete URL
            fis_db_id = None
            if 'competitorid=' in athlete_url:
                fis_db_id_match = re.search(r'competitorid=(\d+)', athlete_url)
                if fis_db_id_match:
                    fis_db_id = int(fis_db_id_match.group(1))
            
            # Parse nation
            nation_cell = cells[2]
            nation = nation_cell.get_text(strip=True)
            
            # Parse times (handle both single and two-run races)
            run1_time = None
            run2_time = None
            racer_time = None
            
            if len(cells) >= 4:
                # Try to parse individual run times
                run1_cell = cells[3]
                run1_time = self._parse_time(run1_cell.get_text(strip=True))
                
                if len(cells) >= 5:
                    run2_cell = cells[4]
                    run2_time = self._parse_time(run2_cell.get_text(strip=True))
                    
                    # Final time is the sum of run times for two-run races
                    if run1_time and run2_time:
                        racer_time = run1_time + run2_time
                    elif run1_time:
                        racer_time = run1_time
                else:
                    # Single run race
                    racer_time = run1_time
            
            # Parse winner's time (if available)
            winner_time_cell = cells[-2] if len(cells) > 4 else None
            win_time = None
            if winner_time_cell:
                win_time = self._parse_time(winner_time_cell.get_text(strip=True))
            
            # Parse points
            points_cell = cells[-1] if len(cells) > 5 else None
            points = None
            if points_cell:
                points_text = points_cell.get_text(strip=True)
                if self._is_float(points_text):
                    points = float(points_text)
            
            # Parse result code with run information
            run_data = {'run1_time': run1_time, 'run2_time': run2_time}
            parsed_result = self._parse_result_code(result, run_data)
            
            return {
                'rank': rank,
                'result': parsed_result,
                'athlete_name': athlete_name,
                'fis_db_id': fis_db_id,
                'nation': nation,
                'racer_time': racer_time,
                'run1_time': run1_time,
                'run2_time': run2_time,
                'win_time': win_time,
                'points': points,
                'race_points': None,  # Will be parsed from results if available
                **race_info
            }
            
        except (ValueError, AttributeError, IndexError) as e:
            logger.error(f"Error parsing result row: {e}")
            return None
    
    def _calculate_total_starters(self, results: List[Dict[str, Any]]) -> int:
        """Calculate total starters based on FIS rules.
        
        Args:
            results: List of race result dictionaries
            
        Returns:
            int: Total number of starters
        """
        starters = 0
        
        for result in results:
            # Count if they have any time recorded
            if result.get('racer_time') or result.get('run1_time') or result.get('run2_time'):
                starters += 1
            # Count if they have DNF/DSQ results
            elif result.get('result') in ['DNF1', 'DNF2', 'DSQ1', 'DSQ2']:
                starters += 1
            # Count DNS2 only if they finished first run
            elif result.get('result') == 'DNS2' and result.get('run1_time'):
                starters += 1
        
        return starters
    
    def _calculate_total_finishers(self, results: List[Dict[str, Any]]) -> int:
        """Calculate total finishers (racers with final times).
        
        Args:
            results: List of race result dictionaries
            
        Returns:
            int: Total number of finishers
        """
        return sum(1 for result in results if result.get('racer_time') is not None)
    
    def _parse_result_code(self, result_text: str, run_data: Dict[str, Any]) -> Optional[str]:
        """Parse result text into standardized result codes.
        
        Args:
            result_text: Raw result text from the page
            run_data: Dictionary containing run time information
            
        Returns:
            Optional[str]: Standardized result code
        """
        if not result_text:
            return None
        
        result_text = result_text.upper().strip()
        
        # Check if this is a two-run race (has run1_time or run2_time fields)
        is_two_run_race = 'run1_time' in run_data or 'run2_time' in run_data
        
        if is_two_run_race:
            if 'DNS' in result_text:
                if run_data.get('run1_time') is not None:
                    return 'DNS2'  # Did not start second run
                else:
                    return 'DNS1'  # Did not start first run
            elif 'DNF' in result_text:
                if run_data.get('run1_time') is not None and run_data.get('run2_time') is None:
                    return 'DNF2'  # Did not finish second run
                else:
                    return 'DNF1'  # Did not finish first run
            elif 'DSQ' in result_text:
                if run_data.get('run1_time') is not None and run_data.get('run2_time') is None:
                    return 'DSQ2'  # Disqualified second run
                else:
                    return 'DSQ1'  # Disqualified first run
        
        # Handle single run races
        if 'DNS' in result_text:
            return 'DNS'
        elif 'DNF' in result_text:
            return 'DNF'
        elif 'DSQ' in result_text:
            return 'DSQ'
        
        return None
    
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
    
    def _parse_discipline(self, discipline_str: str) -> Optional[Discipline]:
        """Parse discipline string to Discipline enum.
        
        Args:
            discipline_str: Discipline string
            
        Returns:
            Optional[Discipline]: Discipline enum value
        """
        if not discipline_str:
            return None
            
        discipline_map = {
            'SL': Discipline.SL,
            'GS': Discipline.GS,
            'SG': Discipline.SG,
            'DH': Discipline.DH,
            'AC': Discipline.AC,
            'Slalom': Discipline.SL,
            'Giant Slalom': Discipline.GS,
            'Super-G': Discipline.SG,
            'Downhill': Discipline.DH,
            'Alpine Combined': Discipline.AC
        }
        
        return discipline_map.get(discipline_str.strip())
    
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
    
    def _get_athlete(self, fis_db_id: Optional[int], athlete_name: Optional[str], nation: Optional[str]) -> Optional[Athlete]:
        """Return an athlete by FIS DB ID or name/nation, or None."""
        if fis_db_id:
            athlete = self.session.query(Athlete).filter_by(fis_db_id=fis_db_id).first()
            if athlete:
                return athlete
        if athlete_name and nation:
            athlete = self.session.query(Athlete).filter_by(
                last_name=athlete_name.split()[-1] if ' ' in athlete_name else athlete_name,
                first_name=athlete_name.split()[0] if ' ' in athlete_name else '',
                nation_code=nation[:3].upper()
            ).first()
            if athlete:
                return athlete
        return None

    def _validate_athlete_exists(self, result_data: Dict[str, Any]) -> Optional[Athlete]:
        """Validate that athlete exists in database."""
        return self._get_athlete(
            result_data.get('athlete_fis_db_id'),
            result_data.get('athlete_name'),
            result_data.get('nation')
        )
    
    def _create_athlete_if_needed(self, result_data: Dict[str, Any]) -> Optional[Athlete]:
        """Create athlete record if needed (should be rare since points lists create athletes).
        
        Args:
            result_data: Race result data containing athlete information
            
        Returns:
            Optional[Athlete]: Athlete object if created, None otherwise
        """
        athlete_fis_db_id = result_data.get('athlete_fis_db_id')
        athlete_name = result_data.get('athlete_name')
        nation = result_data.get('nation')
        
        # Only create if we have sufficient information
        if athlete_fis_db_id and athlete_name and nation:
            try:
                name_parts = athlete_name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = ' '.join(name_parts[1:])
                else:
                    first_name = athlete_name
                    last_name = ''
                
                athlete = Athlete(
                    fis_id=0,  # Will need to be updated
                    fis_db_id=athlete_fis_db_id,
                    last_name=last_name,
                    first_name=first_name,
                    nation_code=nation[:3].upper(),
                    gender=Gender.M  # Default, would need to be determined
                )
                
                self.session.add(athlete)
                self.session.flush()  # Get the ID
                logger.warning(f"Created missing athlete: {athlete_name} (FIS DB ID: {athlete_fis_db_id})")
                return athlete
                
            except Exception as e:
                logger.error(f"Error creating athlete: {e}")
                return None
        
        return None
    
    def save_race_results(self, results: List[Dict[str, Any]]) -> int:
        """Save race results to database.
        
        Args:
            results: List of race result dictionaries
            
        Returns:
            int: Number of results saved
        """
        saved_count = 0
        
        for result_data in results:
            try:
                # Validate athlete exists (should exist from points list ingestion)
                athlete = self._validate_athlete_exists(result_data)
                if not athlete:
                    # Try to create athlete if missing (should be rare)
                    athlete = self._create_athlete_if_needed(result_data)
                    if not athlete:
                        logger.warning(f"Could not find or create athlete for {result_data.get('athlete_name')}")
                        continue
                
                # Create race result
                race_result = RaceResult(
                    athlete_id=athlete.id,
                    fis_db_id=result_data.get('fis_db_id'),  # Race ID
                    race_codex=result_data.get('race_codex'),
                    race_date=result_data['race_date'],
                    discipline=result_data['discipline'],
                    points=result_data.get('points'),
                    rank=result_data.get('rank'),
                    race_name=result_data.get('race_name'),
                    location=result_data.get('location'),
                    win_time=result_data.get('win_time'),
                    racer_time=result_data.get('racer_time'),
                    penalty=result_data.get('penalty'),
                    race_points=result_data.get('race_points'),
                    race_category=result_data.get('race_category'),
                    result=result_data.get('result'),
                    total_starters=result_data.get('total_starters'),
                    total_finishers=result_data.get('total_finishers')
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
            logger.info(f"Successfully saved {saved_count} race results")
        except Exception as e:
            logger.error(f"Error committing results: {e}")
            self.session.rollback()
            saved_count = 0
            
        return saved_count


def _get_argument_parser() -> argparse.ArgumentParser:
    """Get command line argument parser for race results scraper.
    
    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(description='Scrape FIS race results')
    parser.add_argument('--start-date', type=str, help='Start date (DD.MM.YYYY)')
    parser.add_argument('--end-date', type=str, help='End date (DD.MM.YYYY)')
    parser.add_argument('--discipline', type=str, choices=['SL', 'GS', 'SG', 'DH', 'AC'],
                       help='Filter by discipline')
    parser.add_argument('--race-id', type=int, help='Scrape specific race by ID')
    parser.add_argument('--discover-only', action='store_true', 
                       help='Only discover races, don\'t scrape results')
    parser.add_argument('--verbose', action='store_true',
                       help='Verbose logging')
    
    return parser


def main(start_date: Optional[str] = None,
         end_date: Optional[str] = None,
         discipline: Optional[str] = None,
         race_id: Optional[int] = None,
         discover_only: bool = False,
         verbose: bool = False) -> None:
    """Main function for race results scraping.
    
    Args:
        start_date: Start date string (DD.MM.YYYY)
        end_date: End date string (DD.MM.YYYY)
        discipline: Discipline filter
        race_id: Specific race ID to scrape
        discover_only: Only discover races, don't scrape results
        verbose: Enable verbose logging
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scraper = RaceResultsScraper()
    
    # Parse dates
    parsed_start_date = None
    parsed_end_date = None
    
    if start_date:
        try:
            parsed_start_date = datetime.strptime(start_date, '%d.%m.%Y').date()
        except ValueError:
            logger.error(f"Invalid start date format: {start_date}")
            return
    
    if end_date:
        try:
            parsed_end_date = datetime.strptime(end_date, '%d.%m.%Y').date()
        except ValueError:
            logger.error(f"Invalid end date format: {end_date}")
            return
    
    # Parse discipline
    parsed_discipline = None
    if discipline:
        discipline_map = {
            'SL': Discipline.SL,
            'GS': Discipline.GS,
            'SG': Discipline.SG,
            'DH': Discipline.DH,
            'AC': Discipline.AC
        }
        parsed_discipline = discipline_map.get(discipline)
    
    if race_id:
        # Scrape specific race
        logger.info(f"Scraping race {race_id}")
        results = scraper.scrape_race_results(race_id)
        if results:
            saved_count = scraper.save_race_results(results)
            logger.info(f"Saved {saved_count} results for race {race_id}")
        else:
            logger.warning(f"No results found for race {race_id}")
    else:
        # Discover and scrape races
        logger.info("Discovering races...")
        races = scraper.discover_races(
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            discipline=parsed_discipline
        )
        
        logger.info(f"Found {len(races)} races")
        
        if discover_only:
            for race in races:
                print(f"Race ID: {race['race_id']}, Date: {race['race_date']}, "
                      f"Location: {race['location']}, Discipline: {race['discipline']}")
        else:
            # Scrape all discovered races
            total_saved = 0
            for race in races:
                logger.info(f"Scraping race {race['race_id']} at {race['location']}")
                results = scraper.scrape_race_results(race['race_id'])
                if results:
                    saved_count = scraper.save_race_results(results)
                    total_saved += saved_count
                    logger.info(f"Saved {saved_count} results for race {race['race_id']}")
            
            logger.info(f"Total results saved: {total_saved}")


if __name__ == "__main__":
    parser = _get_argument_parser()
    args = parser.parse_args()
    
    main(
        start_date=args.start_date,
        end_date=args.end_date,
        discipline=args.discipline,
        race_id=args.race_id,
        discover_only=args.discover_only,
        verbose=args.verbose
    ) 