import requests
from bs4 import BeautifulSoup, Tag
from datetime import datetime
from typing import Dict, Any
import logging
import re

from ..database.connection import get_session
from ..database.models import Athlete, RaceResult, Discipline, Gender

class AthleteResultsScraper:
    BASE_URL: str = "https://www.fis-ski.com/DB/alpine-skiing/biographies.html"
    DATA_URL: str = "https://data.fis-ski.com"
    ATHLETE_BIO_URL: str = "https://www.fis-ski.com/DB/general/athlete-biography.html"
    ATHLETE_RESULTS_URL: str = f"{DATA_URL}/fis_athletes/ajax/athletesfunctions/load_results.html"
    
    def __init__(self):
        self.session = get_session()

    def _get_athlete_results_url(self, fis_db_id: int) -> str:
        """
        Get the URL for the athlete results page.
        
        Args:
            fis_db_id: FIS DB ID of the athlete

        Returns:
            str: URL for the athlete results page
        """
        return f"{self.ATHLETE_RESULTS_URL}?sectorcode=AL&competitorid={fis_db_id}&limit=1000"
    
    def get_athlete_results(self, fis_id):
        """Scrape results for a specific athlete."""
        params = {'mi': 'menu-athletes', 'fiscode': fis_id}
        response = requests.get(self.BASE_URL, params=params)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find athlete details
        athlete_details = self._parse_athlete_details(soup)
        
        # Find results table
        results_table = soup.find('table', {'class': 'table'})
        if not results_table:
            return []
        
        results = []
        for row in results_table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 7:
                try:
                    result = self._parse_result_row(cells, fis_id)
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
        
        return results
    
    def _parse_athlete_details(self, soup):
        """Parse athlete details from the biography page."""
        details = {}
        
        # Find the athlete info section
        info_section = soup.find('div', {'class': 'athlete-info'})
        if info_section:
            # Extract details from the info section
            for row in info_section.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    key = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    
                    if 'nation' in key:
                        details['nation_code'] = value
                    elif 'gender' in key:
                        details['gender'] = Gender.M if value.upper() == 'M' else Gender.F
                    elif 'birth date' in key:
                        try:
                            birth_date = datetime.strptime(value, '%d.%m.%Y').date()
                            details['birth_date'] = birth_date
                            details['birth_year'] = birth_date.year
                        except ValueError:
                            pass
                    elif 'ski club' in key:
                        details['ski_club'] = value
                    elif 'national code' in key:
                        details['national_code'] = value
        
        return details
    
    def _parse_result_row(self, html_tag: Tag) -> Dict[str, Any]:
        """
        Parse a single result row from the HTML 'table'.

        Args:
            html_tag: BeautifulSoup Tag object containing the result row

        Returns:
            Dict[str, Any]: Dictionary of result row data
        """
        try:
            # Extract URL and IDs from the href
            href = html_tag.get('href', '')
            competitorid = int(href.split('competitorid=')[1].split('&')[0]) if 'competitorid=' in href else None
            fis_race_id = int(href.split('raceid=')[1]) if 'raceid=' in href else None
            race_url = href
            
            # Get the container div
            container = html_tag.find('div', {'class': 'container'})
            if not container:
                return None
                
            # Find all divs with text content
            cells = container.find_all('div', recursive=False)
            
            # Parse date - handle both hyphen and dot separators
            date_str = cells[0].text.strip()
            try:
                race_date = datetime.strptime(date_str, '%d-%m-%Y').date()
            except ValueError:
                try:
                    race_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                except ValueError:
                    print(f"Error parsing date: {date_str}")
                    return None
            
            # Parse location and discipline robustly
            discipline_str = None
            gray_div = container.find('div', {'class': 'gray'})
            if gray_div:
                discipline_str = gray_div.text.strip()
            discipline = self._parse_discipline(discipline_str) if discipline_str else None
            
            location = None
            clip_div = container.find('div', {'class': 'clip-xs'})
            if clip_div:
                location = clip_div.text.strip()
            elif len(cells) > 1:
                location = cells[1].text.strip()
            
            # Parse nation
            nation_div = container.find('div', {'class': 'country'})
            nation = nation_div.find('span', {'class': 'country__name-short'}).text.strip() if nation_div else None
            
            # Parse race category: look for the first cell with text in ['FIS', 'NAC', 'Nor-Am Cup', etc.]
            race_category = None
            race_category_div = container.find('div',
                                               {'class': 'g-sm-3 g-md-5 g-lg-5 justify-left hidden-xs hidden-md-up'})
            if race_category_div:
                race_category = race_category_div.text.strip()
            
            # Parse results section (last cell with justify-right)
            results_div = None
            for div in cells[::-1]:
                if 'justify-right' in div.get('class', []):
                    results_div = div
                    break
            if results_div:
                result_divs = results_div.find_all('div', recursive=False)
                result_texts = [d.text.strip() for d in result_divs if d.text.strip() != '']
                rank = result_texts[0] if len(result_texts) > 0 else None
                points = float(result_texts[1]) if len(result_texts) > 1 and self._is_float(result_texts[1]) else None
                cup_points = int(result_texts[2]) if len(result_texts) > 2 and result_texts[2].isdigit() else None
            else:
                rank = None
                points = None
                cup_points = None
            
            # Handle DNF/DSQ/DNS and result field
            result = None
            if rank and not rank.isdigit():
                result = rank
                rank = None
            else:
                try:
                    rank = int(rank) if rank else None
                except ValueError:
                    rank = None
            
            # Return None for invalid rows
            if not ((discipline and location and fis_race_id) and (rank or result)):
                return None
            
            return {
                'race_date': race_date,
                'discipline': discipline,
                'location': location,
                'rank': rank,
                'result': result,
                'points': points,
                'race_category': race_category,
                'competitorid': competitorid,
                'fis_race_id': fis_race_id,
                'cup_points': cup_points,
                'nation': nation,
                'race_url': race_url
            }
            
        except (ValueError, IndexError, AttributeError) as e:
            logging.error(f"Error parsing result row: {e}")
            return None

    def _is_float(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _parse_discipline(self, discipline_str):
        """Convert discipline string to Discipline enum, handling both abbreviations and full names."""
        if not discipline_str:
            return None
        discipline_str = discipline_str.strip().lower()
        discipline_map = {
            'sl': Discipline.SL,
            'slalom': Discipline.SL,
            'gs': Discipline.GS,
            'giant slalom': Discipline.GS,
            'sg': Discipline.SG,
            'super-g': Discipline.SG,
            'dh': Discipline.DH,
            'downhill': Discipline.DH,
            'ac': Discipline.AC,
            'alpine combined': Discipline.AC,
        }
        return discipline_map.get(discipline_str) 