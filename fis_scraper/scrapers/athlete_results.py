import requests
from bs4 import BeautifulSoup
from datetime import datetime
from ..database.connection import get_session
from ..database.models import Athlete, RaceResult, Discipline, Gender

class AthleteResultsScraper:
    BASE_URL = "https://www.fis-ski.com/DB/alpine-skiing/biographies.html"
    
    def __init__(self):
        self.session = get_session()
    
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
    
    def _parse_result_row(self, cells, fis_id):
        """Parse a single result row from the table."""
        try:
            date_str = cells[0].text.strip()
            race_date = datetime.strptime(date_str, '%d.%m.%Y').date()
            
            discipline = self._parse_discipline(cells[1].text.strip())
            if not discipline:
                return None
            
            race_name = cells[2].text.strip()
            location = cells[3].text.strip()
            rank = int(cells[4].text.strip())
            points = float(cells[5].text.strip()) if cells[5].text.strip() else None
            
            # Get or create athlete
            athlete = self.session.query(Athlete).filter_by(fis_id=fis_id).first()
            if not athlete:
                # Get athlete details from the page
                details = self._parse_athlete_details(BeautifulSoup(requests.get(
                    self.BASE_URL, 
                    params={'mi': 'menu-athletes', 'fiscode': fis_id}
                ).text, 'html.parser'))
                
                athlete = Athlete(
                    fis_id=fis_id,
                    name=details.get('name', ''),
                    country=details.get('nation_code', ''),
                    nation_code=details.get('nation_code', ''),
                    gender=details.get('gender', Gender.M),
                    birth_date=details.get('birth_date'),
                    birth_year=details.get('birth_year'),
                    ski_club=details.get('ski_club', ''),
                    national_code=details.get('national_code', '')
                )
                self.session.add(athlete)
                self.session.flush()
            
            # Create result entry
            result = RaceResult(
                athlete_id=athlete.id,
                race_date=race_date,
                discipline=discipline,
                points=points,
                rank=rank,
                race_name=race_name,
                location=location
            )
            
            self.session.add(result)
            self.session.commit()
            
            return result
            
        except (ValueError, IndexError) as e:
            print(f"Error parsing result row: {e}")
            return None
    
    def _parse_discipline(self, discipline_str):
        """Convert discipline string to Discipline enum."""
        discipline_map = {
            'SL': Discipline.SL,
            'GS': Discipline.GS,
            'SG': Discipline.SG,
            'DH': Discipline.DH
        }
        return discipline_map.get(discipline_str) 