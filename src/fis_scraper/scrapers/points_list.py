import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from ..database.connection import get_session
from ..database.models import PointsList, Athlete, AthletePoints, Gender
import re
import os
from urllib.parse import urljoin

class PointsListScraper:
    BASE_URL = "https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html"
    
    def __init__(self):
        self.session = get_session()
        
    def get_points_lists(self):
        """Scrape the FIS points lists page and return list of available points lists."""
        response = requests.get(self.BASE_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        points_lists = []
        for row in soup.find_all('tr'):
            if 'FIS points list' in row.text:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    name = cells[0].text.strip()
                    dates = cells[1].text.strip()
                    links = row.find_all('a', href=True)
                    excel_link = next((link['href'] for link in links if 'Excel' in link.text), None)
                    
                    if excel_link and dates:
                        valid_from, valid_to = self._parse_dates(dates)
                        points_lists.append({
                            'name': name,
                            'valid_from': valid_from,
                            'valid_to': valid_to,
                            'excel_url': urljoin(self.BASE_URL, excel_link)
                        })
        
        return points_lists
    
    def _parse_dates(self, date_str):
        """Parse date string into valid_from and valid_to dates."""
        try:
            # Format: DD-MM-YYYY - DD-MM-YYYY
            parts = date_str.split(' - ')
            if len(parts) == 2:
                valid_from = datetime.strptime(parts[0].strip(), '%d-%m-%Y').date()
                valid_to = datetime.strptime(parts[1].strip(), '%d-%m-%Y').date()
                return valid_from, valid_to
        except (ValueError, AttributeError):
            pass
        return None, None
    
    def download_and_process_points_list(self, points_list_data):
        """Download and process a single points list."""
        try:
            # Download Excel file
            response = requests.get(points_list_data['excel_url'])
            if response.status_code != 200:
                return False
            
            # Save temporary file
            temp_file = 'temp_points.xlsx'
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            # Read Excel file
            df = pd.read_excel(temp_file)
            
            # Process data and save to database
            self._save_points_list(points_list_data, df)
            
            # Clean up
            os.remove(temp_file)
            return True
            
        except Exception as e:
            print(f"Error processing points list: {e}")
            return False
    
    def _save_points_list(self, points_list_data, df):
        """Save points list data to database."""
        # Create PointsList entry
        points_list = PointsList(
            publication_date=datetime.now().date(),
            valid_from=points_list_data['valid_from'],
            valid_to=points_list_data['valid_to'],
            season=self._extract_season(points_list_data['name'])
        )
        self.session.add(points_list)
        self.session.flush()
        
        # Process each row
        for _, row in df.iterrows():
            try:
                # Create or update athlete
                athlete = self.session.query(Athlete).filter_by(fis_id=row['FIS Code']).first()
                if not athlete:
                    # Extract birth year from birth date if available
                    birth_year = None
                    if 'Birth Date' in row and pd.notna(row['Birth Date']):
                        try:
                            birth_date = pd.to_datetime(row['Birth Date'])
                            birth_year = birth_date.year
                        except:
                            pass
                    
                    athlete = Athlete(
                        fis_id=row['FIS Code'],
                        name=row['Name'],
                        country=row['Nation'],
                        nation_code=row.get('Nation Code', row['Nation'][:3].upper()),
                        gender=Gender.M if row.get('Gender', 'M') == 'M' else Gender.F,
                        birth_date=pd.to_datetime(row['Birth Date']).date() if 'Birth Date' in row and pd.notna(row['Birth Date']) else None,
                        birth_year=birth_year,
                        ski_club=row.get('Ski Club', ''),
                        national_code=row.get('National Code', '')
                    )
                    self.session.add(athlete)
                    self.session.flush()
                
                # Create athlete points entry
                athlete_points = AthletePoints(
                    athlete_id=athlete.id,
                    points_list_id=points_list.id,
                    sl_points=row.get('SL', None),
                    gs_points=row.get('GS', None),
                    sg_points=row.get('SG', None),
                    dh_points=row.get('DH', None),
                    sl_rank=row.get('SLpos', None),
                    gs_rank=row.get('GSpos', None),
                    sg_rank=row.get('SGpos', None),
                    dh_rank=row.get('DHpos', None)
                )
                self.session.add(athlete_points)
                
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        self.session.commit()
    
    def _extract_season(self, name):
        """Extract season from points list name."""
        match = re.search(r'(\d{4}/\d{2})', name)
        return match.group(1) if match else None 