import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import pandas as pd
from ..database.connection import get_session
from ..database.models import PointsList, Athlete, AthletePoints, Gender
import re
import os
from urllib.parse import urljoin
from typing import List, Dict, Optional, Tuple, Union
from sqlalchemy.orm import Session

class PointsListScraper:
    """Scraper for FIS points lists from the FIS website.
    
    This class handles downloading and processing FIS points lists, including
    parsing athlete information and their points/rankings across different disciplines.
    """
    
    BASE_URL: str = "https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html"
    
    def __init__(self) -> None:
        """Initialize the PointsListScraper with a database session."""
        self.session: Session = get_session()
        
    def get_points_lists(self) -> List[Dict[str, Union[str, date]]]:
        """Scrape the FIS points lists page and return list of available points lists.
        
        Returns:
            List[Dict[str, Union[str, date]]]: List of dictionaries containing points list information:
                - name: Name of the points list
                - valid_from: Start date of validity period
                - valid_to: End date of validity period
                - excel_url: URL to download the Excel file
        """
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
    
    def _parse_dates(self, date_str: str) -> Tuple[Optional[date], Optional[date]]:
        """Parse date string into valid_from and valid_to dates.
        
        Args:
            date_str: String in format "DD-MM-YYYY - DD-MM-YYYY"
            
        Returns:
            Tuple[Optional[date], Optional[date]]: Tuple of (valid_from, valid_to) dates
        """
        try:
            parts = date_str.split(' - ')
            if len(parts) == 2:
                valid_from = datetime.strptime(parts[0].strip(), '%d-%m-%Y').date()
                valid_to = datetime.strptime(parts[1].strip(), '%d-%m-%Y').date()
                return valid_from, valid_to
        except (ValueError, AttributeError):
            pass
        return None, None
    
    def download_and_process_points_list(self, points_list_data: Dict[str, Union[str, date]]) -> bool:
        """Download and process a single points list.
        
        Args:
            points_list_data: Dictionary containing points list information
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            response = requests.get(points_list_data['excel_url'])
            if response.status_code != 200:
                return False
            
            temp_file = 'temp_points.xlsx'
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_excel(temp_file)
            self._save_points_list(points_list_data, df)
            
            os.remove(temp_file)
            return True
            
        except Exception as e:
            print(f"Error processing points list: {e}")
            return False
    
    def _save_points_list(self, points_list_data: Dict[str, Union[str, date]], df: pd.DataFrame) -> None:
        """Save points list data to database.
        
        Args:
            points_list_data: Dictionary containing points list information
            df: DataFrame containing the points list data
        """
        points_list = PointsList(
            publication_date=datetime.now().date(),
            valid_from=points_list_data['valid_from'],
            valid_to=points_list_data['valid_to'],
            season=self._extract_season(points_list_data['name'])
        )
        self.session.add(points_list)
        self.session.flush()
        
        for _, row in df.iterrows():
            try:
                athlete = self.session.query(Athlete).filter_by(fis_id=row['FIS Code']).first()
                if not athlete:
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
    
    def _extract_season(self, name: str) -> Optional[str]:
        """Extract season from points list name.
        
        Args:
            name: Points list name string
            
        Returns:
            Optional[str]: Season string (e.g., "2023/24") or None if not found
        """
        match = re.search(r'(\d{4}/\d{2})', name)
        return match.group(1) if match else None 