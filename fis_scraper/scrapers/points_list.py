import requests
import re
import os
import logging

from typing import List, Dict, Optional, Tuple, Union
from sqlalchemy.orm import Session
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4 import Tag
from datetime import datetime, date
import pandas as pd

from ..database.connection import get_session
from ..database.models import PointsList, Athlete, AthletePoints, Gender

# Add logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PointsListScraper:
    """Scraper for FIS points lists from the FIS website.
    
    This class handles downloading and processing FIS points lists, including
    parsing athlete information and their points/rankings across different disciplines.
    """
    
    BASE_URL: str = "https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html"
    DATA_URL: str = "https://data.fis-ski.com"
    
    def __init__(self) -> None:
        """Initialize the PointsListScraper with a database session."""
        self.session: Session = get_session()
        
    def get_points_lists(self) -> List[Dict[str, Union[str, date]]]:
        """Scrape the FIS points lists page and return list of available points lists.
        
        Returns:
            List[Dict[str, Union[str, date]]]: List of dictionaries containing points list information:
                - sectorcode: Sector code (e.g., "AL")
                - seasoncode: Season code (e.g., "2023")
                - listid: List ID (e.g., "83"); None if Base List for given season code
                - name: Name of the points list
                - valid_from: Start date of validity period
                - valid_to: End date of validity period
                - excel_url: URL to download the Excel file
        """
        response = requests.get(self.BASE_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        points_lists = []

        for row in soup.find_all("div", {"class":"container g-xs-24"}):
            points_lists.append(self._parse_list_row(row))
        
        return points_lists
    
    def _parse_list_row(self, row: Tag ) -> Dict[str, Union[str, date]]:
        """Parse a single points list row into a dictionary.
        
        Args:
            row: BeautifulSoup element representing a points list row
            
        Returns:
            Dict[str, Union[str, date]]: Dictionary containing points list information
        """
        # NB: Javascript onclick parameters are encoded in the onclick attribute eg
        # onclick="export_fispointslist('AL','2023/24','83');"
        linkparams = re.compile(r"fct_export_fispointslist_xlsx\('(\w\w)','(\d\d\d\d)','(\d*)'\)")

        link = row.find("a", {"title": "Export the full list in Excel"})
        matches = linkparams.match(link['onclick'])
        list_title = self._parse_title(row.text)

        return {
            'sectorcode': matches[1],
            'seasoncode': matches[2],
            'listid': matches[3],
            'name': list_title[0],
            'valid_from': list_title[1],
            'valid_to': list_title[2],
            'excel_url': self._get_list_url(matches[1], matches[2], matches[3])
        }

    def _parse_title(self, text: str) -> Tuple[str, Optional[date], Optional[date]]:
        """Parse title string into name, valid_from, and valid_to.
        
        Args:
            text: String containing title information
            
        Returns:
            Tuple[str, Optional[date], Optional[date]]: Tuple of (name, valid_from, valid_to)
        """
        # First try to match regular points list
        points_list_regex = r"(\d.+ FIS points list \d\d\d\d.\d\d).+(\d\d-\d\d-\d\d\d\d).+(\d\d-\d\d-\d\d\d\d)"
        points_list_match = re.search(points_list_regex, text, re.DOTALL + re.IGNORECASE)
        if points_list_match:
            return points_list_match[1], self._parse_date(points_list_match[2]), self._parse_date(points_list_match[3])
            
        # If not a regular points list, try to match base list
        base_list_regex = r"(Internal Base list|Base List) (\d\d\d\d)"
        base_list_match = re.search(base_list_regex, text, re.DOTALL + re.IGNORECASE)
        if base_list_match:
            return f"{base_list_match[1]} {base_list_match[2]}", None, None
            
        return None, None, None
        
    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string into date object.
        
        Args:
            date_str: String in format "DD-MM-YYYY"
            
        Returns:
            date: Date object
        """
        if date_str:
            try:
                return datetime.strptime(date_str, '%d-%m-%Y').date()
            except ValueError:
                return None
        else:
            return None

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
        breakpoint()
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
    
    def _get_list_url(self, sectorcode: "AL", seasoncode, listid) -> str:
        """Get the URL for a specific points list.
        
        Args:
            sectorcode: Sector code (e.g., "AL")
            seasoncode: Season code (e.g., "2023/24")
            listId: List ID (e.g., "83"); None will return Base List for given season code
        """
        url = f"{self.DATA_URL}/fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html?export_xlsx=true&sectorcode={sectorcode}&seasoncode={seasoncode}"
        if listid:
            url += f"&listid={listid}"
        return url

    def _extract_season(self, name: str) -> Optional[str]:
        """Extract season from points list name.
        
        Args:
            name: Points list name string
            
        Returns:
            Optional[str]: Season string (e.g., "2023/24") or None if not found
        """
        match = re.search(r'(\d{4}/\d{2})', name)
        return match.group(1) if match else None 

def main():
    """Main entry point for the points list scraper."""
    try:
        logger.info("Starting FIS points list scraper")
        scraper = PointsListScraper()
        
        # Get available points lists
        logger.info("Fetching available points lists")
        points_lists = scraper.get_points_lists()
        logger.info(f"Found {len(points_lists)} points lists")
        breakpoint()
        # Process each points list
        for i, points_list in enumerate(points_lists, 1):
            logger.info(f"Processing points list {i}/{len(points_lists)}: {points_list['name']}")
            success = scraper.download_and_process_points_list(points_list)
            if success:
                logger.info(f"Successfully processed points list: {points_list['name']}")
            else:
                logger.error(f"Failed to process points list: {points_list['name']}")
        
        logger.info("Points list scraping completed")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 