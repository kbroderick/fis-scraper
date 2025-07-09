import argparse
import os
from pprint import pprint
import requests
import re
import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple, Union
from sqlalchemy.orm import Session
from bs4 import BeautifulSoup
from bs4 import Tag
import pandas as pd
from pandas._libs.tslibs.parsing import DateParseError
from pandas.errors import ParserError

from .fis_constants import DATA_URL, FisSector
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
    
    POINTS_LISTS_URL: str = "https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html"
    DATA_FOLDER: str = "data" # local folder for file storage, lists in points_lists subfolder

    def __init__(self) -> None:
        """Initialize the PointsListScraper with a database session."""
        self.session: Session = get_session()

    def _get_filename_for_points_list(self, list_data: Dict[str, Union[str, date]]) -> str:
        """Get the filename for a points list.
        
        Args:
            list_data: Dictionary containing points list info

        Returns:
            str: Filename for the points list of form
            F{sectorcode}_{seasoncode}{listid} (e.g. FAL_202383 for the
            2023/24 alpine list with ID 83)
        """
        return f"F{list_data['sectorcode']}_{list_data['seasoncode']}{list_data['listid']}"
    
    def _get_filelocation_for_points_list(self, list_data: Dict[str, Union[str, date]]) -> str:
        """Get the file location for a points list.
        
        Args:
            list_data: Dictionary containing points list info

        Returns:
            str: File location for the points list of form
            "data/points_lists/FAL_202383.csv"
        """
        filename = self._get_filename_for_points_list(list_data)
        return f"{self.DATA_FOLDER}/points_lists/{filename}.csv"
        
    def get_points_lists(self, include_base_lists: bool = False) -> \
        List[Dict[str, Union[str, date]]]:
        """Scrape the FIS points lists page and return list of available
        points lists.

        Args:
            include_base_lists: Whether to include base lists in the
            returned list

        Returns:
            List[Dict[str, Union[str, date]]]: List of dictionaries
            containing points list information:
                - sectorcode: Sector code (e.g., "AL")
                - seasoncode: Season code (e.g., "2023")
                - listid: List ID (e.g., "83"); None if Base List for
                    given season code
                - name: Name of the points list
                - valid_from: Start date of validity period
                - valid_to: End date of validity period
                - csv_url: URL to download the CSV file
        """
        points_lists = []
 
        response = requests.get(self.POINTS_LISTS_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
       
        for row in soup.find_all("div", {"class":"container g-xs-24"}):
            list_data = self._parse_list_row(row)
            if list_data['valid_from'] is not None or include_base_lists:
                points_lists.append(list_data)
        return points_lists
    
    def _parse_list_row(self, row: Tag ) -> Dict[str, Union[str, date]]:
        """Parse a single points list row into a dictionary.
        
        Args:
            row: BeautifulSoup element representing a points list row
            
        Returns:
            Dict[str, Union[str, date]]: Dictionary containing points
            list information
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
            'csv_url': self._get_list_url(matches[1], matches[2], matches[3])
        }

    def _parse_title(self, text: str) -> Tuple[str, Optional[date], Optional[date]]:
        """Parse title string into name, valid_from, and valid_to.
        
        Args:
            text: String containing title information
            
        Returns:
            Tuple[str, Optional[date], Optional[date]]: Tuple of 
            (name, valid_from, valid_to)
        """
        # First try to match regular points list
        points_list_regex = \
            r"(\d.+ FIS points list \d\d\d\d.\d\d).+(\d\d-\d\d-\d\d\d\d).+(\d\d-\d\d-\d\d\d\d)"
        points_list_match = re.search(points_list_regex, text, re.DOTALL + re.IGNORECASE)
        if points_list_match:
            return points_list_match[1], \
                self._parse_date(points_list_match[2]), \
                self._parse_date(points_list_match[3])
            
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
            Tuple[Optional[date], Optional[date]]: Tuple of 
            (valid_from, valid_to) dates
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
  
    
    def download_and_process_points_list(self,
                                         points_list_data: Dict[str, Union[str, date]]) -> bool:
        """Download and process a single points list.
        
        Args:
            points_list_data: Dictionary containing points list info
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            if not os.path.exists(f'{self.DATA_FOLDER}/points_lists'):
                os.makedirs(f'{self.DATA_FOLDER}/points_lists')
            points_list_file = self._get_filelocation_for_points_list(points_list_data)

            if os.path.exists(points_list_file):
                logger.info(f"Points list already exists: {points_list_file}")                
            else:
                logger.info(f"Downloading points list: {points_list_data['name']}")
                response = requests.get(points_list_data['csv_url'])
                if response.status_code != 200:
                    return False
                
                with open(points_list_file, 'wb') as f:
                    f.write(response.content)
            
            self._save_points_list(points_list_data, pd.read_csv(points_list_file))
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing points list: {e}")
            return False
    
    def _points_list_from_dict(self, points_list_data: Dict[str, Union[str, date]]) -> PointsList:
        """Create a PointsList object from a dictionary.
        
        Args:
            points_list_data: Dictionary containing points list information
        """
        return PointsList(
            listid=points_list_data['listid'],
            valid_from=points_list_data['valid_from'],
            valid_to=points_list_data['valid_to'],
            season=points_list_data['seasoncode'],
            name=points_list_data['name']
        )

    def _athlete_from_row(self, row: pd.Series) -> Athlete:
        """
        Create an Athlete object from a row of points list data.
        
        Args:
            row: Pandas Series containing points list data
        
        Returns:
            Athlete: Athlete object
        """
        athlete = Athlete(
            fis_id=row.Fiscode,
            fis_db_id=row.Competitorid,
            last_name=row.Lastname,
            first_name=row.Firstname,
            nation_code=row.Nationcode,
            gender=Gender.M if row.Gender == 'M' else Gender.F,
            birth_date=self._date_or_none(row.Birthdate, '%Y-%m-%d'),
            birth_year=self._int_or_none(row.Birthyear),
            ski_club=self._str_or_none(row.Skiclub),
            national_code=self._str_or_none(row.Nationalcode)
        )
        return athlete

    def _athlete_points_from_row(self, row: pd.Series, athlete: Athlete,
                                 points_list: PointsList) -> AthletePoints:
        """
        Create an AthletePoints object from a row of points list data.

        Args:
            row: Pandas Series containing points list data
            athlete: Athlete object
            points_list: PointsList object

        Returns:
            AthletePoints: AthletePoints object
        """
        # NB: ACpoints and ACpos only present in season 2007 and later
        if 'ACpoints' in row:
            ac_points = self._float_or_none(row.ACpoints)
            ac_rank = self._int_or_none(row.ACpos)
            ac_status = self._str_or_none(row.ACSta)
        else:
            ac_points = None
            ac_rank = None
            ac_status = None

        return AthletePoints(
                    athlete = athlete,
                    points_list = points_list,
                    sl_points=self._float_or_none(row.SLpoints),
                    gs_points=self._float_or_none(row.GSpoints),
                    sg_points=self._float_or_none(row.SGpoints),
                    dh_points=self._float_or_none(row.DHpoints),
                    ac_points=ac_points,
                    sl_rank=self._int_or_none(row.SLpos),
                    gs_rank=self._int_or_none(row.GSpos),
                    sg_rank=self._int_or_none(row.SGpos),
                    dh_rank=self._int_or_none(row.DHpos),
                    ac_rank=ac_rank,
                    calculated_date=self._date_or_none(row.Calculationdate, '%d-%m-%Y'),
                    sl_status=self._str_or_none(row.SLSta),
                    gs_status=self._str_or_none(row.GSSta),
                    sg_status=self._str_or_none(row.SGSta),
                    dh_status=self._str_or_none(row.DHSta),
                    ac_status=ac_status,
                    ski_club=self._str_or_none(row.Skiclub),
                )

    def _save_points_list(self, points_list_data: Dict[str, Union[str, date]],
                          df: pd.DataFrame) -> None:
        """
        Save points list data to database.
        
        Args:
            points_list_data: Dictionary containing points list info
            df: DataFrame containing the points list data
        """
        points_list = self._points_list_from_dict(points_list_data)
        self.session.add(points_list)
        self.session.flush()
        
        for row in df.itertuples(index=False):
            try:
                athlete = self.session.query(Athlete).filter_by(fis_id=row.Fiscode).first()
                if not athlete:
                    athlete = self._athlete_from_row(row)
                    self.session.add(athlete)
                
                athlete_points = self._athlete_points_from_row(row, athlete, points_list)
                self.session.add(athlete_points)
                
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                logger.debug(pprint(row))
                continue
        
        self.session.commit()


    def _str_or_none(self, value: str) -> Optional[str]:
        """Convert string to None if it's empty, otherwise return the string.
        
        Args:
            value: String value
        
        Returns:
            String if value can be so coerced
            None if provided None, NaN, or non-convertible value
        """
        if value is None:
            return None
        elif pd.isna(value):
            return None
        
        return str(value)
    
    def _int_or_none(self, value: float) -> Optional[int]:
        """Convert float to int if it's an integer, otherwise return None.
        
        Args:
            value: Float value
        
        Returns:
            integer value if given number that can be converted to int
            None otherwise
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
        return None
    
    def _float_or_none(self, value: float) -> Optional[float]:
        """Convert float to float if it's a float, otherwise return None.
        
        Args:
            value: Float value
        
        Returns:
            float value if given number that can be converted to float
            None otherwise; NaN is converted to None
        """
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
        return None

    def _date_or_none(self, value: str, format: str) -> Optional[date]:
        """Convert string to date if it's a date, otherwise return None.
        
        Args:
            value: String value
            format: Format of the date string
        """
        try:
            date = pd.to_datetime(value, format=format).date()
            if date is not pd.NaT:
                return date
        except (TypeError, ValueError, AttributeError):
            return None
        return None
    
    def _get_list_url(self, sectorcode: str, seasoncode: str, listid: Optional[str] = None) -> str:
        """Get the URL for a specific points list.
        
        Args:
            sectorcode: Sector code (e.g., "AL")
            seasoncode: Season code (e.g., "2023/24")
            listId: List ID (e.g., "83"); None will return Base List for
            given season code
        """
        url_path = "fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html"
        list_params = f"export_csv=true&sectorcode={sectorcode}&seasoncode={seasoncode}"
        if listid:
            list_params += f"&listid={listid}"
        url = f"{DATA_URL}/{url_path}?{list_params}"
        return url

    def _extract_season(self, name: str) -> Optional[str]:
        """Extract season from points list name.
        
        Args:
            name: Points list name string
            
        Returns:
            Optional[str]: Season string (e.g., "2023/24") or None
            if not found
        """
        match = re.search(r'(\d{4}/\d{2})', name)
        return match.group(1) if match else None 

    def _get_updated_points_lists(self,
                              found_points_lists: List[Dict[str, Union[str, date]]]
                             ) -> List[Dict[str, Union[str, date]]]:
        """Filter list of points lists to only those not ingested to DB.
        
        Args:
            found_points_lists: List of dictionaries of points list information, eg from FIS site:
                - listid: List ID (e.g., "83"); None if Base List for given season code
                other fields are not used here but will be elsewhere

        Returns:
            List[Dict[str, Union[str, date]]]: 
            List of dictionaries containing points list information that are not yet in the DB:
                - sectorcode: Sector code (e.g., "AL")
                - seasoncode: Season code (e.g., "2023")
                - listid: List ID (e.g., "83"); None if Base List for given season code
                - name: Name of the points list
                - valid_from: Start date of validity period
                - valid_to: End date of validity period
        """
        new_lists = []
        for points_list in found_points_lists:
            if not self.session.query(PointsList).filter_by(listid=points_list['listid']).first():
                new_lists.append(points_list)
        return new_lists

    def _filter_lists_by_date(self,
                             points_lists: List[Dict[str, Union[str, date]]],
                             start_date: Optional[date],
                             end_date: Optional[date]) -> List[Dict[str, Union[str, date]]]:
        """Filter points lists by date. All points lists that would be
        valid for any date in the range are included.
        
        Args:
            points_lists: List of dictionaries of points list information
            start_date: Start date of validity period
            end_date: End date of validity period
        """
        filtered_lists = []
        for points_list in points_lists:
            if start_date is not None and points_list['valid_to'] < start_date:
                continue
            if end_date is not None and points_list['valid_from'] > end_date:
                continue
            filtered_lists.append(points_list)
        return filtered_lists

def _get_argument_parser() -> argparse.ArgumentParser:
    """Get the argument parser for the points list scraper."""
    parser = argparse.ArgumentParser(description="FIS points list scraper")
    parser.add_argument("--start-date", type=date.fromisoformat, help="Start date of validity period")
    parser.add_argument("--end-date", type=date.fromisoformat, help="End date of validity period")
    parser.add_argument("--only-list", type=int, help="Ingest a single list by id")
    return parser

def main(start_date: Optional[date] = None,
         end_date: Optional[date] = None, only_list: Optional[int] = None):
    """
    Main entry point for the points list scraper.

    Args:
        start_date: Start date of validity period
        end_date: End date of validity period
        only_list: Ingest a single list by id
    """
    try:
        logger.info("Starting FIS points list scraper")
        scraper = PointsListScraper()
        
        # Get available points lists
        logger.info("Fetching available points lists")
        points_lists = scraper.get_points_lists()
        logger.info(f"Found {len(points_lists)} points lists")

        if (start_date is not None or end_date is not None):
            points_lists = scraper._filter_lists_by_date(points_lists, start_date, end_date)
            logger.info(f"Found {len(points_lists)} points lists by date")
        elif only_list is not None:
            points_lists = [points_list for points_list in points_lists if points_list['listid'] == str(only_list)]
            logger.info(f"Found {len(points_lists)} points lists by listid")
        else:
            points_lists = scraper._get_updated_points_lists(points_lists)
            logger.info(f"Found {len(points_lists)} new points lists by listid")
        # Process each points list
        for i, points_list in enumerate(points_lists, 1):
            logger.info(f"Processing points list: {points_list['name']}")
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