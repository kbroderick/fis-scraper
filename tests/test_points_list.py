from numpy import NaN
import pytest
import os
import requests
import builtins

from pprint import pprint
from datetime import date
from collections import namedtuple
from bs4 import BeautifulSoup
from urllib import response
from unittest.mock import MagicMock
import pandas as pd
from sqlalchemy import select

import src.fis_scraper
from src.fis_scraper.scrapers.fis_constants import BASE_URL
from src.fis_scraper.scrapers.points_list_scraper import PointsListScraper
from src.fis_scraper.database.models import PointsList, Athlete, AthletePoints, Gender

@pytest.fixture
def scraper():
    return PointsListScraper()

LIST_DATA_411 = {
        'sectorcode': 'AL',
        'seasoncode': '2025',
        'listid': '411',
        'name': '20th FIS points list 2024/25',
        'valid_from': date(2025, 3, 4),
        'valid_to': date(2025, 4, 16)
    }
LIST_DATA_412 = {
        'sectorcode': 'AL',
        'seasoncode': '2025',
        'listid': '412',
        'name': '21st FIS points list 2024/25',
        'valid_from': date(2025, 4, 17),
        'valid_to': date(2025, 4, 30)
    }
LIST_DATA_413 = {
        'sectorcode': 'AL',
        'seasoncode': '2025',
        'listid': '413',
        'name': '22nd FIS points list 2024/25',
        'valid_from': date(2025, 5, 1),
        'valid_to': date(2025, 5, 31)
    }
LIST_413_DICT = {
        'csv_url': 'https://data.fis-ski.com/fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html?export_csv=true&sectorcode=AL&seasoncode=2025&listid=413',
        'listid': '413',
        'name': '22nd FIS points list 2024/25',
        'seasoncode': '2025',
        'sectorcode': 'AL',
        'valid_from': date(2025, 5, 1),
        'valid_to': date(2025, 5, 31)
    }

FullRow = namedtuple('FullRow', ['Listid', 'Listname', 'listPublished', 'Published', 'Sectorcode', 'Status', 'Competitorid', 'Fiscode', 'Lastname', 'Firstname', 'Nationcode', 'Gender', 'Birthdate', 'Skiclub', 'Nationalcode', 'Competitorname', 'Birthyear', 'Calculationdate', 'DHpoints', 'DHpos', 'DHSta', 'SLpoints', 'SLpos', 'SLSta', 'GSpoints', 'GSpos', 'GSSta', 'SGpoints', 'SGpos', 'SGSta', 'ACpoints', 'ACpos', 'ACSta'])
OldRow =  namedtuple('OldRow',  ['Listid', 'Listname', 'listPublished', 'Published', 'Sectorcode', 'Status', 'Competitorid', 'Fiscode', 'Lastname', 'Firstname', 'Nationcode', 'Gender', 'Birthdate', 'Skiclub', 'Nationalcode', 'Competitorname', 'Birthyear', 'Calculationdate', 'DHpoints', 'DHpos', 'DHSta', 'SLpoints', 'SLpos', 'SLSta', 'GSpoints', 'GSpos', 'GSSta', 'SGpoints', 'SGpos', 'SGSta'])

SEBASTIAN_ROW = OldRow(
    106,
    "8th FIS points list 2004/2005",
    1,
    1,
    'AL',
    'E',
    107303,
    501381,
    'LINDBLOM',
    'Sebastian',
    'SWE',
    'M',
    '1989-02-17',
    "Edsåsdalens SLK",
    NaN,
    "LINDBLOM Sebastian",
    1989,
    '21-06-2005',
    NaN,
    NaN,
    NaN,
    117.52,
    3390,
    NaN,
    131.44,
    4010,
    '*',
    175.83,
    2991,
    '*'
)

ABAJO_ROW = FullRow(
    412,
    "21st FIS points list 2024/25",
    1,
    1,
    'AL',
    'O',
    215778,
    492308,
    "ABAJO MASTRAL",
    'Jaime',
    'ESP',
    'M',
    '2000-11-03',
    "Formigal E.C.",
    'ESP',
    "ABAJO MASTRAL Jaime",
    2000,
    '15-04-2025',
    NaN,
    NaN,
    NaN,
    199.77,
    4989,
    '*',
    389.28,
    5886,
    '*',
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN
    )

BOUCHAAL_ROW = FullRow(
    391,
    "22nd FIS points list 2023/2024",
    1,
    0,
    'AL',
    'E',
    6445,
    160025,
    'BOUCHAAL',
    'Noureddine',
    'MAR',
    'M',
    NaN,
    NaN,
    NaN,
    "BOUCHAAL Noureddine",
    NaN,
    '01-05-2024',
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN,
    NaN
)

def test_parse_dates(scraper):
    # Test valid date string
    date_str = "01-01-2023 - 31-12-2023"  # Format: DD-MM-YYYY - DD-MM-YYYY
    valid_from, valid_to = scraper._parse_dates(date_str)
    assert valid_from == date(2023, 1, 1)
    assert valid_to == date(2023, 12, 31)
    
    # Test invalid date string
    date_str = "invalid"
    valid_from, valid_to = scraper._parse_dates(date_str)
    assert valid_from is None
    assert valid_to is None

def test_parse_title(scraper):
    # common case: two-digit ordinal—22nd list 24/25
    text = _sample_normal_row_soup().div.text
    result = scraper._parse_title(text)
    assert result[0] == '22nd FIS points list 2024/25'
    assert result[1] == date(2025, 5, 1)
    assert result[2] == date(2025, 5, 31)

    # base list ("Internal Base list 2026")
    text = _sample_internal_base_row_soup().div.text
    result = scraper._parse_title(text)
    assert result[0] == 'Internal Base list 2026'
    assert result[1] is None
    assert result[2] is None

    # single-digit ordinal
    text = _sample_normal_row_single_ordinal_soup().div.text
    result = scraper._parse_title(text)
    assert result[0] == '9th FIS points list 2024/25'
    assert result[1] == date(2024, 10, 24)
    assert result[2] == date(2024, 11, 6)

    # another base list
    text = _sample_base_row_soup().div.text
    result = scraper._parse_title(text)
    assert result[0] == 'Base List 2025'
    assert result[1] is None
    assert result[2] is None

def test_parse_list_row(scraper):
    row = scraper._parse_list_row(_sample_normal_row_soup().div)
    assert row['sectorcode'] == 'AL'
    assert row['seasoncode'] == '2025'
    assert row['name'] == '22nd FIS points list 2024/25'
    assert row['valid_from'] == date(2025, 5, 1)
    assert row['valid_to'] == date(2025, 5, 31)
    assert row['csv_url'] == 'https://data.fis-ski.com/fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html?export_csv=true&sectorcode=AL&seasoncode=2025&listid=413'

def test_extract_season(scraper):
    # Test valid season string
    name = "1st FIS points list 2023/24"
    season = scraper._extract_season(name)
    assert season == "2023/24"
    
    # Test invalid season string
    name = "Invalid name"
    season = scraper._extract_season(name)
    assert season is None

def test_get_filelocation_for_points_list(scraper):
    list_data = {
        'sectorcode': 'AL',
        'seasoncode': '2025',
        'listid': '413'
    }
    assert scraper._get_filelocation_for_points_list(list_data) == "data/points_lists/FAL_2025413.csv"

def test_get_filename_for_points_list(scraper):
    list_data = {
        'sectorcode': 'AL',
        'seasoncode': '2025',
        'listid': '413'
    }
    assert scraper._get_filename_for_points_list(list_data) == "FAL_2025413"

def test_get_points_lists(scraper, mocker):
    test_file_path = os.path.join('tests', 'data', 'fis_point_lists_20250523.html')
    with open(test_file_path, 'r') as f:
        mock_html = f.read()

    mock_response = MagicMock(spec=response)
    mock_response.text = mock_html
    mocker.patch('requests.get', return_value=mock_response)
 
    points_lists = scraper.get_points_lists(include_base_lists=True)
    
    # Verify the request was made to the correct URL
    requests.get.assert_called_once_with(scraper.POINTS_LISTS_URL)
   
    # Verify we got the expected number of points lists
    assert len(points_lists) == 349
    
    # Verify the first points list data (base 2026)
    first_list = points_lists[0]
    assert first_list['sectorcode'] == 'AL'
    assert first_list['seasoncode'] == '2026'
    assert first_list['listid'] == ''
    assert first_list['name'] == 'Internal Base list 2026'
    assert first_list['valid_from'] is None
    assert first_list['valid_to'] is None
    
    # Verify the second points list data (22nd 2024/25)
    second_list = points_lists[1]
    assert second_list['sectorcode'] == 'AL'
    assert second_list['seasoncode'] == '2025'
    assert second_list['listid'] == '413'
    assert second_list['name'] == '22nd FIS points list 2024/25'
    assert second_list['valid_from'] == date(2025, 5, 1)
    assert second_list['valid_to'] == date(2025, 5, 31)

    points_lists = scraper.get_points_lists(include_base_lists=False)
    assert len(points_lists) == 331

def _points_list_path_exists(path):
    if path.endswith('csv'):
        return False
    return True

def test_download_and_process_points_list_not_present(scraper, mocker):
    points_list_file = os.path.join('tests', 'data', 'FAL_2025413.csv')
    with open(points_list_file, 'rb') as f:
        mock_file = f.read()
    mock_response = MagicMock(spec=response)
    mock_response.status_code = 200
    mock_response.content = mock_file
    mocker.patch('requests.get', return_value=mock_response)
    mocker.patch('src.fis_scraper.scrapers.points_list.PointsListScraper._save_points_list')

    # NOTE: we are not mocking builtins.open, so file will be written to disk.
    mocker.patch('os.path.exists', side_effect=_points_list_path_exists)
    scraper.download_and_process_points_list(LIST_413_DICT)
    requests.get.assert_called_once_with(LIST_413_DICT['csv_url'])
    src.fis_scraper.scrapers.points_list.PointsListScraper._save_points_list.assert_called_once()

def test_download_and_process_points_list_present(scraper, mocker):
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('builtins.open')
    mocker.patch('requests.get')
    mocker.patch('pandas.read_csv')
    mocker.patch('src.fis_scraper.scrapers.points_list.PointsListScraper._save_points_list')
    scraper.download_and_process_points_list(LIST_413_DICT)
    requests.get.assert_not_called() 
    builtins.open.assert_not_called()
    src.fis_scraper.scrapers.points_list.PointsListScraper._save_points_list.assert_called_once()

def test_save_points_list(scraper):
    # Create test data
    abbreviated_points_list_file = os.path.join('tests', 'data', 'FAL_2025413_abbrev.csv')
    # Save points list
    scraper._save_points_list(LIST_413_DICT, pd.read_csv(abbreviated_points_list_file))
    
    # Verify database entries
    points_list = scraper.session.scalars(select(PointsList)).first()
    assert points_list is not None
    assert points_list.valid_from == date(2025, 5, 1)
    assert points_list.valid_to == date(2025, 5, 31)
    assert points_list.season == '2025'
    
    athlete = scraper.session.scalars(select(Athlete).where(Athlete.fis_id == 820069)).first()
    assert athlete.fis_db_id == 283026
    assert athlete.first_name == 'Umer'
    assert athlete.last_name == '.'
    assert athlete.nation_code == 'PAK'
    assert athlete.gender == Gender.M
    assert athlete.birth_date == date(2006, 2, 10)
    assert athlete.birth_year == 2006
    assert athlete.ski_club is None
    assert athlete.national_code is None
    
    athlete_points = scraper.session.scalars(select(AthletePoints).where(AthletePoints.athlete_id == athlete.id)).first()
    assert athlete_points.sl_points == 680.26
    assert athlete_points.gs_points == 454.43
    assert athlete_points.sg_points is None
    assert athlete_points.dh_points is None
    assert athlete_points.ac_points is None
    assert athlete_points.sl_rank == 6063
    assert athlete_points.gs_rank == 5988
    assert athlete_points.sg_rank is None
    assert athlete_points.dh_rank is None
    assert athlete_points.ac_rank is None
    assert athlete_points.calculated_date == date(2025, 5, 1)
    
    # Clean up
    scraper.session.query(AthletePoints).delete()
    scraper.session.query(Athlete).delete()
    scraper.session.query(PointsList).delete()
    scraper.session.commit() 

def test_get_updated_points_lists(scraper):
    all_lists = [LIST_DATA_411, LIST_DATA_412, LIST_DATA_413]

    scraper.session.add(scraper._points_list_from_dict(LIST_DATA_411))
    scraper.session.add(scraper._points_list_from_dict(LIST_DATA_412))
    scraper.session.commit()

    new_lists = scraper._get_updated_points_lists(all_lists)
    assert len(new_lists) == 1
    assert new_lists[0]['listid'] == '413'
    assert new_lists[0]['name'] == '22nd FIS points list 2024/25'
    assert new_lists[0]['valid_from'] == date(2025, 5, 1)
    assert new_lists[0]['valid_to'] == date(2025, 5, 31)

    scraper.session.query(PointsList).delete()
    scraper.session.commit()

def test_filter_lists_by_date(scraper):
    all_lists = [LIST_DATA_411, LIST_DATA_412, LIST_DATA_413]
    filtered_lists = scraper._filter_lists_by_date(all_lists, None, None)
    assert len(filtered_lists) == 3

    filtered_lists = scraper._filter_lists_by_date(all_lists, date(2025, 4, 1), date(2025, 4, 30))
    assert len(filtered_lists) == 2
    assert filtered_lists[0]['listid'] == '411'
    assert filtered_lists[1]['listid'] == '412'

    filtered_lists = scraper._filter_lists_by_date(all_lists, date(2025, 4, 30), None)
    assert len(filtered_lists) == 2
    assert filtered_lists[0]['listid'] == '412'
    assert filtered_lists[1]['listid'] == '413'

    filtered_lists = scraper._filter_lists_by_date(all_lists, None, date(2025, 4, 30))
    assert len(filtered_lists) == 2
    assert filtered_lists[0]['listid'] == '411'
    assert filtered_lists[1]['listid'] == '412'

def test_points_list_from_dict(scraper):
    points_list = scraper._points_list_from_dict(LIST_DATA_413)
    assert points_list.listid == '413'
    assert points_list.name == '22nd FIS points list 2024/25'
    assert points_list.valid_from == date(2025, 5, 1)
    assert points_list.valid_to == date(2025, 5, 31)
    assert points_list.season == '2025'

def test_athlete_from_row_normal(scraper):
    athlete = scraper._athlete_from_row(ABAJO_ROW)

    assert athlete.fis_id == 492308
    assert athlete.fis_db_id == 215778
    assert athlete.first_name == 'Jaime'
    assert athlete.last_name == 'ABAJO MASTRAL'
    assert athlete.nation_code == 'ESP'
    assert athlete.gender == Gender.M
    assert athlete.birth_date == date(2000, 11, 3)
    assert athlete.birth_year == 2000
    assert athlete.ski_club == 'Formigal E.C.'
    assert athlete.national_code == 'ESP'

def test_athlete_from_row_no_birthyear(scraper):
    # now without a birthdate or birthyear
    athlete = scraper._athlete_from_row(BOUCHAAL_ROW)
    assert athlete.fis_id == 160025
    assert athlete.fis_db_id == 6445
    assert athlete.first_name == 'Noureddine'
    assert athlete.last_name == 'BOUCHAAL'
    assert athlete.nation_code == 'MAR'
    assert athlete.birth_date is None
    assert athlete.birth_year is None

def test_athlete_from_row_no_acpoints(scraper):
    # now with no ACpoints or ACrank
    athlete = scraper._athlete_from_row(SEBASTIAN_ROW)
    assert athlete.fis_id == 501381
    assert athlete.fis_db_id == 107303
    assert athlete.first_name == 'Sebastian'
    assert athlete.last_name == 'LINDBLOM'
    assert athlete.nation_code == 'SWE'
    assert athlete.gender == Gender.M
    assert athlete.birth_date == date(1989, 2, 17)
    assert athlete.birth_year == 1989
    assert athlete.ski_club == 'Edsåsdalens SLK'
    assert athlete.national_code == None

def test_athlete_points_from_row_normal(scraper):
    athlete = scraper._athlete_from_row(ABAJO_ROW)
    points_list = scraper._points_list_from_dict(LIST_DATA_413)
    athlete_points = scraper._athlete_points_from_row(ABAJO_ROW, athlete, points_list)
    assert athlete_points.sl_points == 199.77
    assert athlete_points.gs_points is 389.28
    assert athlete_points.sg_points is None
    assert athlete_points.dh_points is None
    assert athlete_points.ac_points is None
    assert athlete_points.sl_rank == 4989

def test_athlete_points_from_row_no_birthyear(scraper):
    athlete = scraper._athlete_from_row(BOUCHAAL_ROW)
    points_list = scraper._points_list_from_dict(LIST_DATA_413)
    athlete_points = scraper._athlete_points_from_row(BOUCHAAL_ROW, athlete, points_list)
    assert athlete_points.sl_points is None
    assert athlete_points.gs_points is None
    assert athlete_points.sg_points is None
    assert athlete_points.dh_points is None
    assert athlete_points.ac_points is None
    assert athlete_points.sl_rank is None
    assert athlete_points.gs_rank is None

def test_athlete_points_from_row_no_acpoints(scraper):
    athlete = scraper._athlete_from_row(SEBASTIAN_ROW)
    points_list = scraper._points_list_from_dict(LIST_DATA_413)
    athlete_points = scraper._athlete_points_from_row(SEBASTIAN_ROW, athlete, points_list)
    assert athlete_points.sl_points == 117.52
    assert athlete_points.gs_points == 131.44
    assert athlete_points.sg_points == 175.83
    assert athlete_points.dh_points is None
    assert athlete_points.ac_points is None
    assert athlete_points.sl_rank == 3390
    assert athlete_points.gs_rank == 4010
    assert athlete_points.sg_rank == 2991
    assert athlete_points.dh_rank is None

def _sample_internal_base_row_soup():
    # Internal Base list 2026
    raw_row = '<div class="container g-xs-24">\n<div class="g-xs-9 g-sm-9 g-md-7 g-lg-7 justify-left align-xs-top align-md-top">\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/fis-points-details.html?sectorcode=AL&amp;seasoncode=2026" target="_self">\n<span class="link__text">Internal Base list 2026</span>\n</a>\n</div>\n<div class="g-xs-5 g-sm-5 g-md-6 g-lg-6 justify-left gray">\n<div class="clip-sm clip-xs">Not valid for races (calculated\n                                                                at 02-05-2025)\n                                                            </div>\n</div>\n<div class="g-md-1 g-lg-1 justify-left pale hidden-sm-down">\xa0\n                                                    </div>\n<div class="g-xs-10 g-sm-10 g-md-10 g-lg-10 justify-left">\n<div class="split-row split-row_bordered">\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list XLSX" href="javascript:void(0)" onclick="fct_export_fispointslist_xlsx(\'AL\',\'2026\',\'\')" rel="nofollow" target="_self" title="Export the full list in Excel">\n<span class="link__text">Excel (xlsx)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list CSV" href="javascript:void(0)" onclick="fct_export_fispointslist_csv(\'AL\',\'2026\',\'\')" rel="nofollow" target="_self" title="Export the full list in CSV">\n<span class="link__text">Excel (csv)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n</div>\n</div>\n</div>'
    return BeautifulSoup(raw_row, 'html.parser')

def _sample_base_row_soup():
    # Base List 2025
    raw_row = '<div class="container g-xs-24">\n<div class="g-xs-9 g-sm-9 g-md-7 g-lg-7 justify-left align-xs-top align-md-top">\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/fis-points-details.html?sectorcode=AL&amp;seasoncode=2025" target="_self">\n<span class="link__text">Base List 2025</span>\n</a>\n</div>\n<div class="g-xs-5 g-sm-5 g-md-6 g-lg-6 justify-left gray">\n<div class="clip-sm clip-xs">Not valid for races (calculated\n                                                                at 15-01-2025)\n                                                            </div>\n</div>\n<div class="g-md-1 g-lg-1 justify-left pale hidden-sm-down">\xa0\n                                                    </div>\n<div class="g-xs-10 g-sm-10 g-md-10 g-lg-10 justify-left">\n<div class="split-row split-row_bordered">\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list XLSX" href="javascript:void(0)" onclick="fct_export_fispointslist_xlsx(\'AL\',\'2025\',\'\')" rel="nofollow" target="_self" title="Export the full list in Excel">\n<span class="link__text">Excel (xlsx)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list CSV" href="javascript:void(0)" onclick="fct_export_fispointslist_csv(\'AL\',\'2025\',\'\')" rel="nofollow" target="_self" title="Export the full list in CSV">\n<span class="link__text">Excel (csv)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n</div>\n</div>\n</div>'
    return BeautifulSoup(raw_row, 'html.parser')

def _sample_normal_row_soup():
    # 22nd FIS points list 2024/25
    raw_row = '<div class="container g-xs-24">\n<div class="g-xs-9 g-sm-9 g-md-7 g-lg-7 justify-left align-xs-top align-md-top">\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/fis-points-details.html?sectorcode=AL&amp;seasoncode=2025&amp;listid=413" target="_self">\n<span class="link__text">22nd FIS points list 2024/25</span>\n</a>\n</div>\n<div class="g-sm-3 g-md-3 g-lg-3 justify-left hidden-sm-down">01-05-2025</div>\n<div class="g-sm-3 g-md-3 g-lg-3 justify-left hidden-sm-down">31-05-2025</div>\n<div class="g-xs-5 justify-left hidden-md-up">\n<div class="split-row">\n<div class="split-row__item pr-xs-0">01-05-2025</div>\n<div class="split-row__item pr-xs-0">31-05-2025</div>\n</div>\n</div>\n<div class="g-md-1 g-lg-1 justify-left pale hidden-sm-down">\xa0\n                                                    </div>\n<div class="g-xs-10 g-sm-10 g-md-10 g-lg-10 justify-left">\n<div class="split-row split-row_bordered">\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list XLSX" href="javascript:void(0)" onclick="fct_export_fispointslist_xlsx(\'AL\',\'2025\',\'413\')" rel="nofollow" target="_self" title="Export the full list in Excel">\n<span class="link__text">Excel (xlsx)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list CSV" href="javascript:void(0)" onclick="fct_export_fispointslist_csv(\'AL\',\'2025\',\'413\')" rel="nofollow" target="_self" title="Export the full list in CSV">\n<span class="link__text">Excel (csv)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points full list" href="https://www.fis-ski.com/DB/v2/download/fis-list/ALFP2225F.zip" rel="nofollow" target="_blank" title="Link to full list">\n<span class="link__text">Full list</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points partial list" href="https://www.fis-ski.com/DB/v2/download/fis-list/ALFP2225P.zip" rel="nofollow" target="_blank" title="Link to partial list">\n<span class="link__text">Partial list</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points 1st page" href="https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html?sectorcode=AL&amp;seasoncode=2025&amp;listid=413&amp;firstpage=true" rel="nofollow" target="_blank" title="Link to first page">\n<span class="link__text">1st page (Z-value,Adder,...)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/parallel-world-seeding-list.html?sectorcode=AL&amp;seasoncode=2025&amp;fispointslistid=413&amp;listtype=PWSL" rel="nofollow" target="_blank" title="Link to the Parallel World Seeding List (PWSL)">\n<span class="link__text">Parallel World Seeding List (PWSL)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS Parallel World Seeding XLSX" href="javascript:void(0)" onclick="fct_export_pwsl_xlsx(\'AL\',\'2025\',\'413\')" rel="nofollow" target="_self" title="Export the PWSL in Excel">\n<span class="link__text">PWSL (Excel)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS Parallel World Seeding CSV" href="javascript:void(0)" onclick="fct_export_pwsl_csv(\'AL\',\'2025\',\'413\')" rel="nofollow" target="_self" title="Export the PWSL in CSV">\n<span class="link__text">PWSL (CSV)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n</div>\n</div>\n</div>'
    return BeautifulSoup(raw_row, 'html.parser')

def _sample_normal_row_single_ordinal_soup():
    # 9th FIS points list 2024/25
    raw_row = '<div class="container g-xs-24">\n<div class="g-xs-9 g-sm-9 g-md-7 g-lg-7 justify-left align-xs-top align-md-top">\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/fis-points-details.html?sectorcode=AL&amp;seasoncode=2025&amp;listid=400" target="_self">\n<span class="link__text">9th FIS points list 2024/25</span>\n</a>\n</div>\n<div class="g-sm-3 g-md-3 g-lg-3 justify-left hidden-sm-down">24-10-2024</div>\n<div class="g-sm-3 g-md-3 g-lg-3 justify-left hidden-sm-down">06-11-2024</div>\n<div class="g-xs-5 justify-left hidden-md-up">\n<div class="split-row">\n<div class="split-row__item pr-xs-0">24-10-2024</div>\n<div class="split-row__item pr-xs-0">06-11-2024</div>\n</div>\n</div>\n<div class="g-md-1 g-lg-1 justify-left pale hidden-sm-down">\xa0\n    </div>\n<div class="g-xs-10 g-sm-10 g-md-10 g-lg-10 justify-left">\n<div class="split-row split-row_bordered">\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list XLSX" href="javascript:void(0)" onclick="fct_export_fispointslist_xlsx(\'AL\',\'2025\',\'400\')" rel="nofollow" target="_self" title="Export the full list in Excel">\n<span class="link__text">Excel (xlsx)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points list CSV" href="javascript:void(0)" onclick="fct_export_fispointslist_csv(\'AL\',\'2025\',\'400\')" rel="nofollow" target="_self" title="Export the full list in CSV">\n<span class="link__text">Excel (csv)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points full list" href="https://www.fis-ski.com/DB/v2/download/fis-list/ALFP925F.zip" rel="nofollow" target="_blank" title="Link to full list">\n<span class="link__text">Full list</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points partial list" href="https://www.fis-ski.com/DB/v2/download/fis-list/ALFP925P.zip" rel="nofollow" target="_blank" title="Link to partial list">\n<span class="link__text">Partial list</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS points 1st page" href="https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html?sectorcode=AL&amp;seasoncode=2025&amp;listid=400&amp;firstpage=true" rel="nofollow" target="_blank" title="Link to first page">\n<span class="link__text">1st page (Z-value,Adder,...)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n<div class="split-row__item">\n<ul class="info__list flex-lg-wrap flex-md-wrap flex-sm-wrap flex-xs-wrap">\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" href="https://www.fis-ski.com/DB/general/parallel-world-seeding-list.html?sectorcode=AL&amp;seasoncode=2025&amp;fispointslistid=400&amp;listtype=PWSL" rel="nofollow" target="_blank" title="Link to the Parallel World Seeding List (PWSL)">\n<span class="link__text">Parallel World Seeding List (PWSL)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS Parallel World Seeding XLSX" href="javascript:void(0)" onclick="fct_export_pwsl_xlsx(\'AL\',\'2025\',\'400\')" rel="nofollow" target="_self" title="Export the PWSL in Excel">\n<span class="link__text">PWSL (Excel)</span>\n</a>\n</span>\n</li>\n<li class="info__item info__item_type_category">\n<span>\n<a class="link link_theme_dark link_text_underline" data-ga-download="FIS Parallel World Seeding CSV" href="javascript:void(0)" onclick="fct_export_pwsl_csv(\'AL\',\'2025\',\'400\')" rel="nofollow" target="_self" title="Export the PWSL in CSV">\n<span class="link__text">PWSL (CSV)</span>\n</a>\n</span>\n</li>\n</ul>\n</div>\n</div>\n</div>\n</div>'
    return BeautifulSoup(raw_row, 'html.parser')

        
