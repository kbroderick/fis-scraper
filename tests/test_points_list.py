import pytest
import os
import requests

from datetime import date
from bs4 import BeautifulSoup
from urllib import response
from unittest.mock import MagicMock

import fis_scraper
from fis_scraper.scrapers.points_list import PointsListScraper
from fis_scraper.database.models import PointsList, Athlete, AthletePoints, Gender

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
 
    # Call the method
    points_lists = scraper.get_points_lists(include_base_lists=True)
    
    # Verify the request was made to the correct URL
    requests.get.assert_called_once_with(scraper.BASE_URL)
    
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

def test_download_and_process_points_list(scraper, mocker):
    points_list_data = {
        'csv_url': 'https://data.fis-ski.com/fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html?export_csv=true&sectorcode=AL&seasoncode=2025&listid=413',
        'listid': '413',
        'name': '22nd FIS points list 2024/25',
        'seasoncode': '2025',
        'sectorcode': 'AL',
        'valid_from': date(2025, 5, 1),
        'valid_to': date(2025, 5, 31)
    }
    points_list_file = os.path.join('tests', 'data', 'FAL_2025413.csv')
    with open(points_list_file, 'rb') as f:
        mock_file = f.read()
    mock_response = MagicMock(spec=response)
    mock_response.status_code = 200
    mock_response.content = mock_file
    mocker.patch('requests.get', return_value=mock_response)
    mocker.patch('fis_scraper.scrapers.points_list.PointsListScraper._save_points_list')

    # Call the method
    scraper.download_and_process_points_list(points_list_data)

    requests.get.assert_called_once_with(points_list_data['csv_url'])
    fis_scraper.scrapers.points_list.PointsListScraper._save_points_list.assert_called_once()

    # cleanup
    scraper.session.query(PointsList).delete()
    scraper.session.query(Athlete).delete()
    scraper.session.query(AthletePoints).delete()
    scraper.session.commit()

def test_save_points_list(scraper):
    # Create test data
    abbreviated_points_list_file = os.path.join('tests', 'data', 'FAL_2025413_abbrev.csv')
    points_list_data = {
        'csv_url': 'https://data.fis-ski.com/fis_athletes/ajax/fispointslistfunctions/export_fispointslist.html?export_csv=true&sectorcode=AL&seasoncode=2025&listid=413',
        'listid': '413',
        'name': '22nd FIS points list 2024/25',
        'seasoncode': '2025',
        'sectorcode': 'AL',
        'valid_from': date(2025, 5, 1),
        'valid_to': date(2025, 5, 31)
    }
    
    # Create test DataFrame
    import pandas as pd
    df = pd.read_csv(abbreviated_points_list_file)
    
    # Save points list
    scraper._save_points_list(points_list_data, df)
    
    # Verify database entries
    points_list = scraper.session.query(PointsList).first()
    assert points_list is not None
    assert points_list.valid_from == date(2025, 5, 1)
    assert points_list.valid_to == date(2025, 5, 31)
    assert points_list.season == '2025'
    
    athlete = scraper.session.query(Athlete).first()
    assert athlete is not None
    assert athlete.fis_id == 820069
    assert athlete.first_name == 'Umer'
    assert athlete.last_name == '.'
    assert athlete.nation_code == 'PAK'
    assert athlete.gender == Gender.M
    assert athlete.birth_date == date(2006, 2, 10)
    assert athlete.birth_year == 2006
    assert athlete.ski_club is None
    assert athlete.national_code is None
    
    athlete_points = scraper.session.query(AthletePoints).first()
    assert athlete_points is not None
    assert athlete_points.sl_points == 680.26
    assert athlete_points.gs_points == 454.43
    assert athlete_points.sg_points is None
    assert athlete_points.dh_points is None
    assert athlete_points.sl_rank == 6063
    assert athlete_points.gs_rank == 5988
    assert athlete_points.sg_rank is None
    assert athlete_points.dh_rank is None
    
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

        
