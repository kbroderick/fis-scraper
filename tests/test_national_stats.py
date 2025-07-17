import pandas as pd
import pytest
import logging
from datetime import date
from typing import Dict, List, Any
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.fis_scraper.analysis.national_stats import NationalStatsAnalyzer
from src.fis_scraper.database.models import Athlete, AthletePoints, Gender, Discipline, PointsList
from src.fis_scraper.scrapers.race_results_scraper import RaceResultsScraper


class TestNationalStatsAnalyzer:
    """Test suite for NationalStatsAnalyzer class."""
    
    @pytest.fixture
    def analyzer(self) -> NationalStatsAnalyzer:
        """Create a NationalStatsAnalyzer instance for testing."""
        return NationalStatsAnalyzer()
    
    @pytest.fixture
    def mock_session(self) -> Mock:
        """Create a mock database session for testing."""
        return Mock(spec=Session)
    
    @pytest.fixture
    def sample_points_list(self) -> PointsList:
        """Create a sample points list for testing."""
        return PointsList(
            id=1,
            listid='413',
            name='22nd FIS points list 2024/25',
            season='2025',
            valid_from=date(2025, 5, 1),
            valid_to=date(2025, 5, 31),
            sectorcode='AL'
        )
    
    @pytest.fixture
    def sample_athlete(self) -> Athlete:
        """Create a sample athlete for testing."""
        return Athlete(
            id=1,
            fis_id=12345,
            fis_db_id=67890,
            first_name='John',
            last_name='Doe',
            nation_code='USA',
            gender=Gender.M,
            birth_date=date(1995, 1, 1),
            birth_year=1995,
            ski_club='Test Club',
            national_code='USA'
        )
    
    @pytest.fixture
    def sample_athlete_points(self, sample_athlete: Athlete, sample_points_list: PointsList) -> AthletePoints:
        """Create sample athlete points for testing."""
        return AthletePoints(
            id=1,
            athlete_id=sample_athlete.id,
            points_list_id=sample_points_list.id,
            sl_points=50.0,
            gs_points=75.0,
            sg_points=100.0,
            dh_points=125.0,
            ac_points=150.0,
            sl_rank=10,
            gs_rank=15,
            sg_rank=20,
            dh_rank=25,
            ac_rank=30,
            calculated_date=date(2025, 5, 1),
            sl_status='*',
            gs_status='*',
            sg_status='*',
            dh_status='*',
            ac_status='*'
        )

    @pytest.fixture(scope='class')
    def sample_series_by_year_gender_output(self) -> Dict[int, Dict[str, int]]:
        """Create a sample national_series_for_seasons output for testing with only Gender.M."""
        return  {
            2024: {
                'total-Male': 848, 'SL-50-Male': 2, 'SL-100-Male': 5, 'SL-300-Male': 24, 'SL-ranked-Male': 763,
                  'GS-50-Male': 5, 'GS-100-Male': 8, 'GS-300-Male': 26, 'GS-500-Male': 53,'GS-ranked-Male': 711,
                  'SG-50-Male': 5, 'SG-100-Male': 10, 'SG-300-Male': 27, 'SG-500-Male': 48, 'SG-ranked-Male': 497,
                  'DH-50-Male': 5, 'DH-100-Male': 9, 'DH-300-Male': 28, 'DH-500-Male': 52, 'DH-ranked-Male': 371,
                  'AC-50-Male': 2, 'AC-100-Male': 8, 'AC-300-Male': 25, 'AC-500-Male': 37, 'AC-ranked-Male': 98,
                  'ALL-50-Male': 14, 'ALL-100-Male': 24, 'ALL-300-Male': 68, 'ALL-500-Male': 103, 'ALL-ranked-Male': 779
                },
            2025: {'total-Male': 840, 'SL-50-Male': 2, 'SL-100-Male': 7, 'SL-300-Male': 21, 'SL-500-Male': 48, 'SL-ranked-Male': 743,
                  'GS-50-Male': 3, 'GS-100-Male': 7, 'GS-300-Male': 26, 'GS-500-Male': 54, 'GS-ranked-Male': 716,
                  'SG-50-Male': 5, 'SG-100-Male': 10, 'SG-300-Male': 28, 'SG-500-Male': 52, 'SG-ranked-Male': 470,
                  'DH-50-Male': 6, 'DH-100-Male': 10, 'DH-300-Male': 28, 'DH-500-Male': 51, 'DH-ranked-Male': 385,
                  'AC-50-Male': 2, 'AC-100-Male': 6, 'AC-300-Male': 23, 'AC-500-Male': 30, 'AC-ranked-Male': 79,
                  'ALL-50-Male': 12, 'ALL-100-Male': 24, 'ALL-300-Male': 59, 'ALL-500-Male': 106, 'ALL-ranked-Male': 766
                }
            }

    @pytest.fixture(scope='class')
    def sample_series_by_year_output(self) -> Dict[int, Dict[str, int]]:
        """Create a sample national_series_for_seasons output for testing."""
        return  {
            2024: {
                'total-Male': 848, 'SL-50-Male': 2, 'SL-100-Male': 5, 'SL-300-Male': 24, 'SL-ranked-Male': 763,
                  'GS-50-Male': 5, 'GS-100-Male': 8, 'GS-300-Male': 26, 'GS-500-Male': 53,'GS-ranked-Male': 711,
                  'SG-50-Male': 5, 'SG-100-Male': 10, 'SG-300-Male': 27, 'SG-500-Male': 48, 'SG-ranked-Male': 497,
                  'DH-50-Male': 5, 'DH-100-Male': 9, 'DH-300-Male': 28, 'DH-500-Male': 52, 'DH-ranked-Male': 371,
                  'AC-50-Male': 2, 'AC-100-Male': 8, 'AC-300-Male': 25, 'AC-500-Male': 37, 'AC-ranked-Male': 98,
                  'ALL-50-Male': 14, 'ALL-100-Male': 24, 'ALL-300-Male': 68, 'ALL-500-Male': 103, 'ALL-ranked-Male': 779,
                'total-Female': 651,
                  'SL-50-Female': 4, 'SL-100-Female': 9, 'SL-300-Female': 24, 'SL-500-Female': 56, 'SL-ranked-Female': 591,
                  'GS-50-Female': 4, 'GS-100-Female': 12, 'GS-300-Female': 28, 'GS-500-Female': 56, 'GS-ranked-Female': 545,
                  'SG-50-Female': 4, 'SG-100-Female': 9, 'SG-300-Female': 25, 'SG-500-Female': 36, 'SG-ranked-Female': 334,
                  'DH-50-Female': 6, 'DH-100-Female': 9, 'DH-300-Female': 17, 'DH-500-Female': 32, 'DH-ranked-Female': 191,
                  'AC-50-Female': 3, 'AC-100-Female': 6, 'AC-300-Female': 20, 'AC-500-Female': 31, 'AC-ranked-Female': 51,
                  'ALL-50-Female': 10, 'ALL-100-Female': 22, 'ALL-300-Female': 47, 'ALL-500-Female': 95, 'ALL-ranked-Female': 602,
                },
            2025: {'total-Male': 840, 'SL-50-Male': 2, 'SL-100-Male': 7, 'SL-300-Male': 21, 'SL-500-Male': 48, 'SL-ranked-Male': 743,
                  'GS-50-Male': 3, 'GS-100-Male': 7, 'GS-300-Male': 26, 'GS-500-Male': 54, 'GS-ranked-Male': 716,
                  'SG-50-Male': 5, 'SG-100-Male': 10, 'SG-300-Male': 28, 'SG-500-Male': 52, 'SG-ranked-Male': 470,
                  'DH-50-Male': 6, 'DH-100-Male': 10, 'DH-300-Male': 28, 'DH-500-Male': 51, 'DH-ranked-Male': 385,
                  'AC-50-Male': 2, 'AC-100-Male': 6, 'AC-300-Male': 23, 'AC-500-Male': 30, 'AC-ranked-Male': 79,
                  'ALL-50-Male': 12, 'ALL-100-Male': 24, 'ALL-300-Male': 59, 'ALL-500-Male': 106, 'ALL-ranked-Male': 766,
                'total-Female': 653,
                  'SL-50-Female': 3, 'SL-100-Female': 8, 'SL-300-Female': 27, 'SL-500-Female': 57, 'SL-ranked-Female': 596,
                  'GS-50-Female': 6, 'GS-100-Female': 9, 'GS-300-Female': 24, 'GS-500-Female': 55, 'GS-ranked-Female': 561,
                  'SG-50-Female': 4, 'SG-100-Female': 11, 'SG-300-Female': 24, 'SG-500-Female': 37, 'SG-ranked-Female': 332,
                  'DH-50-Female': 6, 'DH-100-Female': 10, 'DH-300-Female': 19, 'DH-500-Female': 36, 'DH-ranked-Female': 218,
                  'AC-50-Female': 4, 'AC-100-Female': 5, 'AC-300-Female': 18, 'AC-500-Female': 34, 'AC-ranked-Female': 51,
                  'ALL-50-Female': 13, 'ALL-100-Female': 21, 'ALL-300-Female': 49, 'ALL-500-Female': 97, 'ALL-ranked-Female': 607,
                }
            }

    def test_init(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test NationalStatsAnalyzer initialization."""
        assert analyzer.session is not None
        assert analyzer.RANKING_DEFAULTS == [50, 100, 300, 500, 1000, 2000, 3000]

    def test_national_report(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_report method."""
        mock_report_by_gender = mocker.patch.object(analyzer, 'national_report_by_gender')
        mock_report_by_gender.return_value = {'licenses': 100, 'SL': {50: 5, 100: 10}}
        
        result = analyzer.national_report('USA', 2025)
        
        assert Gender.M in result
        assert Gender.F in result
        assert mock_report_by_gender.call_count == 2
        # Check that it was called for both genders
        calls = mock_report_by_gender.call_args_list
        assert calls[0][0][0] == Gender.M  # First call with Gender.M
        assert calls[1][0][0] == Gender.F  # Second call with Gender.F

    def test_national_report_by_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_report_by_gender method."""
        mock_licenses_per_year = mocker.patch.object(analyzer, 'licenses_per_year')
        mock_report_by_discipline = mocker.patch.object(analyzer, 'national_report_by_discipline')
        
        mock_licenses_per_year.return_value = 100
        mock_report_by_discipline.return_value = {50: 5, 100: 10}
        
        result = analyzer.national_report_by_gender(Gender.M, 'USA', 2025)
        
        assert 'licenses' in result
        assert result['licenses'] == 100
        assert mock_licenses_per_year.call_count == 1
        assert mock_report_by_discipline.call_count == len(Discipline)

    def test_national_report_by_discipline(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_report_by_discipline method."""
        # Test the method structure and logic without complex SQLAlchemy mocking
        mock_max_rank = mocker.patch.object(analyzer, '_max_rank_for_discipline')
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_session_execute = mocker.patch.object(analyzer.session, 'execute')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        mock_max_rank.return_value = 1000
        
        # Mock the session.execute().scalar() calls
        mock_result = Mock()
        mock_result.scalar.return_value = 5
        mock_session_execute.return_value = mock_result
        
        # Mock the _athletes_under_ranking_query to avoid SQLAlchemy complexity
        # Create a proper mock that can be used in SQLAlchemy queries
        mock_query = select(func.count("*"))
        mocker.patch.object(analyzer, '_athletes_under_ranking_query', return_value=mock_query)
        
        # Test that the method calls the expected dependencies
        result = analyzer.national_report_by_discipline(Discipline.SL, 'USA', Gender.M, 2025)
        
        # Verify the method structure works
        assert isinstance(result, dict)
        assert mock_max_rank.call_count == 1
        assert mock_get_final_points_list.call_count == 1

    def test_get_athletes_under_ranking(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test get_athletes_under_ranking method."""
        # Mock the query method to avoid SQLAlchemy complexity
        mock_query = mocker.patch.object(analyzer, '_athletes_under_ranking_query')
        mock_result = Mock()
        mock_scalars = Mock()
        
        mock_query.return_value = mock_result
        mock_result.scalars.return_value = mock_scalars
        mock_scalars.all.return_value = [Mock(), Mock()]  # Two sample athletes
        
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer.get_athletes_under_ranking(100, 2025, 'USA', Gender.M, Discipline.SL)
        
        assert len(result) == 2
        mock_query.assert_called_once_with(100, 2025, 'USA', Gender.M, Discipline.SL)

    def test_discipline_series_for_seasons(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test discipline_series_for_seasons method."""
        mock_report_by_discipline = mocker.patch.object(analyzer, 'national_report_by_discipline')
        mock_report_by_discipline.return_value = {50: 5, 100: 10}
        
        result = analyzer.discipline_series_for_seasons(Discipline.SL, 'USA', Gender.M, 2024, 2025)
        
        assert 2024 in result
        assert 2025 in result
        assert mock_report_by_discipline.call_count == 2

    def test_national_report_series_for_seasons(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_report_series_for_seasons method."""
        mock_licenses_series = mocker.patch.object(analyzer, 'licenses_series_for_seasons')
        mock_discipline_series = mocker.patch.object(analyzer, 'discipline_series_for_seasons')
        
        mock_licenses_series.return_value = {2024: 100, 2025: 105}
        mock_discipline_series.return_value = {2024: {50: 5}, 2025: {50: 6}}
        
        result = analyzer.national_report_series_for_seasons('USA', Gender.M, 2024, 2025)
        
        assert f"USA licenses - {Gender.M.value}" in result
        assert mock_licenses_series.call_count == 1
        assert mock_discipline_series.call_count == len(Discipline)

    def test_national_series_for_seasons(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_series_for_seasons method."""
        mock_national_report_series = mocker.patch.object(analyzer, 'national_report_series_for_seasons')
        mock_national_report_series.return_value = { "usa licenses - male": {2024: 100, 2025: 105}, "SL-male": {2024: {50: 5}, 2025: {50: 6}}, "GS-male": {2024: {50: 5}, 2025: {50: 6}}, "SG-male": {2024: {50: 5}, 2025: {50: 6}}, "DH-male": {2024: {50: 5}, 2025: {50: 6}}, "AC-male": {2024: {50: 5}, 2025: {50: 6}}, "ALL-male": {2024: {50: 5}, 2025: {50: 6}}}
        
        result = analyzer.national_series_for_seasons('USA', 2024, 2025)
        
        assert mock_national_report_series.call_count == 2  # Called for both M and F

    def test_series_by_year(self, analyzer: NationalStatsAnalyzer, mocker, sample_series_by_year_output) -> None:
        """Test series_by_year method."""
        mock_season_series = mocker.patch.object(analyzer, 'season_series')
        mock_season_series.return_value = sample_series_by_year_output
        
        result = analyzer.series_by_year('USA', 2024, 2025)
        
        assert 2024 in result
        assert 2025 in result
        assert mock_season_series.call_count == 2

    def test_season_series(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series method."""
        mock_season_series_for_gender = mocker.patch.object(analyzer, 'season_series_for_gender')
        mock_season_series_for_gender.return_value = {'total-men': 100, 'sl-50-men': 5}
        
        result = analyzer.season_series('USA', 2025)
        
        assert mock_season_series_for_gender.call_count == 2  # Called for both M and F

    def test_season_series_with_specific_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series method with specific gender."""
        mock_season_series_for_gender = mocker.patch.object(analyzer, 'season_series_for_gender')
        mock_season_series_for_gender.return_value = {'total-men': 100, 'sl-50-men': 5, 'sl-ranked-men': 10}
        
        result = analyzer.season_series('USA', 2025, Gender.M)
        
        assert mock_season_series_for_gender.call_count == 1
        assert mock_season_series_for_gender.call_args[0][2] == Gender.M

    def test_season_series_for_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_gender method."""
        mock_season_series_for_discipline = mocker.patch.object(analyzer, 'season_series_for_discipline')
        mock_season_series_for_discipline.return_value = {'total-men': 100, 'sl-50-men': 5}
        
        result = analyzer.season_series_for_gender('USA', 2025, Gender.M)
        
        assert mock_season_series_for_discipline.call_count == len(Discipline)

    def test_season_series_for_discipline(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_discipline method."""
        mock_licenses_per_year = mocker.patch.object(analyzer, 'licenses_per_year')
        mock_max_rank = mocker.patch.object(analyzer, '_max_rank_for_discipline')
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        
        mock_licenses_per_year.return_value = 100
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        mock_max_rank.return_value = 1000
        
        # Mock the _athletes_under_ranking_query to return a proper SQLAlchemy query
        from sqlalchemy import select, func
        mock_query = select(func.count("*"))
        mocker.patch.object(analyzer, '_athletes_under_ranking_query', return_value=mock_query)
        
        mock_result = Mock()
        mock_result.scalar.return_value = 5
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer.season_series_for_discipline(Discipline.SL, 'USA', 2025, Gender.M)
        
        assert f"total-{Gender.M.value}" in result
        assert "SL-50-Male" in result
        assert "SL-ranked-Male" in result

    def test_licenses_per_year(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test licenses_per_year method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        mock_result = Mock()
        mock_result.scalar.return_value = 100
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer.licenses_per_year(2025, 'USA', Gender.M)
        
        assert result == 100
        mock_get_final_points_list.assert_called_once_with(2025)

    def test_licenses_per_year_no_points_list(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test licenses_per_year method when no points list is found."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = None
        
        with pytest.raises(ValueError, match="No points list found for season 2025"):
            analyzer.licenses_per_year(2025, 'USA', Gender.M)

    def test_licenses_series_for_seasons(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test licenses_series_for_seasons method."""
        mock_licenses_per_year = mocker.patch.object(analyzer, 'licenses_per_year')
        mock_licenses_per_year.return_value = 100
        
        result = analyzer.licenses_series_for_seasons(2024, 2025, 'USA', Gender.M)
        
        assert 2024 in result
        assert 2025 in result
        assert result[2024] == 100
        assert result[2025] == 100
        assert mock_licenses_per_year.call_count == 2

    def test_max_rank_for_discipline(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _max_rank_for_discipline method."""
        mock_get_discipline_rank_column = mocker.patch.object(analyzer, '_get_discipline_rank_column')
        mock_column = Mock()
        mock_get_discipline_rank_column.return_value = mock_column
        
        mock_result = Mock()
        mock_result.scalar.return_value = 1000
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer._max_rank_for_discipline(Discipline.SL, Gender.M, Mock())
        
        assert result == 1000
        mock_get_discipline_rank_column.assert_called_once_with(Discipline.SL)

    def test_get_final_points_list_for_season(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _get_final_points_list_for_season method."""
        mock_result = Mock()
        mock_scalars = Mock()
        mock_first = Mock()
        
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        mock_result.scalars.return_value = mock_scalars
        mock_scalars.first.return_value = Mock()
        
        result = analyzer._get_final_points_list_for_season(2025)
        
        assert result is not None
        # Verify the query was constructed correctly
        call_args = analyzer.session.execute.call_args[0][0]
        assert 'points_lists' in str(call_args)  # The actual table name in SQL
        assert 'season' in str(call_args)

    def test_athletes_under_ranking_query(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _athletes_under_ranking_query method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_discipline_rank_column = mocker.patch.object(analyzer, '_get_discipline_rank_column')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        # Return a proper SQLAlchemy column instead of a Mock
        from src.fis_scraper.database.models import AthletePoints
        mock_get_discipline_rank_column.return_value = AthletePoints.sl_rank
        
        # Mock the session to avoid actual database calls
        mocker.patch.object(analyzer, 'session')
        
        result = analyzer._athletes_under_ranking_query(100, 2025, 'USA', Gender.M, Discipline.SL)
        
        assert result is not None
        mock_get_final_points_list.assert_called_once_with(2025)
        mock_get_discipline_rank_column.assert_called_once_with(Discipline.SL)

    def test_athletes_under_ranking_query_no_points_list(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _athletes_under_ranking_query method when no points list is found."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = None
        
        with pytest.raises(ValueError, match="No points list found for season 2025"):
            analyzer._athletes_under_ranking_query(100, 2025, 'USA', Gender.M, Discipline.SL)

    def test_get_discipline_rank_column(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test _get_discipline_rank_column method."""
        from src.fis_scraper.database.models import AthletePoints
        
        # Test all disciplines
        assert analyzer._get_discipline_rank_column(Discipline.SL) == AthletePoints.sl_rank
        assert analyzer._get_discipline_rank_column(Discipline.GS) == AthletePoints.gs_rank
        assert analyzer._get_discipline_rank_column(Discipline.SG) == AthletePoints.sg_rank
        assert analyzer._get_discipline_rank_column(Discipline.DH) == AthletePoints.dh_rank
        assert analyzer._get_discipline_rank_column(Discipline.AC) == AthletePoints.ac_rank

    def test_get_first_points_list_season(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _get_first_points_list_season method."""
        mock_result = Mock()
        mock_result.scalar.return_value = 2020
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer._get_first_points_list_season()
        
        assert result == 2020

    def test_get_last_points_list_season(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _get_last_points_list_season method."""
        mock_result = Mock()
        mock_result.scalar.return_value = 2025
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        result = analyzer._get_last_points_list_season()
        
        assert result == 2025

    def test_national_dataframe(self, analyzer: NationalStatsAnalyzer, mocker, sample_series_by_year_output) -> None:
        """Test national_dataframe method."""
        mock_series_by_year = mocker.patch.object(analyzer, 'series_by_year')
        mock_series_by_year.return_value = sample_series_by_year_output
        
        result = analyzer.national_dataframe('USA', 2024, 2025)
        
        assert mock_series_by_year.call_count == 1
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (62, 2)

    def test_national_dataframe_by_gender(self, analyzer: NationalStatsAnalyzer, mocker, sample_series_by_year_gender_output) -> None:
        """Test national_dataframe_by_gender method."""
        mock_series_by_year = mocker.patch.object(analyzer, 'series_by_year')
        mock_series_by_year.return_value = sample_series_by_year_gender_output
        
        result = analyzer.national_dataframe_by_gender('USA', Gender.M, 2024, 2025)
        assert mock_series_by_year.call_count == 1
        assert mock_series_by_year.call_args[0][3] == Gender.M
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (31, 2)

class TestNationalStatsAnalyzerDatabase:
    """Test suite for NationalStatsAnalyzer database functionality."""
    
    @pytest.fixture
    def analyzer(self) -> NationalStatsAnalyzer:
        """Create a NationalStatsAnalyzer instance for database testing."""
        return NationalStatsAnalyzer()
    
    @pytest.fixture(scope='class', autouse=True)
    def setup_test_data(self):
        """Set up test data in the database."""
        from src.fis_scraper.scrapers.points_list_scraper import PointsListScraper
        
        # Create test points list
        pls = PointsListScraper()
        with patch.object(PointsListScraper,
                          '_get_filelocation_for_points_list',
                          return_value='tests/data/points_lists/FAL_2025412-abbrev.csv'):
            res = pls.download_and_process_points_list(
                {'sectorcode': 'AL', 'seasoncode': '2025', 'listid': '412',
                 'name': '21st FIS points list 2024/25',
                 'valid_from': date(2025, 3, 12),
                 'valid_to': date(2025, 3, 22)})
            if not res:
                raise Exception("Failed to load points list")
        
        yield
        
        # Clean up test data
        points_lists = pls.session.query(PointsList).filter_by(listid='412')
        points_list_ids = [pl.id for pl in points_lists]
        athlete_points = pls.session.query(AthletePoints).filter(
            AthletePoints.points_list_id.in_(points_list_ids))
        athlete_ids = [ap.athlete_id for ap in athlete_points]
        athlete_points.delete()
        pls.session.query(PointsList).filter(PointsList.id.in_(points_list_ids)).delete()
        pls.session.query(Athlete).filter(Athlete.id.in_(athlete_ids)).delete()
        pls.session.commit()

    def test_licenses_per_year_with_real_data(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test licenses_per_year with real database data."""
        result = analyzer.licenses_per_year(2025, 'ESP', Gender.M)
        assert isinstance(result, int)
        assert result >= 0

    def test_get_athletes_under_ranking_with_real_data(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test get_athletes_under_ranking with real database data."""
        athletes = analyzer.get_athletes_under_ranking(100, 2025, 'ESP', Gender.M, Discipline.SL)
        assert isinstance(athletes, list)
        assert all(isinstance(athlete, Athlete) for athlete in athletes)

    def test_report_by_discipline_with_real_data(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test report_by_discipline with real database data."""
        result = analyzer.national_report_by_discipline(Discipline.SL, 'ESP', Gender.M, 2025)
        assert isinstance(result, dict)
        assert all(isinstance(k, int) for k in result.keys())
        assert all(isinstance(v, int) for v in result.values())


class TestNationalStatsAnalyzerIntegration:
    """Integration tests for NationalStatsAnalyzer."""
    
    @pytest.fixture
    def analyzer(self) -> NationalStatsAnalyzer:
        """Create a NationalStatsAnalyzer instance for integration testing."""
        return NationalStatsAnalyzer()
    
    @pytest.mark.skip(reason="Not implemented")
    def test_end_to_end_national_report(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test end-to-end national report generation."""
        # This would be an integration test that uses real database data

        # In a real integration test, we would:
        # 1. Ensure test data exists in database
        # 2. Generate a national report
        # 3. Verify the report structure and data
        # 4. Check that all expected keys are present
        # 5. Verify data consistency across different methods
        assert False

    @pytest.mark.skip(reason="Not implemented")
    def test_error_handling_integration(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test error handling in integration scenarios."""
        # This would test:
        # 1. Handling of missing points lists
        # 2. Handling of empty result sets
        # in particular -- no ranked athletes in a discipline for a season
        # 3. Handling of invalid parameters
        # 4. Database connection issues
        assert False


class TestNationalStatsAnalyzerEdgeCases:
    """Test edge cases and error conditions for NationalStatsAnalyzer."""
    
    @pytest.fixture
    def analyzer(self) -> NationalStatsAnalyzer:
        """Create a NationalStatsAnalyzer instance for edge case testing."""
        return NationalStatsAnalyzer()
    
    def test_empty_database_results(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test behavior when database queries return empty results."""
        mock_result = Mock()
        mock_result.scalar.return_value = 0
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = Mock()
        
        result = analyzer.licenses_per_year(2025, 'NONEXISTENT', Gender.M)
        assert result == 0

    def test_invalid_nation_code(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test behavior with invalid nation codes."""
        # This should not raise an exception, just return 0 or empty results
        mock_result = Mock()
        mock_result.scalar.return_value = 0
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = Mock()
        
        result = analyzer.licenses_per_year(2025, 'INVALID', Gender.M)
        assert result == 0

    def test_future_season_handling(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test behavior when querying for future seasons."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = None
        
        with pytest.raises(ValueError, match="No points list found for season 2030"):
            analyzer.licenses_per_year(2030, 'USA', Gender.M)

    def test_discipline_all_handling(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test behavior when using Discipline.ALL."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = Mock()
        
        mock_result = Mock()
        mock_result.scalar.return_value = 5
        mocker.patch.object(analyzer.session, 'execute', return_value=mock_result)
        
        # This should not raise an exception
        result = analyzer.national_report_by_discipline(Discipline.ALL, 'USA', Gender.M, 2025)
        assert isinstance(result, dict) 