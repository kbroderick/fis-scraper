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

    @pytest.fixture
    def sample_athletes_dataframe(self) -> pd.DataFrame:
        """Create a sample athletes DataFrame for testing."""
        return pd.DataFrame({
            'nation_code': ['USA', 'USA', 'SUI', 'SUI'],
            'gender': [Gender.M, Gender.F, Gender.M, Gender.F],
            'sl_rank': [10, 15, 20, 25],
            'gs_rank': [30, 35, 40, 45],
            'sg_rank': [50, 55, 60, 65],
            'dh_rank': [70, 75, 80, 85],
            'ac_rank': [90, 95, 100, 105],
            'sl_status': ['*', '*', '*', '*'],
            'gs_status': ['*', '*', '*', '*'],
            'sg_status': ['*', '*', '*', '*'],
            'dh_status': ['*', '*', '*', '*'],
            'ac_status': ['*', '*', '*', '*']
        })

    def test_init(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test NationalStatsAnalyzer initialization."""
        assert analyzer.session is not None
        assert analyzer.RANKING_DEFAULTS == [50, 100, 300, 500, 1000, 2000, 3000]
        assert analyzer.cached_athletes_on_points_list == {}

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

    def test_national_dataframe(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_dataframe method."""
        mock_series_by_year = mocker.patch.object(analyzer, 'series_by_year')
        mock_df = pd.DataFrame({
            'nation': ['USA', 'USA'],
            'discipline': [Discipline.SL, Discipline.GS],
            'gender': [Gender.M, Gender.M],
            'season': [2024, 2024],
            'ranking': [50, 50],
            'count': [5, 10]
        })
        mock_series_by_year.return_value = mock_df
        
        result = analyzer.national_dataframe('USA', 2024, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert 'season' in result.index.names
        mock_series_by_year.assert_called_once_with('USA', 2024, 2025)

    def test_national_dataframe_by_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test national_dataframe_by_gender method."""
        mock_series_by_year = mocker.patch.object(analyzer, 'series_by_year')
        mock_df = pd.DataFrame({
            'nation': ['USA', 'USA'],
            'discipline': [Discipline.SL, Discipline.GS],
            'gender': [Gender.M, Gender.M],
            'season': [2024, 2024],
            'ranking': [50, 50],
            'count': [5, 10]
        })
        mock_series_by_year.return_value = mock_df
        
        result = analyzer.national_dataframe_by_gender('USA', Gender.M, 2024, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert 'season' in result.index.names
        assert 'gender' in result.index.names
        mock_series_by_year.assert_called_once_with('USA', 2024, 2025, Gender.M)

    def test_series_by_year(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test series_by_year method."""
        mock_season_series = mocker.patch.object(analyzer, 'season_series')
        mock_df1 = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_df2 = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2025], 'ranking': [50], 'count': [6]})
        mock_season_series.side_effect = [mock_df1, mock_df2]
        
        result = analyzer.series_by_year('USA', 2024, 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert mock_season_series.call_count == 2
        assert mock_season_series.call_args_list[0][0] == ('USA', 2024, Gender.A)
        assert mock_season_series.call_args_list[1][0] == ('USA', 2025, Gender.A)

    def test_series_by_year_with_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test series_by_year method with specific gender."""
        mock_season_series = mocker.patch.object(analyzer, 'season_series')
        mock_df1 = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_df2 = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2025], 'ranking': [50], 'count': [6]})
        mock_season_series.side_effect = [mock_df1, mock_df2]
        
        result = analyzer.series_by_year('USA', 2024, 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        assert mock_season_series.call_count == 2
        assert mock_season_series.call_args_list[0][0] == ('USA', 2024, Gender.M)
        assert mock_season_series.call_args_list[1][0] == ('USA', 2025, Gender.M)

    def test_season_series(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series method."""
        mock_season_series_for_gender = mocker.patch.object(analyzer, 'season_series_for_gender')
        mock_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_season_series_for_gender.return_value = mock_df
        
        result = analyzer.season_series('USA', 2025)
        
        assert isinstance(result, pd.DataFrame)
        assert mock_season_series_for_gender.call_count == 2  # Called for both M and F

    def test_season_series_with_specific_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series method with specific gender."""
        mock_season_series_for_gender = mocker.patch.object(analyzer, 'season_series_for_gender')
        mock_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_season_series_for_gender.return_value = mock_df
        
        result = analyzer.season_series('USA', 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        assert mock_season_series_for_gender.call_count == 1
        assert mock_season_series_for_gender.call_args[0][2] == Gender.M

    def test_season_series_for_gender(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_gender method."""
        mock_season_series_for_discipline = mocker.patch.object(analyzer, 'season_series_for_discipline')
        mock_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_season_series_for_discipline.return_value = mock_df
        
        result = analyzer.season_series_for_gender('USA', 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        assert mock_season_series_for_discipline.call_count == len(Discipline)

    def test_season_series_for_discipline(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_discipline method."""
        mock_max_rank = mocker.patch.object(analyzer, 'max_rank_for_discipline')
        mock_build_results = mocker.patch.object(analyzer, '_build_results_for_season')
        
        mock_max_rank.return_value = 1000
        mock_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_build_results.return_value = mock_df
        
        result = analyzer.season_series_for_discipline(Discipline.SL, 'USA', 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        mock_max_rank.assert_called_once_with(Discipline.SL, Gender.M, 2025)
        mock_build_results.assert_called_once()

    def test_season_series_for_discipline_all(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_discipline method with Discipline.ALL."""
        mock_total_licenses = mocker.patch.object(analyzer, '_total_licenses_for_season')
        mock_build_results = mocker.patch.object(analyzer, '_build_results_for_season')
        
        mock_total_licenses.return_value = 1000
        mock_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.ALL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_build_results.return_value = mock_df
        
        result = analyzer.season_series_for_discipline(Discipline.ALL, 'USA', 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        mock_total_licenses.assert_called_once_with(2025, Gender.M)
        mock_build_results.assert_called_once()

    def test_season_series_for_discipline_nan_max_rank(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test season_series_for_discipline method when max_rank is NaN."""
        mock_max_rank = mocker.patch.object(analyzer, 'max_rank_for_discipline')
        
        mock_max_rank.return_value = float('nan')
        
        result = analyzer.season_series_for_discipline(Discipline.SL, 'USA', 2025, Gender.M)
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert list(result.columns) == ['nation', 'discipline', 'gender', 'season', 'ranking', 'count']

    def test_build_results_for_season(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _build_results_for_season method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        mock_build_one_discipline = mocker.patch.object(analyzer, '_build_results_for_one_discipline')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA'],
            'gender': [Gender.M, Gender.M],
            'sl_rank': [10, 20],
            'gs_rank': [30, 40],
            'sg_rank': [50, 60],
            'dh_rank': [70, 80],
            'ac_rank': [90, 100]
        })
        mock_athletes_on_points_list.return_value = mock_df
        
        mock_result_df = pd.DataFrame({'nation': ['USA'], 'discipline': [Discipline.SL], 'gender': [Gender.M], 'season': [2024], 'ranking': [50], 'count': [5]})
        mock_build_one_discipline.return_value = mock_result_df
        
        result = analyzer._build_results_for_season('USA', 2024, Gender.M, Discipline.SL, 1000, pd.DataFrame())
        
        assert isinstance(result, pd.DataFrame)
        mock_get_final_points_list.assert_called_once_with(2024)
        mock_athletes_on_points_list.assert_called_once_with(mock_points_list)
        mock_build_one_discipline.assert_called_once()

    def test_build_results_for_one_discipline(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test _build_results_for_one_discipline method."""
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA'],
            'gender': [Gender.M, Gender.M],
            'sl_rank': [10, 200],  # One under 50, one not
            'gs_rank': [30, 40],
            'sg_rank': [50, 60],
            'dh_rank': [70, 80],
            'ac_rank': [90, 100]
        })
        
        result = analyzer._build_results_for_one_discipline('USA', 2024, Gender.M, Discipline.SL, 1000, mock_df)
        
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['nation', 'discipline', 'gender', 'season', 'ranking', 'count']
        assert len(result) > 0
        # Should have entries for each ranking in RANKING_DEFAULTS that are less than max_rank, plus the max_rank
        # In this case, max_rank is 1000, so we should have entries for 50, 100, 300, 500, 1000
        assert len(result) == 5

    def test_build_results_for_all_disciplines(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test _build_results_for_all_disciplines method."""
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA'],
            'gender': [Gender.M, Gender.M],
            'sl_rank': [10, 200],
            'gs_rank': [30, 40],
            'sg_rank': [50, 60],
            'dh_rank': [70, 80],
            'ac_rank': [90, 100]
        })
        
        result = analyzer._build_results_for_all_disciplines('USA', 2024, Gender.M, 1000, mock_df)
        
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['nation', 'discipline', 'gender', 'season', 'ranking', 'count']
        assert len(result) > 0
        # Should have entries for each ranking in RANKING_DEFAULTS that are less than max_rank, plus the max_rank
        # In this case, max_rank is 1000, so we should have entries for 50, 100, 300, 500, 1000
        assert len(result) == 5
        # All disciplines should be Discipline.ALL
        assert all(result['discipline'] == Discipline.ALL)

    def test_licenses_per_year(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test licenses_per_year method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA', 'SUI'],
            'gender': [Gender.M, Gender.M, Gender.M]
        })
        mock_athletes_on_points_list.return_value = mock_df
        
        result = analyzer.licenses_per_year(2025, 'USA', Gender.M)
        
        assert result == 2  # Should count only USA athletes
        mock_get_final_points_list.assert_called_once_with(2025)
        mock_athletes_on_points_list.assert_called_once_with(mock_points_list)

    def test_licenses_per_year_no_points_list(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test licenses_per_year method when no points list is found."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = None
        
        with pytest.raises(ValueError, match="No points list found for season 2025"):
            analyzer.licenses_per_year(2025, 'USA', Gender.M)

    def test_athletes_on_points_list(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test athletes_on_points_list method."""
        mock_points_list = Mock()
        mock_read_sql_query = mocker.patch('pandas.read_sql_query')
        
        mock_df = pd.DataFrame({'nation_code': ['USA'], 'gender': [Gender.M]})
        mock_read_sql_query.return_value = mock_df
        
        result = analyzer.athletes_on_points_list(mock_points_list)
        
        assert isinstance(result, pd.DataFrame)
        mock_read_sql_query.assert_called_once()
        # Should be cached
        assert mock_points_list in analyzer.cached_athletes_on_points_list

    def test_athletes_on_points_list_cached(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test athletes_on_points_list method with cached data."""
        mock_points_list = Mock()
        mock_df = pd.DataFrame({'nation_code': ['USA'], 'gender': [Gender.M]})
        analyzer.cached_athletes_on_points_list[mock_points_list] = mock_df
        
        result = analyzer.athletes_on_points_list(mock_points_list)
        
        assert isinstance(result, pd.DataFrame)
        assert result.equals(mock_df)

    def test_total_licenses_for_season(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _total_licenses_for_season method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA', 'SUI'],
            'gender': [Gender.M, Gender.F, Gender.M]
        })
        mock_athletes_on_points_list.return_value = mock_df
        
        result = analyzer._total_licenses_for_season(2025, Gender.M)
        
        assert result == 2  # Should count only male athletes
        mock_get_final_points_list.assert_called_once_with(2025)
        mock_athletes_on_points_list.assert_called_once_with(mock_points_list)

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
        """Test max_rank_for_discipline method."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_max_rank_one_discipline = mocker.patch.object(analyzer, '_max_rank_for_one_discipline')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        mock_max_rank_one_discipline.return_value = 1000
        
        result = analyzer.max_rank_for_discipline(Discipline.SL, Gender.M, 2025)
        
        assert result == 1000
        mock_get_final_points_list.assert_called_once_with(2025)
        mock_max_rank_one_discipline.assert_called_once_with(Discipline.SL, Gender.M, mock_points_list)

    def test_max_rank_for_discipline_all(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test max_rank_for_discipline method with Discipline.ALL."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_max_rank_all_disciplines = mocker.patch.object(analyzer, '_max_rank_for_all_disciplines')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        mock_max_rank_all_disciplines.return_value = 1000
        
        result = analyzer.max_rank_for_discipline(Discipline.ALL, Gender.M, 2025)
        
        assert result == 1000
        mock_get_final_points_list.assert_called_once_with(2025)
        mock_max_rank_all_disciplines.assert_called_once_with(Gender.M, mock_points_list)

    def test_max_rank_for_discipline_no_points_list(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test max_rank_for_discipline method when no points list is found."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_get_final_points_list.return_value = None
        
        with pytest.raises(ValueError, match="No points list found for season 2025"):
            analyzer.max_rank_for_discipline(Discipline.SL, Gender.M, 2025)

    def test_max_rank_for_one_discipline(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _max_rank_for_one_discipline method."""
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        mock_get_discipline_rank_column = mocker.patch.object(analyzer, '_get_discipline_rank_column')
        
        mock_points_list = Mock()
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA'],
            'gender': [Gender.M, Gender.M],
            'sl_rank': [10, 20]
        })
        mock_athletes_on_points_list.return_value = mock_df
        
        from src.fis_scraper.database.models import AthletePoints
        mock_get_discipline_rank_column.return_value = AthletePoints.sl_rank
        
        result = analyzer._max_rank_for_one_discipline(Discipline.SL, Gender.M, mock_points_list)
        
        assert result == 20  # Should return max of sl_rank
        mock_athletes_on_points_list.assert_called_once_with(mock_points_list)
        mock_get_discipline_rank_column.assert_called_once_with(Discipline.SL)

    def test_max_rank_for_all_disciplines(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _max_rank_for_all_disciplines method."""
        mock_max_rank_one_discipline = mocker.patch.object(analyzer, '_max_rank_for_one_discipline')
        
        # Mock different max ranks for different disciplines
        mock_max_rank_one_discipline.side_effect = [100, 200, 150, 300, 250]
        
        mock_points_list = Mock()
        result = analyzer._max_rank_for_all_disciplines(Gender.M, mock_points_list)
        
        assert result == 300  # Should return the maximum
        assert mock_max_rank_one_discipline.call_count == 5  # Called for each discipline

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

    def test_athletes_under_ranking_query_all_disciplines(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test _athletes_under_ranking_query method with Discipline.ALL."""
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        # Mock the session to avoid actual database calls
        mocker.patch.object(analyzer, 'session')
        
        result = analyzer._athletes_under_ranking_query(100, 2025, 'USA', Gender.M, Discipline.ALL)
        
        assert result is not None
        mock_get_final_points_list.assert_called_once_with(2025)

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

    def test_athletes_on_points_list_with_real_data(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test athletes_on_points_list with real database data."""
        points_list = analyzer._get_final_points_list_for_season(2025)
        if points_list:
            result = analyzer.athletes_on_points_list(points_list)
            assert isinstance(result, pd.DataFrame)
            assert not result.empty


class TestNationalStatsAnalyzerIntegration:
    """Integration tests for NationalStatsAnalyzer."""
    
    @pytest.fixture
    def analyzer(self) -> NationalStatsAnalyzer:
        """Create a NationalStatsAnalyzer instance for integration testing."""
        return NationalStatsAnalyzer()
    
    @pytest.mark.skip(reason="Not implemented")
    def test_end_to_end_series_generation(self, analyzer: NationalStatsAnalyzer) -> None:
        """Test end-to-end series generation."""
        # This would be an integration test that uses real database data

        # In a real integration test, we would:
        # 1. Ensure test data exists in database
        # 2. Generate a series by year
        # 3. Verify the series structure and data
        # 4. Check that all expected columns are present
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
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        # Return empty DataFrame
        mock_df = pd.DataFrame(columns=['nation_code', 'gender'])
        mock_athletes_on_points_list.return_value = mock_df
        
        result = analyzer.licenses_per_year(2025, 'NONEXISTENT', Gender.M)
        assert result == 0

    def test_invalid_nation_code(self, analyzer: NationalStatsAnalyzer, mocker) -> None:
        """Test behavior with invalid nation codes."""
        # This should not raise an exception, just return 0 or empty results
        mock_get_final_points_list = mocker.patch.object(analyzer, '_get_final_points_list_for_season')
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        # Return DataFrame with no matching nation codes
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'SUI'],
            'gender': [Gender.M, Gender.M]
        })
        mock_athletes_on_points_list.return_value = mock_df
        
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
        mock_athletes_on_points_list = mocker.patch.object(analyzer, 'athletes_on_points_list')
        mock_max_rank_one_discipline = mocker.patch.object(analyzer, '_max_rank_for_one_discipline')
        
        mock_points_list = Mock()
        mock_get_final_points_list.return_value = mock_points_list
        
        # Return sample DataFrame
        mock_df = pd.DataFrame({
            'nation_code': ['USA', 'USA'],
            'gender': [Gender.M, Gender.M],
            'sl_rank': [10, 20],
            'gs_rank': [30, 40],
            'sg_rank': [50, 60],
            'dh_rank': [70, 80],
            'ac_rank': [90, 100]
        })
        mock_athletes_on_points_list.return_value = mock_df
        mock_max_rank_one_discipline.return_value = 100
        
        # This should not raise an exception
        result = analyzer.max_rank_for_discipline(Discipline.ALL, Gender.M, 2025)
        assert isinstance(result, int) 