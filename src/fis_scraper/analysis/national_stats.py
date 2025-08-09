import logging
from typing import Any, Dict, List
from enum import Enum
import numpy as np
import pandas as pd
from pandas import DataFrame
from sqlalchemy import or_, and_,select, Column, Select, func
from sqlalchemy.orm import Session
from ..database.connection import get_session
from ..database.models import Athlete, AthletePoints, Gender, Discipline, PointsList
from ..scrapers.race_results_scraper import RaceResultsScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class NationalStatsAnalyzer:
    """Analyzer for national stats data including race results and FIS points.
    
    This class provides methods to analyze data at larger scale, e.g.
    by national affiliation.
    """
    
    RANKING_DEFAULTS = [50, 100, 300, 500, 1000, 2000, 3000]

    RANKING_QUERY_DTYPE = {
        'nation_code': 'category',
        'gender': 'category',
        'sl_status': 'category',
        'gs_status': 'category',
        'sg_status': 'category',
        'dh_status': 'category',
        'ac_status': 'category'
        }

    cached_athletes_on_points_list: Dict[PointsList, DataFrame] = {}

    def __init__(self) -> None:
        """Initialize the NationalStatsAnalyzer with a database session."""
        self.session: Session = get_session()

    def get_athletes_under_ranking(self,
                                   ranking: int,
                                   season: int = None,
                                   nation: str = None,
                                   gender: Gender = Gender.A,
                                   discipline: Discipline = Discipline.ALL) -> List[Athlete]:
        """Get athletes under a given ranking for a given nation, gender, and discipline.
        
        Args:
            ranking: Ranking to filter by (get all athletes better than this ranking)
            season: Season to filter by (default: current season)
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            gender: Gender to filter by (default: all)
            discipline: Discipline to filter by (default: all)
            
        Returns:
            List of athletes under the given ranking
        """
        query = self._athletes_under_ranking_query(ranking, season, nation, gender, discipline)
        result = self.session.execute(query)
        return result.scalars().all()
        
    def national_dataframe(self, nation: str, start_season: int, end_season: int) -> pd.DataFrame:
        """ series_by_year indexed by season"""
        return self.series_by_year(nation, start_season, end_season).set_index('season')
    
    def national_dataframe_by_gender(self, nation: str, gender: Gender, start_season: int, end_season: int) -> pd.DataFrame:
        """ series_by_year indexed by season and gender"""
        return self.series_by_year(nation, start_season, end_season, gender).set_index(['season', 'gender'])

    def series_by_year(self, nation: str, start_season: int, end_season: int, gender: Gender = Gender.A) -> DataFrame:
        """
        Get the number of athletes under a given ranking for a set of
        seasons, returned as DataFrame-ready dictionary with each series
        as a column.
        
        Args:
            nation: three-letter national code (e.g. "USA", "SUI")
            start_season: First season to include
            end_season: Last season to include
            gender: Gender to filter by (default: all)

        Returns:
            DataFrame with the following columns:
            - nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            - discipline: Discipline for results
            - gender: Gender for results
            - season: Season for results
            - ranking: Ranking for results
            - count: Number of athletes under a given ranking with
            the given nation, gender, discipline and season.
        """
        results = []
        for season in range(start_season, end_season + 1):
            results.append(self.season_series(nation, season, gender))
        return pd.concat(results)
    
    def season_series(self, nation: str, season: int, gender: Gender = Gender.A) -> DataFrame:
        """Get the number of athletes under a given ranking for a given nation for a given season.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by (default: all)

        Returns:
            DataFrame with the following columns:
            - nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            - discipline: Discipline for results
            - gender: Gender for results
            - season: Season for results
            - ranking: Ranking for results
            - count: Number of athletes under a given ranking with
            the given nation, gender, discipline and season.            
        """
        results = []
        # NB: we only need to do this for M and F, as the total is the sum of the two
        if gender != Gender.A:
            results.append(self.season_series_for_gender(nation, season, gender))
        else:
            for gender in [Gender.M, Gender.F]:
                results.append(self.season_series_for_gender(nation, season, gender))
        return pd.concat(results, ignore_index=True)
    
    def season_series_for_gender(self, nation: str, season: int, gender: Gender) -> DataFrame:
        """
        Get the number of athletes under a given ranking for a given
        nation for a given season and gender.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by

        Returns:
            DataFrame with the following columns:
            - nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            - discipline: Discipline for results
            - gender: Gender for results
            - season: Season for results
            - ranking: Ranking for results
            - count: Number of athletes under a given ranking with
            the given nation, gender, discipline and season.
        """
        logger.info(f"Getting season series for {nation} {season} {gender}")
        results = []
        for discipline in Discipline:
            results.append(self.season_series_for_discipline(discipline, nation, season, gender))
        return pd.concat(results, ignore_index=True)

    def season_series_for_discipline(self, discipline: Discipline, nation: str, season: int, gender: Gender) -> DataFrame:
        """
        Get the number of athletes under a given ranking for a given nation for a given season and discipline.

        Args:
            discipline: Discipline to filter by
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by

        Returns:
            DataFrame with the following columns:
            - nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            - discipline: Discipline for results
            - gender: Gender for results
            - season: Season for results
            - ranking: Ranking for results
            - count: Number of athletes under a given ranking with
            the given nation, gender, discipline and season.
            NOTE: as as special case, the max rank value corresponds to
            the total number of athletes ranked in that discipline for
            that season.

            e.g.
            nation  discipline  gender  season  ranking  count
            USA     SL          M     2024      9850      1345
            USA       SL      M     2024      50      3
            USA       SL      M     100      7
            USA       SL      M     300     20
            USA       SL      M     500     40
            USA       SL      M    1000    131
            USA       SL      M    2000    354
        """
        logger.debug(f"Getting season series for {discipline} {nation} {season} {gender}")
        results = pd.DataFrame(columns=['nation', 'discipline', 'gender', 'season', 'ranking',
                                        'count'])

        if discipline == Discipline.ALL:
            max_rank = self._total_licenses_for_season(season, gender)
            if np.isnan(max_rank):
                return results
        else:
            max_rank = self.max_rank_for_discipline(discipline, gender, season)
            if np.isnan(max_rank):
                return results
            # NB: we get nan max rank if no athletes are ranked in this discipline for this season,
            # which may happen if the discipline was not contested in that season
            # (e.g. AC on earlier lists)

        return self._build_results_for_season(nation, season, gender, discipline, max_rank, results)

    def _build_results_for_season(self, nation: str, season: int, gender: Gender,
                                  discipline: Discipline, max_rank: int, 
                                  results: DataFrame) -> DataFrame:
        """Build the results for a given season."""

        athletes = self.athletes_on_points_list(self._get_final_points_list_for_season(season))
        athletes = athletes[(athletes['gender'] == gender) & (athletes['nation_code'] == nation)]

        if discipline == Discipline.ALL:
            return self._build_results_for_all_disciplines(nation, season, gender, max_rank, athletes)
        else:
            return self._build_results_for_one_discipline(nation, season, gender, discipline, max_rank, athletes)

    def _build_results_for_one_discipline(self, nation: str, season: int, gender: Gender,
                                          discipline: Discipline, max_rank: int, athletes: DataFrame) -> DataFrame:
        """Build the results for a given season and discipline."""

        tmp_results = []
        for ranking in self.RANKING_DEFAULTS:
            if ranking < max_rank:
                count = athletes[athletes[self._get_discipline_rank_column(discipline).name] <= ranking].shape[0]
                tmp_results.append([nation, discipline, gender, season, ranking, count])

        count = athletes[athletes[self._get_discipline_rank_column(discipline).name] <= max_rank].shape[0]
        tmp_results.append([nation, discipline, gender, season, int(max_rank), count])

        return pd.DataFrame(tmp_results,
                            columns=['nation', 'discipline', 'gender', 'season', 'ranking', 'count'])
    
    def _build_results_for_all_disciplines(self, nation: str, season: int, gender: Gender,
                                           max_rank: int, athletes: DataFrame) -> DataFrame:
        """
        Build the results for a given season and all disciplines.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by
            max_rank: Maximum rank to include
            results: DataFrame to store results
            athletes: DataFrame of athletes, already filtered by nation,
                gender and season

        Returns:
            DataFrame with the results
        """
        tmp_results = []
        for ranking in self.RANKING_DEFAULTS:
            if ranking < max_rank:
                count = athletes[(athletes['sl_rank'] <= ranking) |
                                 (athletes['gs_rank'] <= ranking) |
                                 (athletes['sg_rank'] <= ranking) |
                                 (athletes['dh_rank'] <= ranking) |
                                 (athletes['ac_rank'] <= ranking)].shape[0]
                tmp_results.append([nation, Discipline.ALL, gender, season, ranking, count])

        # add the total number of athletes ranked in all disciplines
        count = athletes[(athletes['sl_rank'] <= max_rank) |
                         (athletes['gs_rank'] <= max_rank) |
                         (athletes['sg_rank'] <= max_rank) |
                         (athletes['dh_rank'] <= max_rank) |
                         (athletes['ac_rank'] <= max_rank)].shape[0]
        tmp_results.append([nation, Discipline.ALL, gender, season, int(max_rank), count])

        return pd.DataFrame(tmp_results,
                            columns=['nation', 'discipline', 'gender', 'season', 'ranking', 'count'])
    
    def licenses_per_year(self, season: int, nation: str = None, gender: Gender = Gender.A) -> Dict[int, int]:
        """Get the number of licenses per year for a given nation and gender."""
        points_list = self._get_final_points_list_for_season(season)
        if points_list is None:
            raise ValueError(f"No points list found for season {season}")

        athletes = self.athletes_on_points_list(points_list)
        
        if nation:
            athletes = athletes[athletes['nation_code'] == nation]
        if gender != Gender.A:
            athletes = athletes[athletes['gender'] == gender]

        return athletes.shape[0]
    
    def athletes_on_points_list(self, points_list: PointsList) -> DataFrame:
        """Get a DataFrame of athletes on a given points list."""
        if points_list not in self.cached_athletes_on_points_list:
            query = select(Athlete, AthletePoints).join(AthletePoints).where(AthletePoints.points_list == points_list)
            self.cached_athletes_on_points_list[points_list] = pd.read_sql_query(query, self.session.bind, dtype=self.RANKING_QUERY_DTYPE)
        return self.cached_athletes_on_points_list[points_list]
    
    def _total_licenses_for_season(self, season: int, gender: Gender) -> int:
        """Get the total number of licenses for a given season and gender."""
        athletes = self.athletes_on_points_list(self._get_final_points_list_for_season(season))
        if gender != Gender.A:
            return athletes[athletes['gender'] == gender].shape[0]
        return athletes.shape[0]
    
    def licenses_series_for_seasons(self, start_season: int, end_season: int, nation: str = None,
                                    gender: Gender = Gender.A) -> Dict[int, int]:
        """Get the number of licenses per year for a given nation and gender for a given range of seasons."""
        results = {}
        for season in range(start_season, end_season + 1):
            results[season] = self.licenses_per_year(season, nation, gender)
        return results

    def max_rank_for_discipline(self, discipline: Discipline, gender: Gender, season: int) -> int:
        """Get the maximum rank for a given discipline and season."""
        points_list = self._get_final_points_list_for_season(season)
        if points_list is None:
            raise ValueError(f"No points list found for season {season}")
        if discipline == Discipline.ALL:
            return self._max_rank_for_all_disciplines(gender, points_list)
        return self._max_rank_for_one_discipline(discipline, gender, points_list)

    def _max_rank_for_one_discipline(self, discipline: Discipline, gender: Gender,
                                 points_list: PointsList) -> int:
        """Get the maximum rank for a given discipline and points list."""
        athletes = self.athletes_on_points_list(points_list)
        athletes = athletes[athletes['gender'] == gender]
        return athletes[self._get_discipline_rank_column(discipline).name].max()

    def _max_rank_for_all_disciplines(self, gender: Gender, points_list: PointsList) -> int:
        """Get the maximum rank for all disciplines for a given points list."""
        max_rank = 0
        for discipline in [Discipline.SL, Discipline.GS, Discipline.SG, Discipline.DH, Discipline.AC]:
            max_rank = max(max_rank, self._max_rank_for_one_discipline(discipline, gender, points_list))
        return max_rank

    def _get_final_points_list_for_season(self, season: int) -> PointsList:
        """Get the final points list for a given season.
        
        Args:
            season: Season to filter by
            
        Returns:
            PointsList object
        """
        q = select(PointsList).where(PointsList.season == str(season)).order_by(PointsList.listid.desc())
        return self.session.execute(q).scalars().first()

    def _athletes_under_ranking_query(self,
                                      ranking: int,
                                      season: int = None,
                                      nation: str = None,
                                      gender: Gender = Gender.A,
                                      discipline: Discipline = Discipline.ALL) -> Select:
        """Get a query for athletes under a given ranking for a given nation, gender, and discipline."""
        if season is None:
            season = RaceResultsScraper.get_current_season()

        points_list = self._get_final_points_list_for_season(season)
        if points_list is None:
            raise ValueError(f"No points list found for season {season}")

        query = select(Athlete).join(AthletePoints).where(AthletePoints.points_list == points_list)

        if nation:
            query = query.where(Athlete.nation_code == nation)
        if gender != Gender.A:
            query = query.where(Athlete.gender == gender)
        if discipline == Discipline.ALL:
            query = query.where(or_(
                AthletePoints.sl_rank <= ranking,
                AthletePoints.gs_rank <= ranking,
                AthletePoints.sg_rank <= ranking,
                AthletePoints.dh_rank <= ranking,
                AthletePoints.ac_rank <= ranking
            ))
        else:
            query = query.where(self._get_discipline_rank_column(discipline) <= ranking)
        return query
    
    def _get_discipline_rank_column(self, discipline: Discipline) -> Column:
        """Get the column name for a given discipline.
        
        Args:
            discipline: Discipline to filter by
            
        Returns:
            Column for the discipline rank
        """
        discipline_map = {
            Discipline.SL: AthletePoints.sl_rank,
            Discipline.GS: AthletePoints.gs_rank,
            Discipline.SG: AthletePoints.sg_rank,
            Discipline.DH: AthletePoints.dh_rank,
            Discipline.AC: AthletePoints.ac_rank
        }
        return discipline_map[discipline]

    def _get_first_points_list_season(self) -> int:
        """Get the first season for which there is a points list."""
        return self.session.execute(
            select(func.min(PointsList.season)).select_from(PointsList)
        ).scalar()
    
    def _get_last_points_list_season(self) -> int:
        """Get the last season for which there is a points list."""
        return self.session.execute(
            select(func.max(PointsList.season)).select_from(PointsList)
        ).scalar()