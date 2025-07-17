import logging
from typing import Any, Dict, List
from enum import Enum
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

    def __init__(self) -> None:
        """Initialize the NationalStatsAnalyzer with a database session."""
        self.session: Session = get_session()

    def national_report(self, nation: str, season: int = None) -> Dict[str, Dict[int, int]]:
        """Generate a national report for a given nation and season.
        
        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by (default: current season)
        """
        results = {}
        for gender in [Gender.M, Gender.F]:
            results[gender] = self.report_by_gender(gender, nation, season)
        return results

    def report_by_gender(self, gender: Gender, nation: str, season: int = None) -> Dict[str, Dict[int, int]]:
        """Generate a report by gender for a given nation and season."""
        results = {}
        if season is None:
            season = RaceResultsScraper.get_current_season()
        results["licenses"] = self.licenses_per_year(season, nation, gender)

        for discipline in Discipline:
            results[discipline.name] = self.report_by_discipline(discipline, nation, gender, season)
        return results

    def report_by_discipline(self, discipline: Discipline,
                             nation: str, gender: Gender, season: int = None) -> Dict[int, int]:
        """Generate a report by discipline for a given nation, gender, and season."""
        if season is None:
            season = RaceResultsScraper.get_current_season()

        results = {}
        max_rank = None

        if discipline != Discipline.ALL:
            max_rank = self._max_rank_for_discipline(discipline, gender, self._get_final_points_list_for_season(season))
            if max_rank is not None:
                results[max_rank] = self.session.execute(
                    select(func.count("*")).select_from(
                        self._athletes_under_ranking_query(max_rank, season, nation, gender, discipline).subquery()
                    )
                ).scalar()

        for ranking in self.RANKING_DEFAULTS:
            if max_rank and ranking > max_rank:
                break  # no need to continue if we're past total ranked athletes in this discipline
            athletes = self._athletes_under_ranking_query(ranking, season, nation, gender, discipline)
            results[ranking] = self.session.execute(
                select(func.count("*")).select_from(athletes.subquery())
            ).scalar()

        return results

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
    
    def discipline_series_for_seasons(self, discipline: Discipline, nation: str, gender: Gender,
                                    start_season: int, end_season: int) -> Dict[int, int]:
        """
        Get the number of athletes under a given ranking for a given
        nation, gender, and discipline for a given range of seasons.

        Args:
            discipline: Discipline to filter by
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            gender: Gender to filter by
            start_season: First season to include
            end_season: Last season to include

        Returns:
            Dictionary with seasons as keys and the number of athletes
            under a given ranking for a given nation, gender, and
            discipline for a given range of seasons as values.
        """
        results = {}
        for season in range(start_season, end_season + 1):
            logger.debug(f"Getting discipline series for {discipline} {nation} {gender} {season}")
            results[season] = self.report_by_discipline(discipline, nation, gender, season)
        return results
    
    def all_disciplines_series_for_seasons(self, nation: str, gender: Gender,
                                           start_season: int, end_season: int) -> Dict[int, Dict[str, int]]:
        """
        Get the number of athletes under a given ranking for a given
        nation, gender, and all disciplines for a given range of seasons.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            gender: Gender to filter by (required; Gender.A will combine)
            start_season: First season to include
            end_season: Last season to include

        Returns:
            Dictionary with discipline/gender strings as keys and the
            number of athletes under a given ranking for a given nation,
            gender, and all disciplines for a given range of seasons as
            values.
        """
        logger.debug(f"Entering all_disciplines_series_for_seasons for {nation} from {start_season} to {end_season}")
        results = {}
        results[f"{nation} licenses - {gender.value}"] = self.licenses_series_for_seasons(
            start_season, end_season, nation, gender
        )
        for discipline in Discipline:
            results[f"{discipline.name} - {gender.value}"] = self.discipline_series_for_seasons(
                discipline, nation, gender, start_season, end_season
            )
        logger.debug(f"Exiting all_disciplines_series_for_seasons for {nation} from {start_season} to {end_season}")
        return results

    def national_series_for_seasons(self, nation: str, start_season: int, end_season: int) -> Dict[int, Dict[str, int]]:
        """
        Get the number of athletes under a given ranking for a given nation for a given range of seasons.
        
        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            start_season: First season to include
            end_season: Last season to include

        Returns:
            Dictionary with gender-discipline strings as keys and the
            number of athletes under a given ranking for a given nation
            for a given range of seasons as values.

            Note that the highest rank category for each discipline is
            the total number of ranked athletes in that discipline, and
            the number of athletes in that column is the number of
            athletes from the given nation who are ranked in that
            discipline.

            E.g.
            {
                "total_licenses - male": {
                    2024: 100,
                    2025: 105
                    },
                "SL - male": {
                    2024: {
                        50: 10,
                        100: 20,
                        300: 30,
                        500: 40,
                        1000: 50,
                        2000: 60,
                        2523: 354
                    }
                    2025: { ... }
                },
                "GS - male": {
                    2024: { ... },
                    2025: { ... }
                },
                ...
                "total_licenses - female": { ... },
                "SL - female": {
                    2024: { ... },
                    2025: { ... }
                }, ...
            }
        """
        results = {}

        logger.debug(f"Getting national series for {nation} from {start_season} to {end_season}")
        for gender in [Gender.M, Gender.F]:
            results.update(self.all_disciplines_series_for_seasons(nation, gender, start_season, end_season))

        return results
    
    def series_by_year(self, nation: str, start_season: int, end_season: int) -> Dict[int, int]:
        """
        Get the number of athletes under a given ranking for a set of
        seasons, returned as DataFrame-ready dictionary with each series
        as a column.
        
        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            start_season: First season to include
            end_season: Last season to include

        Returns:
            Dictionary with seasons as keys and the number of athletes
            under a given ranking for a given nation, gender, and
            discipline for a given range of seasons as values.

            e.g.

            { 2024:
                {'total-men': 805, 'sl-50-men': 3, 'sl-100-men': 7, 'sl-300-men': 20, 'sl-500-men': 40, 'sl-1000-men': 131, 'sl-ranked-men': 761,
                               'gs-50-men': 5, 'gs-100-men': 10, 'gs-300-men': 30, 'gs-500-men': 40, 'gs-1000-men': 132, 'gs-ranked-men': 711,
                               'sg-50': 3 ...
                 'total-women': 805, 'sl-50-women': 3, 'sl-100-women': 7, 'sl-300-women': 20, 'sl-500-women': 40, 'sl-1000-women': 131, 'sl-ranked-women': 761,
                               'gs-50': 3 ...
                 'total-all': 1610, 'sl-50-all': 6, 'sl-100-all': 14, 'sl-300-all': 40, 'sl-500-all': 80, 'sl-1000-all': 262, 'sl-ranked-all': 1522,
                               'gs-50': 8 ...
                }
                2025: { ... }
                ...
            }

        """
        results = {}
        for season in range(start_season, end_season + 1):
            results[season] = self.season_series(nation, season)
        return results
    
    def season_series(self, nation: str, season: int) -> Dict[str, int]:
        """Get the number of athletes under a given ranking for a given nation for a given season.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by

        Returns:
            Dictionary with gender-discipline strings as keys and the
            number of athletes under a given ranking for a given nation
            for a given season as values.

            e.g.
            {'total-men': 805, 'sl-50-men': 3 ... 'gs-50-men': 5 ... }
            
        """
        results = {}
        # NB: we only need to do this for M and F, as the total is the sum of the two
        for gender in [Gender.M, Gender.F]:
            results.update(self.season_series_for_gender(nation, season, gender))
        return results
    
    def season_series_for_gender(self, nation: str, season: int, gender: Gender) -> Dict[str, int]:
        """
        Get the number of athletes under a given ranking for a given
        nation for a given season and gender.

        Args:
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by

        Returns:
            Dictionary with gender-discipline strings as keys and the
            number of athletes under a given ranking for a given nation
            for a given season and gender as values.

            e.g.
            {'total-men': 805, 'sl-50-men': 3 ... 'gs-50-men': 5 ... }
        """
        logger.info(f"Getting season series for {nation} {season} {gender}")
        results = {}
        for discipline in Discipline:
            results.update(self.season_series_for_discipline(discipline, nation, season, gender))
        return results

    def season_series_for_discipline(self, discipline: Discipline, nation: str, season: int, gender: Gender) -> Dict[str, int]:
        """
        Get the number of athletes under a given ranking for a given nation for a given season and discipline.

        Args:
            discipline: Discipline to filter by
            nation: three-letter national code (e.g. "USA", "SUI", "AUT")
            season: Season to filter by
            gender: Gender to filter by

        Returns:
            Dictionary with gender-discipline strings as keys and the
            number of athletes under a given ranking for a given nation
            for a given season and discipline as values.

            e.g.
            {'total-men': 805, 'sl-50-men': 3 ... 'sl-ranked-men': 761 }
        """
        logger.debug(f"Getting season series for {discipline} {nation} {season} {gender}")
        results = {}
        results[f"total-{gender.value}"] = self.licenses_per_year(season, nation, gender)
        if discipline == Discipline.ALL:
            max_rank = self._total_licenses_for_season(season, gender)
        else:
            max_rank = self._max_rank_for_discipline(discipline, gender, self._get_final_points_list_for_season(season))
            if max_rank is None:
                return results
                # max_rank is None if no athletes are ranked in this discipline for this season,
                # which may happen if the discipline was not contested in that season
                # (e.g. AC prior to 2017)
        for ranking in self.RANKING_DEFAULTS:
            if ranking < max_rank:
                results[f"{discipline.name}-{ranking}-{gender.value}"] = self.session.execute(
                    select(func.count("*")).select_from(
                        self._athletes_under_ranking_query(ranking, season, nation, gender, discipline).subquery()
                    )
                ).scalar()
        results[f"{discipline.name}-ranked-{gender.value}"] = self.session.execute(
            select(func.count("*")).select_from(
                self._athletes_under_ranking_query(max_rank, season, nation, gender, discipline).subquery()
            )
        ).scalar()
        return results
    
    
    def licenses_per_year(self, season: int, nation: str = None, gender: Gender = Gender.A) -> Dict[int, int]:
        """Get the number of licenses per year for a given nation and gender."""
        points_list = self._get_final_points_list_for_season(season)
        if points_list is None:
            raise ValueError(f"No points list found for season {season}")

        query = select(func.count("*")) \
            .select_from(Athlete).join(AthletePoints) \
            .where(AthletePoints.points_list == points_list)
        
        if nation:
            query = query.where(Athlete.nation_code == nation)
        if gender != Gender.A:
            query = query.where(Athlete.gender == gender)

        return self.session.execute(query).scalar()
    
    def _total_licenses_for_season(self, season: int, gender: Gender) -> int:
        """Get the total number of licenses for a given season and gender."""
        return self.session.execute(
            select(func.count("*")).select_from(Athlete).join(AthletePoints)
                .where(AthletePoints.points_list == self._get_final_points_list_for_season(season),
                       Athlete.gender == gender)
        ).scalar()
    
    def licenses_series_for_seasons(self, start_season: int, end_season: int, nation: str = None,
                                    gender: Gender = Gender.A) -> Dict[int, int]:
        """Get the number of licenses per year for a given nation and gender for a given range of seasons."""
        results = {}
        for season in range(start_season, end_season + 1):
            results[season] = self.licenses_per_year(season, nation, gender)
        return results

    def _max_rank_for_discipline(self, discipline: Discipline, gender: Gender,
                                 points_list: PointsList) -> int:
        """Get the maximum rank for a given discipline and points list."""
        return self.session.execute(
            select(func.max(self._get_discipline_rank_column(discipline)))
                .select_from(AthletePoints)
                .join(Athlete)
                .where(
                    AthletePoints.points_list == points_list,
                    Athlete.gender == gender
                )
            ).scalar()


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