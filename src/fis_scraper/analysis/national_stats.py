from typing import Any, Dict, List
from enum import Enum
from sqlalchemy import or_, and_,select, Column, Select, func
from sqlalchemy.orm import Session
from ..database.connection import get_session
from ..database.models import Athlete, AthletePoints, Gender, Discipline, PointsList
from ..scrapers.race_results_scraper import RaceResultsScraper

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
            results[discipline] = self.report_by_discipline(discipline, nation, gender, season)
        return results

    def report_by_discipline(self, discipline: Discipline,
                             nation: str, gender: Gender, season: int = None) -> Dict[int, int]:
        """Generate a report by discipline for a given nation, gender, and season."""
        if season is None:
            season = RaceResultsScraper.get_current_season()

        results = {}

        for ranking in self.RANKING_DEFAULTS:
            athletes = self._athletes_under_ranking_query(ranking, season, nation, gender, discipline)
            results[ranking] = self.session.execute(
                select(func.count("*")).select_from(athletes.subquery())
            ).scalar()
        
        if discipline == Discipline.ALL:
            return results
        
        # maximium rank for all FIS athletes with points and the number of athletes ranked for this nation
        max_rank = self._max_rank_for_discipline(discipline, gender, self._get_final_points_list_for_season(season))
        if max_rank:
            results[max_rank] = self.session.execute(
                    select(func.count("*")).select_from(
                        self._athletes_under_ranking_query(max_rank, season, nation, gender, discipline).subquery()
                    )
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
