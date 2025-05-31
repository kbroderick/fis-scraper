from datetime import datetime, timedelta, date
from ..database.connection import get_session
from ..database.models import RaceResult, AthletePoints, PointsList
import pandas as pd
import numpy as np
from typing import Dict, Optional, Any
from sqlalchemy.orm import Session

class PerformanceAnalyzer:
    """Analyzer for athlete performance data including race results and FIS points.
    
    This class provides methods to analyze athlete performance across different
    disciplines, including points trends, rankings, and race results.
    """
    
    def __init__(self) -> None:
        """Initialize the PerformanceAnalyzer with a database session."""
        self.session: Session = get_session()
    
    def get_athlete_performance(self, athlete_id: int, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, Any]:
        """Get comprehensive performance analysis for an athlete.
        
        Args:
            athlete_id: ID of the athlete to analyze
            start_date: Start date for analysis period (defaults to 1 year ago)
            end_date: End date for analysis period (defaults to today)
            
        Returns:
            Dict containing:
                - race_results: Analysis of individual race results
                - points_trends: Analysis of FIS points trends
                - discipline_analysis: Analysis by discipline
        """
        if not start_date:
            start_date = datetime.now().date() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now().date()
        
        # Get race results
        results = self.session.query(RaceResult).filter(
            RaceResult.athlete_id == athlete_id,
            RaceResult.race_date >= start_date,
            RaceResult.race_date <= end_date
        ).all()
        
        # Get points history
        points = self.session.query(AthletePoints).join(
            AthletePoints.points_list
        ).filter(
            AthletePoints.athlete_id == athlete_id,
            PointsList.valid_from >= start_date,
            PointsList.valid_to <= end_date
        ).all()
        
        # Convert to DataFrames for analysis
        results_df = pd.DataFrame([{
            'date': r.race_date,
            'discipline': r.discipline.name,
            'points': r.points,
            'rank': r.rank,
            'race_name': r.race_name,
            'location': r.location
        } for r in results])
        
        points_df = pd.DataFrame([{
            'date': p.points_list.valid_from,
            'sl_points': p.sl_points,
            'gs_points': p.gs_points,
            'sg_points': p.sg_points,
            'dh_points': p.dh_points,
            'sl_rank': p.sl_rank,
            'gs_rank': p.gs_rank,
            'sg_rank': p.sg_rank,
            'dh_rank': p.dh_rank
        } for p in points])
        
        return {
            'race_results': self._analyze_race_results(results_df),
            'points_trends': self._analyze_points_trends(points_df),
            'discipline_analysis': self._analyze_disciplines(results_df, points_df)
        }
    
    def _analyze_race_results(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze race results data.
        
        Args:
            results_df: DataFrame containing race results
            
        Returns:
            Dict containing analysis metrics:
                - total_races: Total number of races
                - average_rank: Average finish position
                - best_rank: Best finish position
                - worst_rank: Worst finish position
                - average_points: Average FIS points
                - best_points: Best FIS points
                - worst_points: Worst FIS points
                - rank_trend: Trend in finish positions
                - points_trend: Trend in FIS points
        """
        if results_df.empty:
            return {}
        
        analysis = {
            'total_races': len(results_df),
            'average_rank': results_df['rank'].mean(),
            'best_rank': results_df['rank'].min(),
            'worst_rank': results_df['rank'].max(),
            'average_points': results_df['points'].mean(),
            'best_points': results_df['points'].min(),
            'worst_points': results_df['points'].max()
        }
        
        # Add trend analysis
        results_df['date'] = pd.to_datetime(results_df['date'])
        results_df = results_df.sort_values('date')
        analysis['rank_trend'] = self._calculate_trend(results_df['rank'])
        analysis['points_trend'] = self._calculate_trend(results_df['points'])
        
        return analysis
    
    def _analyze_points_trends(self, points_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Analyze FIS points trends.
        
        Args:
            points_df: DataFrame containing points and rankings data
            
        Returns:
            Dict containing analysis for each discipline:
                - current_points: Most recent points
                - best_points: Best points achieved
                - worst_points: Worst points achieved
                - points_trend: Trend in points
                - current_rank: Most recent rank
                - best_rank: Best rank achieved
                - worst_rank: Worst rank achieved
                - rank_trend: Trend in rankings
        """
        if points_df.empty:
            return {}
        
        analysis = {}
        disciplines = {
            'sl': ('sl_points', 'sl_rank'),
            'gs': ('gs_points', 'gs_rank'),
            'sg': ('sg_points', 'sg_rank'),
            'dh': ('dh_points', 'dh_rank')
        }
        
        for disc, (points_col, rank_col) in disciplines.items():
            if points_col in points_df.columns and rank_col in points_df.columns:
                analysis[disc] = {
                    'current_points': points_df[points_col].iloc[-1],
                    'best_points': points_df[points_col].min(),
                    'worst_points': points_df[points_col].max(),
                    'points_trend': self._calculate_trend(points_df[points_col]),
                    'current_rank': points_df[rank_col].iloc[-1],
                    'best_rank': points_df[rank_col].min(),
                    'worst_rank': points_df[rank_col].max(),
                    'rank_trend': self._calculate_trend(points_df[rank_col])
                }
        
        return analysis
    
    def _analyze_disciplines(self, results_df: pd.DataFrame, points_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Analyze performance by discipline.
        
        Args:
            results_df: DataFrame containing race results
            points_df: DataFrame containing points and rankings data
            
        Returns:
            Dict containing analysis for each discipline:
                - total_races: Number of races in discipline
                - average_rank: Average finish position
                - best_rank: Best finish position
                - worst_rank: Worst finish position
                - average_points: Average FIS points
                - best_points: Best FIS points
                - worst_points: Worst FIS points
                - rank_trend: Trend in finish positions
                - points_trend: Trend in FIS points
        """
        if results_df.empty:
            return {}
        
        analysis = {}
        for discipline in results_df['discipline'].unique():
            disc_results = results_df[results_df['discipline'] == discipline]
            analysis[discipline] = {
                'total_races': len(disc_results),
                'average_rank': disc_results['rank'].mean(),
                'best_rank': disc_results['rank'].min(),
                'worst_rank': disc_results['rank'].max(),
                'average_points': disc_results['points'].mean(),
                'best_points': disc_results['points'].min(),
                'worst_points': disc_results['points'].max(),
                'rank_trend': self._calculate_trend(disc_results['rank']),
                'points_trend': self._calculate_trend(disc_results['points'])
            }
        
        return analysis
    
    def _calculate_trend(self, series: pd.Series) -> float:
        """Calculate trend using linear regression.
        
        Args:
            series: Series of values to analyze
            
        Returns:
            float: Slope of the trend line (positive = improving, negative = declining)
        """
        if len(series) < 2:
            return 0
        
        x = np.arange(len(series))
        y = series.values if hasattr(series, 'values') else np.array(series)
        slope = np.polyfit(x, y, 1)[0]
        return slope 