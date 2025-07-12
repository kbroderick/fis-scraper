import src.fis_scraper
import src.fis_scraper.scrapers
import argparse
import sys

def main() -> None:
    """Main entry point for the FIS scraper application."""
    # Create main parser
    parser = argparse.ArgumentParser(description='FIS Skiing Data Scraper and Analyzer')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Points list scraper
    points_parser = subparsers.add_parser('points', help='Scrape FIS points lists')
    src.fis_scraper.scrapers.points_list_scraper.PointsListScraper.add_arguments_to_parser(points_parser)
    
    # Race results scraper
    races_parser = subparsers.add_parser('races', help='Scrape FIS race results')
    src.fis_scraper.scrapers.race_results_scraper.RaceResultsScraper.add_arguments_to_parser(races_parser)
    
    args = parser.parse_args()
    
    if args.command == 'points':
        # Run points list scraper
        src.fis_scraper.scrapers.points_list_scraper.main(
            start_date=args.start_date,
            end_date=args.end_date,
            only_list=args.only_list
        )
    elif args.command == 'races':
        # Run race results scraper
        src.fis_scraper.scrapers.race_results_scraper.main(
            race_category=args.race_category,
            race_id=args.race_id,
            discover_only=args.discover_only,
            season=args.season,
            verbose=args.verbose,
            very_verbose=args.very_verbose
        )
    else:
        # Default to points list scraper for backward compatibility
        src.fis_scraper.scrapers.points_list_scraper.main(
            start_date=args.start_date,
            end_date=args.end_date,
            only_list=args.only_list
        )

if __name__ == "__main__":
    main()
