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
    points_parser.add_argument('--start-date', type=str, help='Start date (DD.MM.YYYY)')
    points_parser.add_argument('--end-date', type=str, help='End date (DD.MM.YYYY)')
    points_parser.add_argument('--only-list', type=int, help='Scrape only specific list ID')
    
    # Race results scraper
    races_parser = subparsers.add_parser('races', help='Scrape FIS race results')
    races_parser.add_argument('--start-date', type=str, help='Start date (DD.MM.YYYY)')
    races_parser.add_argument('--end-date', type=str, help='End date (DD.MM.YYYY)')
    races_parser.add_argument('--discipline', type=str, choices=['SL', 'GS', 'SG', 'DH', 'AC'],
                             help='Filter by discipline')
    races_parser.add_argument('--race-id', type=int, help='Scrape specific race by ID')
    races_parser.add_argument('--discover-only', action='store_true', 
                             help='Only discover races, don\'t scrape results')
    
    args = parser.parse_args()
    
    if args.command == 'points':
        # Run points list scraper
        src.fis_scraper.scrapers.points_list.main(
            start_date=args.start_date,
            end_date=args.end_date,
            only_list=args.only_list
        )
    elif args.command == 'races':
        # Run race results scraper
        src.fis_scraper.scrapers.race_results.main(
            start_date=args.start_date,
            end_date=args.end_date,
            discipline=args.discipline,
            race_id=args.race_id,
            discover_only=args.discover_only
        )
    else:
        # Default to points list scraper for backward compatibility
        src.fis_scraper.scrapers.points_list.main(
            start_date=args.start_date,
            end_date=args.end_date,
            only_list=args.only_list
        )

if __name__ == "__main__":
    main()
