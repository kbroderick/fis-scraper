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
    races_parser.add_argument('--race-category', type=str, help='Race category (one of "OWG", "WSC", "WC", "WQUA", "YOG", "ANC", "EC", "FEC", "NAC", "SAC", "ECOM", "EQUA", "NC", "FIS", "FQUA", "AWG", "UVS", "JUN", "NJC", "NJR", "CIT", "UNI", "CHI", "PARA", "DAR", "CORP", "CISM", "ENL", "EXI", "EYOF", "NGP", "TRA")')
    races_parser.add_argument('--race-id', type=int, help='Scrape specific race by ID')
    races_parser.add_argument('--season', type=int, help='Scrape specific season, e.g. ')
    races_parser.add_argument('--discover-only', action='store_true', 
                             help='Only discover races, don\'t scrape results')
    
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
            discover_only=args.discover_only
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
