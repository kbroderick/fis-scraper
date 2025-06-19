import src.fis_scraper
import src.fis_scraper.scrapers
import argparse

parser = src.fis_scraper.scrapers.points_list._get_argument_parser()
args = parser.parse_args()

src.fis_scraper.scrapers.points_list.main(start_date=args.start_date,
                                      end_date=args.end_date,
                                      only_list=args.only_list)
