# FIS Skiing Data Scraper and Analyzer

This Python application scrapes and analyzes FIS (International Ski Federation) skiing data, including:
- Historical FIS points lists since 2002
- Individual athlete results and performance
- Race results with detailed timing and points data
- Performance tracking in technical (SL, GS) and speed (SG, DH) disciplines

## Features

- Automated scraping of FIS points lists
- Race results scraping with detailed timing data
- Individual athlete result tracking
- Performance analysis over time
- World ranking and FIS points trend analysis
- Postgres database storage

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Prep DB — create & run migrations

   createdb fis_data
   alembic upgrade head

## Usage

1. Configure database settings in `.env` file
2. Run the scraper:

   **CAUTION**
   This will attempt to ingest all available FIS points lists since 2002. As of this writing, there
   are 331 available. Expect this to take a while.
   ```bash
   python main.py points
   ```

   **Race Results Scraping**
   
   Discover and scrape race results:
   ```bash
   # Discover races in a date range
   python main.py races --start-date 01.01.2024 --end-date 31.01.2024 --discover-only
   
   # Scrape all races in a date range
   python main.py races --start-date 01.01.2024 --end-date 31.01.2024
   
   # Scrape specific race by ID
   python main.py races --race-id 12345
   
   # Filter by discipline
   python main.py races --start-date 01.01.2024 --end-date 31.01.2024 --discipline SL
   ```

   TODO:
      - split dev / testing DB environments
      - add ability to ingest race results via scraping
      - allow for CSV input of roster for eval
      - create per-athlete analysis (points, rank, and results over time)
      - create per-roster analysis (points, rank and result over time; particular   attention to delta in rank between selection and graduation)
      - allow for web scraping to generate roster
      - web interface?
      - containerized?

   Performance concerns:

      As noted, ingestion defaults to processing all available lists since 2002.
      
      In the dev environment, each list takes approximately 30-40 seconds; ingesting all 331 lists takes a while. The current ingestion is hugely inefficienct because it ensures an Athlete to exist before creating each points record; batching all athlete creation and then all points record import, or perhaps using one of the identifiers already in the FIS DB rather than our own row id (this would have implications for being able to expand app for NGB-level data, however.)

   Cached points lists:

      For efficiency, the system retains a copy of the CSV points list in the data/ folder after downloading it. If the data is removed from the database, re-ingestion will happen from the saved file. Should a saved file be corrupted, simply delete it and it will be downloaded again (as long as the directory of points lists on the FIS site still includes it).

3. Analyze athlete performance: (this doesn't work yet)
   ```bash
   python -m fis_scraper.analysis.performance
   ```

## Project Structure

```
fis-scraper/
├── src/
|     |--fis_scraper/
│           ├── database/     # Database models and connection
│           ├── scrapers/     # Web scraping components
│           │     ├── points_list.py    # FIS points list scraper
│           │     ├── race_results.py   # Race results scraper
│           │     └── athlete_results.py # Individual athlete results
│           └── analysis/     # Data analysis tools
└── tests/               # Test suite
|-- data/
|     |----- points_lists # saved points lists
```

## Database Schema

The application uses the following key database models:

### Athletes
- `id`: Primary key
- `fis_id`: FIS ID
- `fis_db_id`: FIS DB ID for API calls
- `last_name`, `first_name`: Athlete name
- `nation_code`: 3-letter country code
- `gender`: Athlete gender
- `birth_date`, `birth_year`: Birth information

### Race Results
- `id`: Primary key
- `athlete_id`: Foreign key to Athletes
- `fis_db_id`: FIS race ID (from raceid param in FIS URLs)
- `race_date`: Date of the race
- `discipline`: Race discipline (SL, GS, SG, DH, AC)
- `points`: Calculated FIS points
- `penalty`: Calculated race penalty (point value for winner)
- `race_points`: Race points for finish position
- `rank`: Finish position
- `win_time`, `racer_time`: Timing data
- `race_name`, `location`: Race information
- `race_category`: Race category (WC, EC, FIS, UNI, etc.)

The race results scraper now stores the FIS race ID (`fis_db_id`) for each result, matching the `raceid` parameter from FIS URLs.

### Points Lists
- `id`: Primary key
- `valid_from`, `valid_to`: Validity period
- `season`: Season identifier
- `listid`: FIS list ID
- `name`: Points list name

### Athlete Points
- `id`: Primary key
- `athlete_id`: Foreign key to Athletes
- `points_list_id`: Foreign key to Points Lists
- `sl_points`, `gs_points`, `sg_points`, `dh_points`, `ac_points`: Discipline points
- `sl_rank`, `gs_rank`, `sg_rank`, `dh_rank`, `ac_rank`: World rankings

## Testing

Run the test suite:
```bash
createdb fis_data_test ; PYTEST_CURRENT_TEST=true alembic upgrade head ; pytest tests/
```

## License

MIT License 

## Creation

Built with cursor by Kevin Broderick / ktb@kevinbroderick.com