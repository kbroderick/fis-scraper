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

   Testing has been somewhat irregular; I suspect that there might be some sort of anti-bot protection on the FIS site, as loading the FIS Points List page in a browser prior to execution correlated with scraping success in some cases (causation is not determined.) List of FIS Points Lists is at:
   https://www.fis-ski.com/DB/alpine-skiing/fis-points-lists.html

   ```bash
   python main.py points
   ```

   **Race Results Scraping**
   
   Discover and scrape race results:
   ```bash
   # Discover races in current season
   python -m src.fis_scraper.scrapers.race_results_scraper  --discover-only
      
   # Scrape specific race by ID
   python -m src.fis_scraper.scrapers.race_results_scraper --race-id 12345
   ```

   **Important:** To run the race results scraper, you must use the `-m` flag to run it as a module. This is required because the code uses relative imports, which do not work if you run the script directly.

   **Example:**
   ```sh
   python -m src.fis_scraper.scrapers.race_results_scraper --race-category FIS --season 2025 --discover-only
   ```

   **Command Line Arguments**
   - `--race-category`  (e.g., FIS, WC, EC, UNI, NC, CIT, CUP, etc)
   - `--season`         (e.g., 2025 for 2024/2025 season ending 30 Jun 25)
   - `--race-id`        (scrape a specific race by ID)
   - `--discover-only`  (only discover races, do not scrape results)
   - `--verbose`        (enable verbose logging)

   See help for all options:
   ```sh
   python -m src.fis_scraper.scrapers.race_results_scraper --help
   ```

   TODO:
      - update Races to record gender of event
      - allow for CSV input of roster for eval
      - create per-athlete analysis (points, rank, and results over time)
      - create per-roster analysis (points, rank and result over time; particular   attention to delta in rank between selection and graduation)
      - allow for web scraping to generate roster
      - web interface
      - explore hosting options
      - consider supporting Team Parallel (see below)

   Performance concerns:

      As noted, ingestion defaults to processing all available lists since 2002.
      
      In the dev environment, each list takes approximately 30-40 seconds; ingesting all 332 lists takes a while.

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
- `sl_status`, `gs_status`, `sg_status`, `dh_status`, `ac_status`: points status per discipline; c.f. "Rules for the FIS Alpine Points'—
   - '#'-> injury status protection
   - '*' Base list (i.e. skier has not beat BL points twice in current season)
   - '+' only one result and no base list points; in base list -> only one
      result from past season (penalized 20% of single result)
   - '>' no results in event during last season; penalized 50% of BL
   - '-' injured but real points
   - 'C' points confirmation


## Testing

Run the test suite:
```bash
dropdb fis_test_data ; createdb fis_data_test ; PYTEST_CURRENT_TEST=true alembic upgrade head && pytest -v tests/
```

## License

MIT License 

## Creation

Built with cursor by Kevin Broderick / ktb@kevinbroderick.com

## Useful resources

FIS "RULES FOR THE FIS ALPINE POINTS" (probably somewhere on FIS site as well): https://www.skidor.com/download/18.241e27d2184fa16745750a75/1670914704863/FIS_Points_Rules_Alpine_June_22_E.pdf

## Team Parallel

Team Parallel events are currently not supported; the small number of such events makes the analytical value small, and they would take special-case handling to parse.

For example, in season 2025:
JWC Tarvisio:
https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=123701
(lists team with participants, time and win/loss color for final round)

University event at Torino:
https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=126716
(lists teams only)

Chilean even Trofeu Borrufa 2025
https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=123678
(no results listed, PDF download has results)

Saalbach World Champs
https://www.fis-ski.com/DB/general/results.html?sectorcode=AL&raceid=122881
Same as JWC, full results list with participants and times

Bakuriani EYOF
https://www.fis-ski.com/DB/general/event-details.html?sectorcode=AL&eventid=55954&seasoncode=2025
Sames as Chilean event, no actual results

