# FIS Skiing Data Scraper and Analyzer

This Python application scrapes and analyzes FIS (International Ski Federation) skiing data, including:
- Historical FIS points lists since 2002
- Individual athlete results and performance
- Race results with detailed timing and points data

## Features

- Automated scraping of FIS points lists
- Race results scraping with detailed timing data
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

   ```bash
   createdb fis_data
   alembic upgrade head
   ```

## Usage

1. Configure database settings in `.env` file (using default name of `fis_data` strongly recommended)
2. Run the scraper:

   **CAUTION**
   This will attempt to ingest all available FIS points lists since 2002. As of this writing, there
   are 333 available. Expect this to take a while.

   ```bash
   python main.py points
   ```

   For more options:
   ```bash
   python main.py points -h
   ```

   **Race Results Scraping**
   
   Discover and scrape race results:
   ```bash
   # Discover races in current season
   python main.py races  --discover-only
      
   # Scrape specific race by ID
   python main.py races --race-id 12345
   ```
   
   **Example:**
   ```bash
   python main.py races --race-category WC --season 2025 --very-verbose
   ```
   Import the 2025 World Cup events with very verbose logging.

   **Notes:**
   Parallel and team events are not supported and will show errors.

   **Command Line Arguments**
   - `--race-category`  (e.g., FIS, WC, EC, UNI, NC, CIT, CUP, etc)
   - `--season`         (e.g., 2025 for 2024/2025 season ending 30 Jun 25)
   - `--race-id`        (scrape a specific race by ID)
   - `--discover-only`  (only discover races, do not scrape results)
   - `--verbose`        (enable verbose logging)
   - `--very-verbose`   (enable *really* verbose logging)

   See help for all options:
   ```bash
   python main.py races --help
   ```

   Note that some race events have races from multiple categories—e.g. combining SAC and Chilean NC events in 55641. Those races will show up in both SAC and NC categories during discovery, but each will be ingested only once and recorded with the appropriate category in the local DB.

## TODO
   - allow for CSV input of roster for eval
   - create per-athlete analysis (points, rank, and results over time)
   - create per-roster analysis (points, rank and result over time; particular   attention to delta in rank between selection and graduation)
   - allow for web scraping to generate roster
   - web interface
   - update race scraping to identify future and cancelled races and
      report as such (2016 import has suspiciously high number of "no results" returns, but hand sampling shows all to be canceled per FIS site)
   - consider supporting Team Parallel, Team combined (see below)

## Functionality notes
 ### File paths, DB, and such
   File paths are hardcoded relative to program root. I may get around to making this configurable in the future; if this is important to you, please let me know.

   `data/points_list` -- cached points list downloads
      
   App expects DB to be `fis_data` and test DB to be `fis_data_test`. You can configure another DB name if you prefer, but munging for test environment will break (c.f. `connection.py`) and pytest will want to use your production DB.

 ### Performance concerns:

   As noted, ingestion defaults to processing all available lists since 2002.
      
   In the dev environment, each list takes approximately 30-40 seconds; ingesting all 333 lists takes a while.

   Scraping an entire season of race results, likewise, will take significant time. You will be notified of races without results (e.g. those that were cancelled) and any unsupported discipline types (e.g. Parallel, Team Combined, and City Events).

 ### Cached points lists:

   For efficiency, the system retains a copy of the CSV points list in the data/ folder after downloading it. If the data is removed from the database, re-ingestion will happen from the saved file. Should a saved file be corrupted, simply delete it and it will be downloaded again (as long as the directory of points lists on the FIS site still includes it).


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
|     |----- html         # saved HTML for parsing tests
|             |-- races   # saved race results pages for parsing tests
```

## Database Schema

Each PointsList has many AthletePoints
Each contested Race has many RaceResults
Each Athlete has many AthletePoints and many RaceResults

See `src.fis_scraper.database.models.py` for details. Most fields should be reasonably self-explanatory.

Of note as not self-explanatory, `sl_status`, `gs_status`, `sg_status`, `dh_status`, `ac_status` are points status per discipline; c.f. "Rules for the FIS Alpine Points'—
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

## Team events

Not currently supported. Ingestion will be skipped because the discipline won't parse into one that we're expecting, so it doesn't actively break anything, but we won't be able to consider those events for further analysis.

### Team combined
This one may be worth adding support for, but determining how to store and analyze data will depend on whether or not FIS presentation of the same is consistent.

### Team parallel
The small number of such events makes the analytical value small, and they would take special-case handling to parse.

For example, in season 2025, only two events (JWC Tarvisio /raceid 123701 and Saalbach World Championships, race 122881) have full results; one event has partial results listed; and two show "no results" but a PDF is available.

## Also not supported
City Events
