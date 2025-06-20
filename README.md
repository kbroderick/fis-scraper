# FIS Skiing Data Scraper and Analyzer

This Python application scrapes and analyzes FIS (International Ski Federation) skiing data, including:
- Historical FIS points lists since 2002
- Individual athlete results and performance
- Performance tracking in technical (SL, GS) and speed (SG, DH) disciplines

## Features

- Automated scraping of FIS points lists
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
   python main.py
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
│           └── analysis/     # Data analysis tools
└── tests/               # Test suite
|-- data/
|     |----- points_lists # saved points lists
```

## Testing

Run the test suite:
```bash
pytest tests/
```

## License

MIT License 

## Creation

Built with cursor by Kevin Broderick / ktb@kevinbroderick.com