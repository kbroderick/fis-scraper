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
4. Prep DB
   # create DB
   createdb fis_data
   # Then run the migrations
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
      add ability to ingest race results via scraping
      allow for CSV input of roster for eval
      create per-athlete analysis (points, rank, and results over time)
      create per-roster analysis (points, rank and result over time; particular attention to
         delta in rank between selection and graduation)
      allow for web scraping to generate roster
      web interface?
      containerized?

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