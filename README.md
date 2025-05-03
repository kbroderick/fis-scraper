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
- SQLite database storage

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

## Usage

1. Configure database settings in `.env` file
2. Run the scraper:
   ```bash
   python -m fis_scraper.scrapers.points_list
   ```
3. Analyze athlete performance:
   ```bash
   python -m fis_scraper.analysis.performance
   ```

## Project Structure

```
py-scrape-ai/
├── src/
│   └── fis_scraper/
│       ├── database/     # Database models and connection
│       ├── scrapers/     # Web scraping components
│       └── analysis/     # Data analysis tools
└── tests/               # Test suite
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