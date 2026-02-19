# TV Charts Data Analysis

This repository contains Python scripts and tools for extracting, decompressing, processing, and analyzing data from TradingView (TV) charts. The primary focus is on handling binary-encoded chart data for drawings, study templates, indicators, and converting them into readable JSON formats, CSV exports, and SQLite databases for further analysis.

## Features
- Decompress TradingView drawings and study templates from binary format.
- Extract structured data like drawing tools, indicators, and template values.
- Batch import processed data into SQLite database.
- Generate CSV reports for tools and indicators.
- SQL queries for database analysis.
- HTML reports/visualizations (e.g., `ra.html`).

## Project Structure
```
tv_charts_data_analysis/
├── README.md                  # This file
├── .gitignore
├── batch_progress.json        # Processing progress tracking
├── progress_staging.json      # Staging progress
├── drawing_tools.csv          # Extracted drawing tools
├── indicators.csv             # Extracted indicators
├── queries.sql                # Sample SQL queries
├── index.html                 # Index page
├── ra.html                    # Analysis report
├── ra_static_page.zip         # Static page archive
├── combined_json_extractor.py # Main extractor for combined JSON
├── binary_json_conversion.py  # Binary to JSON conversion
├── binary_json_convert_to_db.py # JSON to DB conversion
├── extract_template_values.py # Template value extraction
├── sqlite_batch_import_updated.py # Batch DB import
├── tv_tv_drawings_decompressed/ # Decompressed drawings JSONs (e.g., 311.json)
└── tv_study_templates_decompressed/ # Decompressed templates (e.g., test.json)
```

## Prerequisites
- Python 3.8+
- SQLite3 (usually included with Python)
- No additional pip packages required (uses standard library where possible)

## Quick Start
1. Clone the repository:
   ```
   git clone https://github.com/Dineshvmr/tv_charts_data_analysis.git
   cd tv_charts_data_analysis
   ```
2. Run extraction scripts sequentially:
   ```
   python binary_json_conversion.py  # Convert binary to JSON if needed
   python combined_json_extractor.py # Extract combined data
   python extract_template_values.py # Extract template values
   python sqlite_batch_import_updated.py # Import to DB
   ```
3. Query the database:
   ```
   sqlite3 your_database.db < queries.sql
   ```
4. View reports:
   ```
   open ra.html  # or index.html
   ```

## Workflow
1. **Input**: Binary TradingView chart data (drawings/studies).
2. **Process**: Decompress → Extract JSON → Parse values → CSV/DB export.
3. **Output**: JSON files, CSVs, SQLite DB, HTML reports.
4. Progress tracked in `batch_progress.json` and `progress_staging.json`.

## Notes
- Decompressed data samples are included in `tv_tv_drawings_decompressed/` and `tv_study_templates_decompressed/`.
- Customize scripts for your specific TV data files.
- For large datasets, monitor progress files and run in batches.

## License
MIT License (or specify as needed).

For issues or contributions, open a GitHub issue.