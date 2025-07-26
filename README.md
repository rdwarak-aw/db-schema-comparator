# DB Schema Comparator

A Python-based utility that compares schema structures between two relational databases (SQL Server, MySQL, PostgreSQL) and generates HTML and/or PDF reports showing missing, extra, or mismatched schema objects.

---

## Features

* Supports: SQL Server, MySQL, PostgreSQL *(Oracle planned)*
* Compares:

  * Tables (columns, types, lengths)
  * Views
  * Stored Procedures & Functions
  * Triggers, Constraints, Indexes
* Outputs:

  * HTML Report
  * PDF Report (WeasyPrint)
* Logs actions using configurable logging
* Modular, extensible, and config-driven

---

## Installation

### Prerequisites

* Python 3.9+
* ODBC Driver (for SQL Server)
* libffi, Cairo, and Pango for PDF output (Linux/macOS users)

### Setup

```bash
# Clone the repository
$ git clone https://github.com/your-org/db-schema-comparator.git
$ cd db-schema-comparator

# Optional: Create virtual environment
$ python -m venv venv
$ source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install required dependencies
$ pip install -r requirements.txt
```

---

## Configuration

Edit the `config.json` file. Structure example:

```json
{
  "active_db": "sqlserver",

  "sqlserver": {
    "source": {
      "server": "10.100.10.1",
      "database": "ABC",
      "username": "abc",
      "password": "abc",
      "auth_type": "sql",
      "timeout": 30,
      "schemas": ["dbo"]
    },
    "destination": {
      "server": "10.100.10.2",
      "database": "XYZ",
      "username": "xyz",
      "password": "xyz",
      "auth_type": "sql",
      "timeout": 30,
      "schemas": ["dbo"]
    }
  },

  "compare_objects": {
    "tables": true,
    "views": true,
    "constraints": true,
    "indexes": true,
    "stored_procedures": true,
    "functions": true,
    "triggers": true
  },

  "output": {
    "formats": ["html", "pdf"],
    "html_report": "./reports/schema_diff_report.html",
    "pdf_report": "./reports/schema_diff_report.pdf"
  }
}
```

---

## Usage

```bash
$ python main.py
```

Reports will be saved under the `./reports/` folder as per your config.

---

## Directory Structure

```
db-schema-comparator/
├── db_adapters/
│   ├── base_db_adapter.py
│   ├── sqlserver_adapter.py
│   ├── mysql_adapter.py
│   └── postgresql_adapter.py
├── utils/
│   └── hashlib.py
├── templates/
│   └── report_template.html
├── reports/
├── main.py
├── comparator.py
├── config_loader.py
├── config.json
├── db_factory.py
├── DockerFile
├── LICENSE.txt
├── logger.properties
├── logger.py
├── README.md
├── report_generator.py
└── requirements.txt

```

---

## Extending

To add a new database type (e.g., Oracle):

1. Create a new adapter class that inherits `BaseDBAdapter`
2. Implement `connect`, `extract_metadata`, and `close`
3. Register the adapter in `db_factory.py`
4. Add corresponding config section in `config.json`

---

## Docker 

## Docker Compose

version: '3.9'

services:
  db-schema-comparator:
    build: .
    volumes:
      - ./reports:/app/reports
      - ./config.json:/app/config.json
    environment:
      - PYTHONUNBUFFERED=1


# Build the image
docker build -t db-schema-comparator .

# Run it
docker run --rm -v $(pwd)/reports:/app/reports db-schema-comparator


## License
License: MIT © 2025 Dwarakanath R r.dwarak@gmail.com