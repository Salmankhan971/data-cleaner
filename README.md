# Data Cleaner SaaS

Auto-fix messy CSV/Excel files.

## Features
- Upload CSV/Excel files
- Auto-detect issues (empty cells, duplicates, wrong formats)
- One-click cleaning
- Download cleaned files

## Tech Stack
- Backend: Flask (Python)
- Frontend: HTML/JavaScript
- Database: SQLite

## Local Setup

```bash
# 1. Clone repo
git clone https://github.com/yourusername/data-cleaner.git
cd data-cleaner

# 2. Create virtual environment
python -m venv venv

# 3. Activate (Windows)
venv\Scripts\activate
# Or (Mac/Linux)
source venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Run
python app.py
