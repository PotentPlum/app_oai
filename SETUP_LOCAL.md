# EcoPulse Dashboard - Local Setup (Windows)

## Prerequisites
- Windows 10/11 with Python 3.11+ installed and added to PATH
- Docker Desktop installed and running (for MongoDB)
- Git (optional, if cloning the repo)

## Steps
1. Open a terminal (PowerShell or Command Prompt) in the project folder.
2. Start MongoDB using Docker Compose:
   ```bash
   docker compose up -d
   ```
3. Create and activate a virtual environment:
   - PowerShell
     ```bash
     python -m venv .venv
     .\.venv\Scripts\Activate
     ```
   - Command Prompt
     ```cmd
     python -m venv .venv
     .\.venv\Scripts\activate.bat
     ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Run the app:
   ```bash
   python main.py
   ```

## Stopping
- Close the Tkinter window to stop the app.
- Stop MongoDB:
  ```bash
  docker compose down
  ```

## Where Data Lives
- SQLite curated DB: `ecopulse.sqlite` in the project root.
- MongoDB raw landing: Docker volume `mongo_data` (managed by Docker Compose).

## Running Tests
- Make sure the virtual environment is active and dependencies are installed (`pip install -r requirements.txt`).
- Start MongoDB (`docker compose up -d`) so the MongoDB-specific tests run instead of being skipped.
- Execute the database checks from the project root:
  ```bash
  python -m unittest discover -s tests -p "test_*.py"
  ```

## Inspecting Databases Visually
- **MongoDB:** Install MongoDB Compass to browse the `raw_fetches` and `scraped_pages` collections.
- **SQLite:** Use DB Browser for SQLite or SQLiteStudio to open `ecopulse.sqlite`. SQL Server Management Studio (SSMS) can also view SQLite when paired with an ODBC driver.

## Troubleshooting
- **MongoDB unavailable**: Ensure Docker Desktop is running, and rerun `docker compose up -d`. The Data Ops tab will show Mongo status; the scheduler button stays disabled when Mongo is down.
- **Port 27017 in use**: Stop other MongoDB instances or change the port mapping in `docker-compose.yml`.
- **Tkinter missing**: Install Python with Tcl/Tk support (included in standard Windows installer) or reinstall Python.
- **SSL / network errors**: The app retries once per request. Verify network connectivity; fetch will still log errors to SQLite run log.
