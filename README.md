# Pentaho to PySpark Converter

Convert Pentaho Data Integration (PDI) projects (`.kjb` / `.ktr`) into a single Databricks-ready PySpark file, with optional push to your Databricks workspace.

## Features

- Upload a Pentaho project ZIP and generate combined PySpark code
- Single output file (all transformations + main workflow)
- Web UI with conversion analysis and logs
- Test Databricks connection and push notebooks (split into cells)
- CLI for batch conversion
- **Job Entries** supported across General, Mail, File Management, Conditions, Scripting, Bulk Loading, XML, Utility, Repository, File Transfer, and File Encryption categories
  (see [docs/SUPPORTED_JOB_ENTRIES.md](docs/SUPPORTED_JOB_ENTRIES.md))

## Project structure

```
pentaho_converter/   # Conversion engine
databricks/          # Databricks workspace API client
templates/           # Web UI
tests/               # Unit tests
samples/             # Example Pentaho project
app.py               # Flask web application
converter.py         # CLI entry point
```

## Local setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
python converter.py serve
```

Open http://127.0.0.1:5000

### CLI conversion

```bash
python converter.py path\to\project.zip output_dir\
```

## Databricks

Upload-only deploy compatible with **Databricks Free Edition** (no cluster required):

Enter in the web UI (not stored on the server):

- **Workspace URL** (required) — e.g. `https://adb-xxxxx.azuredatabricks.net`
- **Personal access token** (required) — from Databricks → Settings → Developer → Access tokens
- **Destination folder** (optional) — default `/Workspace/Pentaho_Migration`
- **Cluster ID** (optional) — never required for deploy; jobs are not submitted

Deploy creates folders and uploads the generated project. Run `Master_ETL.py` manually in the Databricks UI (e.g. serverless).

## Tests

```bash
python -m unittest tests.test_pentaho_steps tests.test_general_job_entries -v
```

## Push to GitHub

```bash
git init
git add .
git commit -m "Pentaho to PySpark converter"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/pentaho-pyspark-converter.git
git push -u origin main
```

## Deploy as a web service

This app is a **Flask backend** (not a static site). GitHub Pages hosts static HTML only and cannot run Python/Flask.

Use a Python web host and connect it to your GitHub repo:

### Option A — Render (recommended, `render.yaml` included)

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → **New** → **Blueprint**
3. Connect your GitHub repository
4. Render reads `render.yaml` and deploys automatically
5. Your app URL will be like `https://pentaho-pyspark-converter.onrender.com`

### Option B — Railway / Heroku / Azure App Service

- **Build command:** `pip install -r requirements.txt`
- **Start command:** `gunicorn app:app --bind 0.0.0.0:$PORT`

## GitHub Pages + web service (landing page)

If you want a public page on GitHub Pages that links to the hosted converter:

1. Create a repo `YOUR_USERNAME.github.io` or enable Pages on a `docs/` folder
2. Add a simple `index.html` that redirects or links to your deployed Render/Railway URL
3. In repo **Settings → Pages**, set source to `main` / `docs`

The converter UI itself must run on Render/Railway/etc., not on GitHub Pages.

## Environment variables (optional)

Copy `.env.example` to `.env` for default UI settings only (catalog, schema, notebook folder). Databricks credentials are entered in the browser.
