# CMS Medicare Provider & Service Outlier Explorer (2023)

This project ingests the CMS **Medicare Physician & Other Practitioners — by Provider and Service (2023)** dataset (single-year, public) and generates:
- quick **quality checks + derived metrics**
- a set of **outlier-oriented visualizations**
- **local worklist CSVs** to prioritize review candidates

The aim is to demonstrate an end-to-end analytics workflow (Python + pandas + matplotlib) that is directly transferable to revenue-cycle environments (e.g., Epic reporting / denial management), while using public data and avoiding redistribution of restricted content.

## What the dataset represents (grain)
Each row is aggregated at:
**Rendering NPI × HCPCS code × Place of Service (Facility vs Non-facility)**  
with provider geography (state, ZIP, RUCA), provider specialty, and charge/allowed/paid measures.

## Outputs
When you run the script, it produces:
- charts (shown in a matplotlib window)
- local CSV worklists (not committed to git):
  - `outliers_intensity_top.csv`
  - `outliers_compression_top.csv`
  - `outliers_allowed_dollars_top.csv`

## Data & licensing note
This repo does **not** include raw CMS data files.
Download the CMS dataset yourself (accepting CMS/AMA terms as applicable), then place:

`Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2023.zip`

in the project folder (or in a local `/data/` folder if you prefer).

The script generates worklist CSVs locally for review; generated files are intentionally **not** committed to version control.

## How to run
### 1) Create/activate a virtual environment (recommended)
**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
