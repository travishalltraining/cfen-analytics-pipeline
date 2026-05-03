name: Upload Financial Data to BigQuery

on:
  push:
    paths:
      - 'financials/**.xlsx'
      - 'financials/**.xls'
  workflow_dispatch:
    inputs:
      filename:
        description: 'P&L filename in financials/ folder'
        required: false

jobs:
  upload-financials:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install openpyxl google-cloud-bigquery

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      - name: Find and upload P&L files
        env:
          GCP_PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
        run: |
          for f in financials/*.xlsx financials/*.xls; do
            [ -f "$f" ] || continue
            echo "Uploading: $f"
            python scripts/upload_financials.py "$f"
          done
