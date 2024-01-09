# Big Data Project Winter Semester 2023

## Authors: Zuzanna Kotlińska, Wiktor Jakubowski.

### Project structure

- `analysis`
  - `powerbi`
    - `DrugEventsAnalysis.pbix` : Power BI analysis dashboard of FDA Drug Events from year 2023.
    - `DrugEventsAnalysis.pdf` : Power BI analysis report of FDA Drug Events from year 2023.   
- `spark`
  - `XXX

- `data`
  put your source data files here.

- `environment.yml` : YAML file specifying the project's Python environment configuration (requires conda).

- `integration`
  - `hbase`
    - `get_events_from_api.sh` : Shell script to fetch events from an FDA API for HBase integration.
    - `preprocess_api_drug_events.py` : Python script for preprocessing drug events data for HBase.
    - `preprocess_api_drug_events.sh` : Shell script for preprocessing drug events data for HBase used by NiFi.
  - `hive`
    - `preprocess_country_codes.py` : Python script for preprocessing country codes data for Hive integration.
    - `preprocess_country_codes.sh` : Shell script for preprocessing country codes data for Hive integration used by NiFi.
    - `preprocess_products.py` : Python script for preprocessing product data for Hive integration.
    - `preprocess_products.sh` : Shell script for preprocessing product data for Hive integration used by NiFi.
  - `nifi_template.xml` : XML file containing NiFi template configurations.

- `utils`
  - `commands.txt` : Text file containing utility commands for setup in the VM.

### Data

For source data, navigate to the following external sources:
* Country Codes CSV: [Datahub.io](https://datahub.io/core/country-list?fbclid=IwAR2oJH17RQzK0fugd627E39bDtDt-yHNuvyzm-E7aW_NcjbIziTqEyGE5D4),
* Drugs description CSV: [FDA database](https://open.fda.gov/apis/drug/drugsfda/download/),
* FDA Drug Events jsons: [openFDA API](https://open.fda.gov/apis/drug/event/).


