# Rockwell Automation Marketing Data Platform

## Dev Notes

- Python formatting provider: `black`
- SQL formatting provider: `SQLTools` VS code extension

## Project structure

### DB

- Contains information one retrieving and using data from the MDP server
- MDP server IP: `104.43.220.142`
- PiSrc folder location: `/usr/local/src/pisrc`
- Database (MSSQL) URL: `sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net`
- To access the database: `sqlcmd -S sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net`
- `analytics-db.ipynb`: Shows how to access MDP information and relevant tables

### API

- `analytics-api.ipynb`: Shows how to access Eloqua and Adobe Analytics APIs in Python

### Exploratory Data Analysis

- Contains code to generate data for a presentation
