# Rockwell Automation Marketing Data Platform

## Dev Notes

- Python version: `> 3.10`
  - Recommended initialization:
    ```sh
    python3 -m pip install --user pipenv
    python3 -m pipenv install
    ```
- Python formatting provider: `black`
- SQL formatting provider: `SQLTools` VS code extension

## Project structure

### API

- `analytics-api.ipynb`: Shows how to access Eloqua and Adobe Analytics APIs in Python
- Note: still uses its own `requirements.txt` (TODO: transfer to pipenv)

### DB

- Contains information on retrieving and using data from the MDP server
- MDP server IP: `104.43.220.142`
- PiSrc folder location: `/usr/local/src/pisrc`
- Database (MSSQL) URL: `sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net`
- To access the database: `sqlcmd -S sqlsvr-0092-mdp-02.85f8a2f57eaf.database.windows.net`
- `analytics-db.ipynb`: Shows how to access MDP information and relevant tables

### Engagement scoring

- Binge experience engagement scoring implementation

### Misc projects

- Data collection for specific presentations or inquiries
