## wei version
### File explaination
1. exploration sql
2. 2-retrieve_aemRaw-initial_version.py
	- for getting the initial raw data (contains most of useful columns) and exported csv is too large
3. 3-aemRaw_ETL.py
	- integrating the ETL to extract, transform, clean data, including
		- Retrieve only target columns - to reduce data size
		- minute granularity dedup
		- normalized page URL 
	- TODO
		- get the summary by month and store the summary for futher calculation/analysis/prediction