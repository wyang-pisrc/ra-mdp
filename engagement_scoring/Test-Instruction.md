## Step to data processing

### ETL pipeline 
1. SSH into ra-mdp server
2. Go to engatement_scoring folder and start the pipenv with `python -m pipenv shell`
3. Confirm the config.txt is correct
4. Manually steps
	1. Start processing with `python 3-aemRaw_ETL.py` to get the data summary
	2. Start processing with `python 4-report.py` to get the calculated page score json file 

### scheduler to sync result to AEM
1. Find the credential for ra-content-score-user from 1password. The password starts with _jTK
2. Store the password into local variable by `export CONTENT_SCORE_ADMIN_PW={the pass word you find on 1password};`
3. Run the shell script under this directory to upload the json file in current directory to AEM servers: `./postJson-wyang.sh`
	- it will refresh the hashmap in servlet by the post request as well in the scripts


## Automated scheduled task
1. Start the scheduler by `nohup python -m pipenv run python scoreScheduler.py > nohup_scheduler.log &`
2. Maintain by `ps -ef | grep scoreScheduler`


## ra_launch Frontend Tesing
1. check the file is sync with ra_launch `engagement_scoring/Frontend/bayesianMetrics-dev.js`
2. run `piSightMain()` in the brower console under rockwell website. It should show the current user probablity profiles and page profiles.


## TODO
1. change the probability calculation into exponential log plus
3. add edge case examination -> stable
4. add the calculation result into cookie
5. find two or more different prediction result that make sense
6. backtest result - accuracy


### Done
3. config the ra-content-score-user in stage properly
3. add stage env into curl post request
1. add akamai no-cache header
2. add skip curl SSL credential verification