### scheduled monthly 
import apscheduler
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import time


def main(isTest=False):

  print("scheduler start for page scoring")
  def scheduledtask():
      
      
      try:
        # run ETL
        # os.system("python -m pipenv run python 3-aemRaw_ETL.py")

        # run report to generate page_scores.json
        os.system("python 4-report.py")
        # os.system("python -m pipenv run python 4-report.py")

        # upload to AEM target servers
        os.system("./11-postJsonMetrics.sh")
        print("Waiting AEM processing for 60s.")
        
        time.sleep(60)
        
        os.system("./12-refreshHashMap.sh")
        print("refresh hashmap in different envs.")
      
      except Exception as error:
        ### if error message, send email to wyang
        print("error: ", error)

  
  if isTest is False:
    # schedule task
    sched = BlockingScheduler()
    sched.add_job(scheduledtask, 'interval', hours=6, id='update_pagescores_json')
    sched.start()
  else:
    # local testing
    scheduledtask()
  
if __name__ == "__main__":
    main(isTest=True)

