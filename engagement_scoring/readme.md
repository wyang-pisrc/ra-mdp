## start the scheduler
nohup python -m pipenv run python scoreScheduler.py > nohup_scheduler.log &

## kill the scheduler process
ps -ef | grep scoreScheduler

## install new package
python -m pipenv install apscheduler


