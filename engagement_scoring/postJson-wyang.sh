curl -u wyang:$WYANG_AEM_PW  -X POST  -F file=@"page_scores.json" https://author1.dev.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -u wyang:$WYANG_AEM_PW  -X POST  -F file=@"page_scores.json" http://localhost:4502/content/dam/rockwell-automation/sites/data.createasset.html