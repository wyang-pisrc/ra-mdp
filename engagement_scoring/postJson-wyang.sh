curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://author1.dev.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST "https://author1.dev.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"

curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://publish1.dev.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST "https://publish1.dev.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"


curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://author1.stage.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST "https://author1.stage.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"

curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://publish1.stage.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST "https://publish1.stage.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"

curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://publish2.stage.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
curl -k -u ra-content-score-user:$CONTENT_SCORE_ADMIN_PW -X POST "https://publish2.stage.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"



# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://author1.prod.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST "https://author1.prod.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"

# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://publish1.prod.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST "https://publish1.prod.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"

# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST  -F file=@"page_scores.json" https://publish2.prod.rockwellautomation.adobecqms.net/content/dam/rockwell-automation/sites/data.createasset.html
# curl -k -u ra-content-score-user:$PROD_CONTENT_SCORE_ADMIN_PW -X POST "https://publish2.prod.rockwellautomation.adobecqms.net/bin/rockwell-automation/content-score?path=/products/details.1794-oa16.html&key=autoEScore&value=refreshAutoMap"
