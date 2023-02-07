## Content Score Servlet DEV Test
1. Input URL in browser to request the endpoint in AEM server with the format of `https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score?path={relative-path}&key=autoEScore`
2. Manually update the relateive path in the URL for the pages under rockwell domain e.g. 
	- Valid URL check:
		- https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score?path=/en-us.html&key=autoEScore
		- https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score?path=/support/documentation/literature-library.html&key=autoEScore
		- https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score?path=/capabilities/academy-advanced-manufacturing.html&key=autoEScore
	- No-valid URL or New URL check:
		- https://dev-aem.rockwellautomation.com/bin/rockwell-automation/content-score?path=/empty-page.html&key=autoEScore


Note: The page score and related metrics will be scheduled to update twice a month based on the latest user data from RA-MDP server. 
The valid pages have different values in the kYieldModified and traffic metrics to describe this page.  
The invalid page have null as placeholder in the kYieldModified and traffic metrics.
All of them have the same labelProportion metrics, which are shared attributes across all the URLs.



For testing the POST request on the current metrics related to engagement score (wyang only)

Note: The page score and related metrics will be calculated and scheduled to update twice a month based on the latest user data from RA-MDP server. The user ra-content-score-user has the permission to send the POST request to upload the calculation result and refresh the metrics holding by the Endpoint.
1. Use Curl to upload the processed data into both author and publisher in AEM DAM
2. Send POST request with the valid user ra-content-score-user to refresh the metrics for each page
3. it will successfully refresh the asset in DAM, and response message Reset AutoMap success allowed.
4. Send the GET request with adding random query params to bypass Akamai cache.