import csv
import pymysql
import glob
import re
import socket, struct
from dateutil.parser import parse
from datetime import datetime

"""
CREATE TABLE `crm_data` (
	`leadid` VARCHAR(255) NOT NULL,
	`emailaddress1` VARCHAR(64) NULL,
	`firstname` VARCHAR(64) NULL,
	`lastname` VARCHAR(128) NULL,
	`jobtitle` VARCHAR(128) NULL,
	`companyname` VARCHAR(128) NULL,
	`ra_leadstagename` VARCHAR(32) NULL,
	`ra_salesrejectionreasonname` VARCHAR(128) NULL,
	`ra_telerejectionreasonname` VARCHAR(128) NULL,
	`address1_country` VARCHAR(32) NULL,
    PRIMARY KEY(leadid)
) DEFAULT CHARSET=utf8mb4;

grant all privileges on rockwell.* to 'page_scoring'@'localhost'
CREATE INDEX crm_email_idx ON crm_data(emailaddress1);
"""

mydb = pymysql.connect(host='localhost',
                             user='rockwell',
                             password='rockwell',
                             db='page_scoring',
                             charset='utf8mb4')


cursor = mydb.cursor()

csv_data = csv.reader(open('crm_Lead_20221116_all.csv', 'r'))
next(csv_data)
"""
0 :
1 : accountid
2 : accountidname
3 : address1_addressid
4 : address1_addresstypecode
5 : address1_addresstypecodename
6 : address1_city
7 : address1_country
8 : address1_county
9 : address1_fax
10 : address1_latitude
11 : address1_line1
12 : address1_line2
13 : address1_line3
14 : address1_longitude
15 : address1_name
16 : address1_postalcode
17 : address1_postofficebox
18 : address1_shippingmethodcode
19 : address1_shippingmethodcodename
20 : address1_stateorprovince
21 : address1_telephone1
22 : address1_telephone2
23 : address1_telephone3
24 : address1_upszone
25 : address1_utcoffset
26 : address2_addressid
27 : address2_addresstypecode
28 : address2_addresstypecodename
29 : address2_city
30 : address2_country
31 : address2_county
32 : address2_fax
33 : address2_latitude
34 : address2_line1
35 : address2_line2
36 : address2_line3
37 : address2_longitude
38 : address2_name
39 : address2_postalcode
40 : address2_postofficebox
41 : address2_shippingmethodcode
42 : address2_shippingmethodcodename
43 : address2_stateorprovince
44 : address2_telephone1
45 : address2_telephone2
46 : address2_telephone3
47 : address2_upszone
48 : address2_utcoffset
49 : budgetamount
50 : budgetamount_base
51 : budgetstatus
52 : budgetstatusname
53 : campaignid
54 : campaignidname
55 : companyname
56 : confirminterest
57 : confirminterestname
58 : contactid
59 : contactidname
60 : createdby
61 : createdbyname
62 : createdon
63 : createdonbehalfby
64 : createdonbehalfbyname
65 : customerid
66 : customeridname
67 : customeridtype
68 : decisionmaker
69 : decisionmakername
70 : donotbulkemail
71 : donotbulkemailname
72 : donotemail
73 : donotemailname
74 : donotfax
75 : donotfaxname
76 : donotphone
77 : donotphonename
78 : donotpostalmail
79 : donotpostalmailname
80 : donotsendmarketingmaterialname
81 : donotsendmm
82 : emailaddress1
83 : emailaddress2
84 : emailaddress3
85 : estimatedamount
86 : estimatedamount_base
87 : estimatedclosedate
88 : estimatedvalue
89 : evaluatefit
90 : evaluatefitname
91 : exchangerate
92 : fax
93 : firstname
94 : followemail
95 : followemailname
96 : fullname
97 : importsequencenumber
98 : industrycode
99 : industrycodename
100 : initialcommunication
101 : initialcommunicationname
102 : isautocreatename
103 : isprivatename
104 : jobtitle
105 : lastname
106 : lastonholdtime
107 : lastusedincampaign
108 : leadid
109 : leadqualitycode
110 : leadqualitycodename
111 : leadsourcecode
112 : leadsourcecodename
113 : masterid
114 : masterleadidname
115 : merged
116 : mergedname
117 : middlename
118 : mobilephone
119 : modifiedby
120 : modifiedbyname
121 : modifiedon
122 : modifiedonbehalfby
123 : modifiedonbehalfbyname
124 : msdyn_gdproptout
125 : msdyn_gdproptoutname
126 : msdyn_leadgrade
127 : msdyn_leadgradename
128 : msdyn_leadkpiid
129 : msdyn_leadkpiidname
130 : msdyn_leadscore
131 : msdyn_leadscoretrend
132 : msdyn_leadscoretrendname
133 : msdyn_predictivescoreid
134 : msdyn_predictivescoreidname
135 : msdyn_salesassignmentresult
136 : msdyn_salesassignmentresultname
137 : msdyn_segmentid
138 : msdyn_segmentidname
139 : need
140 : needname
141 : numberofemployees
142 : onholdtime
143 : originatingcaseid
144 : originatingcaseidname
145 : overriddencreatedon
146 : ownerid
147 : owneridname
148 : owneridtype
149 : owningbusinessunit
150 : owningteam
151 : owninguser
152 : pager
153 : parentaccountid
154 : parentaccountidname
155 : parentcontactid
156 : parentcontactidname
157 : participatesinworkflow
158 : participatesinworkflowname
159 : preferredcontactmethodcode
160 : preferredcontactmethodcodename
161 : prioritycode
162 : prioritycodename
163 : processid
164 : purchaseprocess
165 : purchaseprocessname
166 : purchasetimeframe
167 : purchasetimeframename
168 : qualifyingopportunityid
169 : qualifyingopportunityidname
170 : ra_account
171 : ra_accountname
172 : ra_areyouengagedwithraoradistributor
173 : ra_areyouengagedwithraoradistributorname
174 : ra_blockreroutetodistributor
175 : ra_blockreroutetodistributorname
176 : ra_businessprocess
177 : ra_businessprocessname
178 : ra_channeltomarket
179 : ra_channeltomarketname
180 : ra_cnvcmpmostrecent
181 : ra_cnvcmporiginal
182 : ra_cnvdatemostrecentcmp
183 : ra_cnvdatemostrecentsrc
184 : ra_cnvdateoriginal
185 : ra_cnvsrccmpidmostrecent
186 : ra_cnvsrccmpidoriginal
187 : ra_cnvsrcmostrecent
188 : ra_cnvsrcoriginal
189 : ra_contact
190 : ra_contactname
191 : ra_datasrccode
192 : ra_datemodified
193 : ra_datesalesaccepted
194 : ra_datesalesqualified
195 : ra_dateteleaccepted
196 : ra_dateteleassigned
197 : ra_datetelequalified
198 : ra_demandsource
199 : ra_demandsourcename
200 : ra_emailwhoknowswhom
201 : ra_funding
202 : ra_fundingname
203 : ra_generalengagementscore
204 : ra_isthereanactiveneed
205 : ra_isthereanactiveneedname
206 : ra_ketplaysection
207 : ra_ketplaysectionname
208 : ra_keyplayurl
209 : ra_leadacceptanceduedate
210 : ra_leadlineitemcount
211 : ra_leadqualificationduedate
212 : ra_leadstage
213 : ra_leadstagename
214 : ra_ledby
215 : ra_ledbyname
216 : ra_legacyleadid
217 : ra_lmpcontactapr
218 : ra_lmpdateassigned
219 : ra_lmpdateclosed
220 : ra_lmpdatecreated
221 : ra_lmpdatemodified
222 : ra_lmplink
223 : ra_lmpopportunityproduct
224 : ra_lmpopportunitystatus
225 : ra_lmpopportunityvalue
226 : ra_lmpopportunityvalue_base
227 : ra_lmpowneremail
228 : ra_lmpstatus
229 : ra_opportunitygroup
230 : ra_opportunitygroupname
231 : ra_opportunityname
232 : ra_opportunitytype
233 : ra_opportunitytypename
234 : ra_opportunityzone
235 : ra_opportunityzonename
236 : ra_parentlead
237 : ra_parentleadname
238 : ra_primarycontactidentified
239 : ra_primarycontactidentifiedname
240 : ra_reroutetodistributor
241 : ra_reroutetodistributorname
242 : ra_salesaccepted
243 : ra_salesacceptedname
244 : ra_salesdisqualifyreason
245 : ra_salesdisqualifyreasonname
246 : ra_salesplanid
247 : ra_salesplanidname
248 : ra_salesqualified
249 : ra_salesqualifiedname
250 : ra_salesrejectionreason
251 : ra_salesrejectionreasonname
252 : ra_secondarysourcecampaign
253 : ra_secondarysourcecampaignname
254 : ra_specificationposition
255 : ra_specificationpositionname
256 : ra_stateprovinceid
257 : ra_stateprovinceidname
258 : ra_teleaccepted
259 : ra_teleacceptedname
260 : ra_teledisqualifyreason
261 : ra_teledisqualifyreasonname
262 : ra_telequalified
263 : ra_telequalifiedname
264 : ra_telerejectionreason
265 : ra_telerejectionreasonname
266 : ra_telerep
267 : ra_telerepname
268 : ra_wasteleperformed
269 : ra_wasteleperformedname
270 : ra_wouldyoulikefurthercontact
271 : ra_wouldyoulikefurthercontactname
272 : ra_z_internal_preventloopingofaccount
273 : ra_z_internal_preventloopingofaccountname
274 : ra_zcurrenter
275 : ra_zpreviouser
276 : ra_zsalesacceptancestageentered
277 : relatedobjectid
278 : relatedobjectidname
279 : revenue
280 : revenue_base
281 : salesstage
282 : salesstagecode
283 : salesstagecodename
284 : salesstagename
285 : salutation
286 : schedulefollowup_prospect
287 : schedulefollowup_qualify
288 : sic
289 : slaid
290 : slainvokedid
291 : slainvokedidname
292 : slaname
293 : stageid
294 : statecode
295 : statecodename
296 : statuscode
297 : statuscodename
298 : subject
299 : teamsfollowed
300 : telephone1
301 : telephone2
302 : telephone3
303 : timezoneruleversionnumber
304 : transactioncurrencyid
305 : transactioncurrencyidname
306 : utcconversiontimezonecode
307 : websiteurl
308 : owningbusinessunitname
309 : ra_integrationsource
310 : ra_integrationsourcename
311 : ra_externalrecordid
312 : ra_externalreroute
313 : ra_externalreroutename

0 : 0
1 :
2 :
3 : 865286E1-4E0E-4F9C-BDC7-14FA231A5529
4 : 1
5 : Default Value
6 : Milwaukee
7 : USA
8 :
9 :
10 :
11 : 1532 E Oklahoma Ave
12 :
13 :
14 :
15 :
16 : 53207-2433
17 :
18 : 1
19 : Default Value
20 : WI
21 :
22 :
23 :
24 :
25 :
26 : 4F79E350-E0AA-44DB-B88B-75C861BD1106
27 : 1
28 : Default Value
29 :
30 :
31 :
32 :
33 :
34 :
35 :
36 :
37 :
38 :
39 :
40 :
41 : 1
42 : Default Value
43 :
44 :
45 :
46 :
47 :
48 :
49 :
50 :
51 :
52 :
53 : 623DE352-2D9F-EC11-B400-000D3A8A577C
54 : DX Plex Sprint Associates NA
55 : Milwaukee Forge
56 : False
57 : Yes
58 :
59 :
60 : 989F88FA-0360-EB11-A812-000D3A9A17E9
61 : EloquaAppUser EloquaAppUser
62 : 2022-03-09 20:00:28
63 :
64 :
65 : 32416E28-B01B-E711-80F7-FC15B4283DA0
66 : Milwaukee Forge
67 : account
68 : False
69 : mark complete
70 : False
71 : Allow
72 : False
73 : Allow
74 : False
75 : Allow
76 : False
77 : Allow
78 : False
79 : Allow
80 :
81 : False
82 : sustarg@milwaukeeforge.com
83 :
84 :
85 :
86 :
87 :
88 :
89 : False
90 : Yes
91 : 1.0
92 :
93 : Gustav (GUS)
94 : True
95 : Allow
96 : Lead: DX Plex Sprint Associates NA
97 :
98 :
99 :
100 :
101 :
102 :
103 :
104 : Automation Engineer
105 : Sustar
106 :
107 :
108 : 321E5A8F-E39F-EC11-B400-00224834C876
109 : 2
110 : Warm
111 :
112 :
113 :
114 :
115 : False
116 : No
117 :
118 :
119 : 88A7E2E1-FA29-E711-80F8-C4346BAC8A9C
120 : Eric Schulz
121 : 2022-03-11 19:22:53
122 : 88A7E2E1-FA29-E711-80F8-C4346BAC8A9C
123 : Eric Schulz
124 : False
125 : No
126 :
127 :
128 : 321E5A8F-E39F-EC11-B400-00224834C876
129 :
130 :
131 :
132 :
133 : 50397D67-1DA0-EC11-B400-00224827F7F2
134 :
135 :
136 :
137 :
138 :
139 :
140 :
141 :
142 :
143 :
144 :
145 :
146 : 320D3DD3-03AA-EA11-A812-000D3A8DB66E
147 : Shelby Sullivan
148 : systemuser
149 : E0A72F9F-25DB-E611-80F0-C4346BACAB64
150 :
151 : 320D3DD3-03AA-EA11-A812-000D3A8DB66E
152 :
153 : 32416E28-B01B-E711-80F7-FC15B4283DA0
154 : Milwaukee Forge
155 : 14D79E60-D68B-EB11-A812-000D3A14D807
156 : Gustav (GUS) Sustar
157 : False
158 : No
159 : 1
160 : Any
161 : 1
162 : Default Value
163 :
164 :
165 :
166 :
167 :
168 :
169 :
170 : 32416E28-B01B-E711-80F7-FC15B4283DA0
171 : Milwaukee Forge
172 :
173 :
174 : 953810001.0
175 : Yes
176 : 4.0
177 : Outcome-based Selling
178 :
179 :
180 :
181 :
182 :
183 :
184 :
185 :
186 :
187 :
188 :
189 : 14D79E60-D68B-EB11-A812-000D3A14D807
190 : Gustav (GUS) Sustar
191 :
192 :
193 :
194 :
195 :
196 : 2022-03-09 20:00:31
197 :
198 : 953810000.0
199 : Marketing Generated
200 :
201 :
202 :
203 : B4
204 :
205 :
206 :
207 :
208 :
209 :
210 :
211 :
212 : 4.0
213 : Awaiting Sales Acceptance
214 : 953810003.0
215 : Rockwell Sales Team
216 :
217 : 082
218 :
219 :
220 :
221 :
222 :
223 :
224 :
225 :
226 :
227 :
228 :
229 :
230 :
231 :
232 :
233 :
234 :
235 :
236 :
237 :
238 : False
239 : No
240 : False
241 : No
242 :
243 :
244 :
245 :
246 :
247 :
248 :
249 :
250 :
251 :
252 :
253 :
254 :
255 :
256 :
257 :
258 :
259 :
260 :
261 :
262 :
263 :
264 :
265 :
266 :
267 :
268 : False
269 : No
270 :
271 :
272 : False
273 : No
274 :
275 :
276 : Lead Routed Successfully.
277 :
278 :
279 :
280 :
281 :
282 : 1
283 : Default Value
284 :
285 :
286 :
287 :
288 :
289 :
290 :
291 :
292 :
293 :
294 : 0
295 : Open
296 : 1
297 : New
298 : Lead: DX Plex Sprint Associates NA
299 :
300 : 414-722-1864
301 :
302 :
303 :
304 : 2F283B3B-EA62-E611-80E0-C4346BAC8A9C
305 : Dollar
306 :
307 :
308 : NA - SOUTHWEST
309 :
310 :
311 :
312 :
313 :
"""

stmt = 'INSERT INTO crm_data (leadid, emailaddress1, firstname, lastname, jobtitle, companyname, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, address1_country) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

i=0;
for row in csv_data:
    if (i < 1):
        print("%i : %s" % (i,row))
        pi=0;
        i+=1
        for col in row:
            print("%i : %s" % (pi,col))
            pi+=1
    try:
        leadid=str(row[108])
        emailaddress1=str(row[82] or None).lower()
        firstname=str(row[93] or None)
        lastname=str(row[105] or None)
        jobtitle=str(row[104] or None)
        companyname=str(row[55] or None)
        ra_leadstagename=str(row[213] or None)
        ra_salesrejectionreasonname=str(row[251] or None)
        ra_telerejectionreasonname=str(row[265] or None)
        address1_country=str(row[7] or None)
    except Exception as e:
        print("Import parse exception : %s" % e)
        print(row)

    cursor.execute(stmt, (leadid, emailaddress1, firstname, lastname, jobtitle, companyname, ra_leadstagename, ra_salesrejectionreasonname, ra_telerejectionreasonname, address1_country));

mydb.commit()
mydb.close()
