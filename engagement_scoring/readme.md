## install new package
python -m pipenv install apscheduler

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
3. Run the shell script under this directory to upload the json file in current directory to AEM servers: 
	- `os.system("./11-postJsonMetrics.sh")`
	- `os.system("./12-activatePublisher.sh")`
	- `os.system("./13-refreshHashMap.sh")`


## Automated scheduled task
0. Enter the pipenv with `python -m pipenv shell`
1. Start the scheduler by `nohup python scoreScheduler.py > nohup_scheduler.log &`
2. Maintain by `ps -ef | grep scoreScheduler`


## ra_launch Frontend Tesing
1. check the file is sync with ra_launch `engagement_scoring/Frontend/bayesianMetrics-dev.js`
2. run `piSightMain()` in the brower console under rockwell website. It should show the current user probablity profiles and page profiles.


## TODO
2. data align and compare
3. Multi-class prediction adaptation
4. simplify the Analyzer class logic

### Done
1. change the probability calculation into exponential log plus (how to math.exp of BigDecimal) (only implement with python)
4. add the calculation result into cookie
3. config the ra-content-score-user in stage properly
3. add stage env into curl post request
1. add akamai no-cache header
2. add skip curl SSL credential verification
3. add edge case examination -> stable
5. find two or more different prediction result that make sense
6. backtest result - accuracy
3. Update the tracking visit list as a set

## Naming
1. Query Loader
1. Batch Transformer = ETL
3. Data Wrangler = Data Exploratory Processing 
2. Modeling = Analyzer = Trainer 
2. AEM Scheduler = Model deployment
2. Real-Time Inference = API hosting = Model Serving = Servlet/AWS Lambda 




### tracking parts -> losing digits in the end by multiply.
piSight {"lead":{"lead-Good":"0.00000004221163781073974785197933978180673767319486","lead-Bad":"0.00000005259416679312511092435879419826025890386306"},"role":{"role-Csuite":"0.00000004833728446261365405287932300617896084369349","role-Manager":"0.00000003220742115626293727959298135475142656750311","role-Engineer":"0.00000005931907224084082664905055293111821867766086","role-Other":"0.00000008454991459830220016790222404310106689107225"},"industry":{"industry-Aerospace":"0.00000000015381968588085486869319820375256355515196","industry-Infrastructure":"0.00000000222186651648571803710904165731341200511375","industry-Automotive_Tire":"0.00000064947676132041550703712963250525648589104895","industry-Cement":"0.00000000001376454974810088594200262563376741844628","industry-Chemical":"0.00000001752520296017202123094342488685425508119329","industry-Entertainment":"0.00000000000000000000000261820300348611107485191069","industry-Fibers_Textiles":"0.00000048171702935543703977006526622472012741047365","industry-Food_Beverage":"0.00000000132124937905899094311218587090913895918422","industry-Glass":"0.0000000000576310128446024669048294933020324881496","industry-HVAC":"0.00000000000000000000000000208477660724305999541011","industry-Household_Personal_Care":"0.00000000003461476554871307496709301573880081334072","industry-Life_Sciences":"0.00000047863997530558073733835241669950061888629722","industry-Marine":"0.0000000123083058421608692737295053997503177750641","industry-Metals":"0.00000000049499731827907654637002968018768763459946","industry-Mining":"0.0000000005151098875765036195499987982398436528153","industry-Oil_Gas":"0.00000000095862917262059193963992209455039182539273","industry-Power_Generation":"0.00000000222489129950309517252532937950933286584678","industry-Print_Publishing":"0.00000000004788794482824545115065771186830556270567","industry-Pulp_Paper":"0.00000000236557902001282464150753677620341753122597","industry-Semiconductor":"0.00000001201364096447551807384863210369967465936464","industry-Whs_EComm_Dist":"0.","industry-Waste_Management":"0.","industry-Water_Wastewater":"0.00000000801312999463197196363998818860381734778002","industry-Other":"0.00000013785774403485465108417817958775133344598439"}}


piSight {"lead":{"lead-Good":"0.00000000057825518446790597512120950200194510853165","lead-Bad":"0.00000000059693605044188979717555248022295907648206"},"role":{"role-Csuite":"0.00000000067614726797883788717139369846436263167376","role-Manager":"0.00000000040760647258705721578954127104166995243601","role-Engineer":"0.00000000058557907031101658670227998522581152816551","role-Other":"0.0000000010703192700968048207721114276567600401455"},"industry":{"industry-Aerospace":"0.00000000000000682718009010646320823900301143572873","industry-Infrastructure":"0.00000000000001830596491830084254158714629539366862","industry-Automotive_Tire":"0.00000000902355939917843352181095784150450675652148","industry-Cement":"0.00000000000009889519358116530636976697446625355454","industry-Chemical":"0.00000000061922477574229167147130281228115735505952","industry-Entertainment":"0.00000000000000000000000000001739627615151310813581","industry-Fibers_Textiles":"0.00000000178754829960376448561408971854769607523451","industry-Food_Beverage":"0.00000000001202635604842006605780902921922696544268","industry-Glass":"0.00000000000000039823788053151198524314383644488088","industry-HVAC":"0.00000000000000000000000000000000332448014387198345","industry-Household_Personal_Care":"0.00000000000141624759995892056586110774437443213947","industry-Life_Sciences":"0.00000000247804569166033375302611784727529306564472","industry-Marine":"0.0000000005276061230040348389446378196447617244535","industry-Metals":"0.00000000000238560645855312167586678618015949478269","industry-Mining":"0.00000000000337928368100118732676170693060864493024","industry-Oil_Gas":"0.00000000001375380677496547408329541610328488884506","industry-Power_Generation":"0.00000000000005558397725098694809144535751077772936","industry-Print_Publishing":"0.0000000000000014254670716144128420362149511291294","industry-Pulp_Paper":"0.00000000010393392256991568086179318260064358425691","industry-Semiconductor":"0.00000000041893712616566451936603458845813121210938","industry-Whs_EComm_Dist":"0.","industry-Waste_Management":"0.","industry-Water_Wastewater":"0.00000000010754719539070732590939521092916882277201","industry-Other":"0.00000000175470523907780974102508993380470260599872"}}
