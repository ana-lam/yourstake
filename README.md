## YourStake Data Task

#### Purpose:
YourStake provides data on social and environmental issues to allow investors to decide which companies best align with their values. One of our goals is to gather global toxic air pollution data to tie to corporate entities in a methodologically consistent manner. Toxic air pollution reporting to global Pollution Release Transfer Registries (PRTRs) allows us to standardize toxic air pollution metrics for companies across geographical regions.

#### Goal:

1. Using the language and libraries of your choice, write code to produce a CSV of company names, amount of toxic air pollution, and description of details (e.g. facility locations, purpose of facility, names of chemicals released…).

<br>
Code to produce CSV is `yourstake.py` and the CSV is saved as `EPA_EEA_table.csv`.
<br>

<br>

2. Describe the steps you'd take to make your script robust enough to run unsupervised on a regular basis.

<br>
To make my script robust and run unsupervised, I would scale my query and content-to-pandas functions to take inputs from different data sources if possible to reach the goal of gathering global toxic air pollution data in a methodologically consistent matter. This would necessitate doing more research of different PRTR databases. I would write functions to conduct further necessary data transformations and cleaning to account for edge cases that can throw errors later down the ETL workflow and to standardize toxic air pollution metrics across inventories such as the EPA's and EEA's. I would also load the processed data into another destination such as SQL to hold standardized global toxic air pollution metrics.
<br>
<br>
To regularly run the script unsupervised I would use an ETL tool such as Apache Airflow to manage the workflow and schedule jobs based on database update needs and source data update frequency.
<br>

<br>

3. List of quality checks you would want to write when putting into web app, and bullet point outline of how.

<br>

Quality Check for ETL pipeline - Ensure expected or necessary data is there and usable
* Account for edge cases and either replace or throw out incomplete data when data is extracted.
* Create predicates with descriptive print error messages that checks data type to ensure data is in usable condition.
* Create a log process. I would log names and times of file extraction to monitor ETL extraction and transformation.
* Generate email or Slack alerts for specific errors and malfunctions.

Quality Check for Loaded Data - Ensure loaded data to be used in web app is valid
* Write a script that ensures that data values are within expected bounds
* Integrate incremental and refresh loading to ensure only new or updated data is being loaded into the web app. This could be down by comparing
data in final destination with data being loaded in.

Web App Integration Quality Check - Ensure web app holds updated data
* Generate log for data integration errors and fails with messages
* Maybe perform data visualization to spot unexpected data values/outliers that are outside of expected or predicted data values or trends
<br>

<br>


**Resources**:

US EPA

- https://www.epa.gov/toxics-release-inventory-tri-program

- Lists air pollution by facility

- Has mapping of facility to parent company

EU PRTR

- https://prtr.eea.europa.eu/

- Lists air pollution by facility

- Has mapping of facility to parent company
