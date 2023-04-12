# Welcome to my GCP related projects' portfolio!
All files were made by Leandro Fumio Kino.

[![Project Status: Active  The project is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)

Here, you can read more about my personal projects and how they may help you in your routine tasks with Python and SQL in your GCP projects.

## About me
With 2 years of experience as Data Engineer and Cloud Solutions, I've been working in a worldwide digital marketing agency, and I'm responsible for all data-related automations of all data engineers and business intelligence analysts. Also, I have 6 years as Mechanical Engineering in R&D.

## About the projects:
### [Case/When from sheets to Bigquery](https://github.com/fumioq/gcp-projects/tree/main/case_when_sheets_to_bq/scripts)
BI Analysts don't have permissions to edit and delete datasets and tables, and they're working with Google Data Studio.
I connect the tables and views from Bigquery to Data Studio, and they do an additional transform operation to correct some data mistakes through Case/When.
The problem is that Data Studio isn't a good tool to process data and 300+ lines of Case/When in a Calculated Field was making the Dashboard heavy, taking up to 2 minutes to load every graph.
So I developed this code to allow the BI Analyst to insert Case/When lines in a Google Sheets and this code inserts them on the view that is connected to Data Studio.
 - Benefits: ***reduced*** the dashboard loading time from ***120s to 6s*** (95% loading time reduction).

### [Grouping Bigquery Tables Automatically](https://github.com/fumioq/gcp-projects/tree/main/create_grouped)
With more than 15 customers' data do Manage, I was struggling to keep all dashboards's tables up to date.
It was always missing a table from one source or there was no strandard do name all columns.
So I coded this script to check if there are new tables to be agreggated and, if so, the SQL code are updated to extract data from this new sources.
 - Benefits: Views always ***up to date***, standard columns and tables, time reduction from ***50% to 95%***.

### [Creating Views, External Tables backups](https://github.com/fumioq/gcp-projects/tree/main/create_backup/scripts) and [recovering them](https://github.com/fumioq/gcp-projects/tree/main/recover_backup/scripts)
Once the delete operations of Views and External Tables finishes, it's difficult to recover its parameters. Recovering Views' queries is possible through Cloud Logging, but it's time consuming and you may not recover the most recent version, but recovering external tables' parameters is not possible.
This code is going to help you in case of recovery of a deleted views and external tables.
We had to use it once in the agency I'm working and we did recover all views and external tables in less than 30 minutes. Against the 80 hours of work spent in recovering and checking them mannually (yes, we had to mannually recovery everything once before).
 - Benefits: Reliability of your Data Base, up to ***99% of time reduction*** in case of dataset recovery and ***90% of time reduction*** in case of recovery of a single table or view.

## Last words
Feel free to contact me through my [Linkedin page](https://www.linkedin.com/in/leandro-fumio-kino-17358191/).

Thank you!
