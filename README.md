# Welcome to my GCP related projects' portfolio!
All files were made by Leandro Fumio Kino.

Here, you can read more about my personal projects and how they may help you in your routine tasks with Python and SQL in your GCP projects.

# About me
I have 2 years of experience with Data Engineer and Cloud Solutions. I've been working in a worldwide digital marketing agency, and I'm responsible for all data-related automations of all data engineers and business intelligence analysts.

# About the projects:
## [Case/When from sheets to Bigquery](https://github.com/fumioq/gcp-projects/tree/main/case_when_sheets_to_bq/scripts)
BI Analysts don't have permissions to edit and delete datasets and tables, and they're working with Google Data Studio.
I connect the tables and views from Bigquery to Data Studio, and they do an additional transform operation to correct some data mistakes through Case/When.
The problem is that Data Studio isn't a good tool to process data and 300+ lines of Case/When in a Calculated Field was making the Dashboard heavy, taking up to 2 minutes to load every graph.
So I developed this code to allow the BI Analyst to insert Case/When lines in a Google Sheets and this code inserts them on the view that is connected to Data Studio.
Benefits: reduced the loading time from 120s to 6s (95% loading time reduction).

## [Grouping Bigquery Tables Automatically](https://github.com/fumioq/gcp-projects/tree/main/create_grouped)
With more than 15 customers' data do Manage, I was struggling to keep all dashboards's tables up to date.
It was always missing a table from one source or there was no strandard do name all columns.
So I coded this script to check if there are new tables to be agreggated and, if so, the SQL code are updated to extract data from this new sources.
Benefits: Views always up to date, standard columns and tables, time reduction from 50% to 95%.
