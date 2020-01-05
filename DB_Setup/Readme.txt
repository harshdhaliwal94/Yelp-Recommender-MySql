Running 'create.sql' in mysql prompt will create tables and stored procedures needed. 
However Yelp database must be created before running it.
 
Python file 'populate_DB.py' loads business.json, review.json and user.json into corresponding tables in DB. 
Edit JSON file address and run 'python populate_DB.py' in terminal to load data into tables.

Requirements
On linux machine (tested on Unbuntu 16.04)
Install Python 3 and pip (python package manager)
Install required packages by running the following command in the current directory: 'pip install -r requirements.txt'

Input 
DB username, DB password, host (e.g. localhost), database (e.g. Yelp)

Needed Json Files:
user.json
business.json
review.json




