## Dataset used: Yelp Academic Dataset
## Database created: Yelp
## Tables used: users, reviews, business

## Analysis description: 
Clustered users based on the 'latitude', 'longitude' attributes of businesses they visited in a given city along with a third attribute 'avg_star', which is the averaged rating (weighted by times of visit) using K-means clustering algorithm implemented entirely in SQL stored procedure. Based on the obtained cluster of users we recommend businesses to a user which they haven't visited (no reviews) using the top rated businesses of the cluster and the average star rating of that given user.

## Validation k-means:
To validate k-means clustering we find the shift in centroid of clusters when using subset of data for training Vs clustering entire data.
The analysis data (final_dataset table) is used to create a 70% subset for training (final_dataset_train table). Using this table we prepare the actual input dataset (kmeans_dataset2) to k-means algorithm. We run the k-means clustering first on data obtained from training set and then on entire dataset. We plot the shift in respective cluster centroids obtained from the train dataset set and full dataset.

## Validation business recommendation:
Since our final aim is to recommend business to a user based on highest rated businesses of the cluster. We validate it by predicting every users business review rating using cluster members and the user's average star rating. We average the error in prediction grouped by cluster id.

## Implementation of algorithms:
Entire data mining i.e data cleaning, train/test data preparation,kmeans clustering, validation, and business recommendation has been implemented using mysql stored procedures (see 'Procedures folder for description of each stored procedure').

## User interface:
The client code utilizes python and mysql-connector package only to connect to DB and accept basic inputs such as 'k' parameter of kmeans algorithm and calling mysql stored procedures for data cleaning, analysis etc. We can view the graphical results of clustering analysis and validation by only fetching the analyzed data from DB. Thus we have minimized the network traffic.


## Project setup for Ubuntu (Tested on Ubuntu 16.04):
Yelp database is required for the project especially the business, review and user tables. 
If the Yelp database is not setup then do the following in a linux machine else only follow steps 3, 4, 5, and 7.
1. Download yelp dataset and place the business.json, review.json and user.json files in DB_Setup folder.
2. Create a database e.g. 'Yelp' in mysql for populating the tables
3. Install python 3 and pip package manager if not already installed  
4. Navigate to 'DB_Setup' folder and type in terminal the command 'pip install -r requirements.txt' to install package dependencies
5. In mysql execute the 'create.sql' to create the tables and stored procedures.
6. Populate the database tables by typing in terminal 'python populate_DB.py' and enter the mysql username, password, host (e.g. localhost) and database name which you created earlier (e.g. Yelp) when prompted.
7. To create indexes on tables for faster analysis, execute in mysql 'create_index.sql' 


##UI Usage:
To execute the client code, naviage to 'UI' folder and run 'python ui1.py'.
1. Enter proper DB details and connect to db using UI fields.
2. After correct DB details Successsfull connection is established and Clean, Analysis buttons are enabled.
3. Select city (only 3 Major cities for analysis are provided) you want to perform analysis on and then click 'analyse' and wait for about 12 mins (if indexes are used).
4. After successful analysis, validation buttons are enabled.
5. Once 'validate' is clicked and it finishes, all buttons for viewing respective graphs are enabled.
6. Selection of 5 random users from a cluster are provided for recommending them businesses shown by clicking 'recommend'.
7. Monitor the status field shown at the bottom of the pannel for successful execution of each operation.
8. Click disconnect to close db connection before exiting.

