
Hint: You can load a "^" separated file as:
business = LOAD '/yelpdatafall/business/business.csv' using PigStorage('^') as (businessId,
fullAddress, categories);
Q1. List the business_id , full address and categories of the Top 10 highest rated
businesses using the average ratings.
This will require you to use review.csv and business.csv and join them on the common
key (business_id)
Sample output:
business id full address categories avg rating
xdf12344444444, CA 91711 List['Local Services', 'Carpet Cleaning'] 5.0
Q2. Read a user name from the command line and find the average of their review rating.
For example, if the command line argument is "Matt J", you need to output the average review
ratings of that user.
Q3. List the 'user id' and 'stars' of users that reviewed businesses located in Stanford.
You would need to filter the business.csv files by addresses that contain the word
"Stanford". There is no need for any aggregation operation.
Q4. List the user_id , and name of the top 10 users who have written the most
reviews.
Q5. List the business_id, and count of each business's ratings for the businesses that
are located in the state of TX
