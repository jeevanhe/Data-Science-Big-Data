Reviews = LOAD 'review.csv' USING PigStorage('^') as (review_id:chararray, user_id:chararray, business_id:chararray, star:float);
Users = LOAD 'user.csv' Using PigStorage('^') AS (user_id:chararray, name:chararray, url:chararray);
UsersGroups = GROUP Reviews BY user_id;
UsersCounts = FOREACH UsersGroups GENERATE group, AVG(Reviews.star) as countstars;
UsersDetails =JOIN Users BY user_id, UsersCounts BY group;
UserFilter = FILTER UsersDetails BY (Users::name MATCHES '.*$Firstname\\s$Lastname.*');
result = FOREACH UserFilter GENERATE Users::user_id, Users::name, UsersCounts::countstars;
DUMP result;
STORE result INTO 'Q2_pig'; 
