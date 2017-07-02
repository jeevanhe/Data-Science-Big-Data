Reviews = LOAD 'review.csv' USING PigStorage('^') as (review_id:chararray, user_id:chararray, business_id:chararray, star:float);
Businesses = LOAD 'business.csv' USING PigStorage('^') as (business_id:chararray, address:chararray,categories:chararray);
Businessstar = JOIN Businesses BY business_id, Reviews BY business_id;
BusinessStanford = FILTER Businessstar BY (Businesses::address MATCHES '.* Stanford .*');
distinctBusinesses = DISTINCT BusinessStanford;
result = FOREACH distinctBusinesses GENERATE Reviews::user_id, Reviews::star;
DUMP result;
STORE result INTO 'Q3_pig';
