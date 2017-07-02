/*Q1. List the business_id , full address and categories of the Top 10 highest rated
businesses using the average ratings.
This will require you to use review.csv and business.csv and join them on the common
key (business_id)
Please use reduce side join to answer this problem.
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
are located in the state of TX*/
//Question 1:

val data1=sc.textFile("/yelpdatafall/business/business.csv").map(line=>line.split("\\^"))
val data2=data1.map(line=>(line(0),line(1).toString+line(2).toString))
val data3=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
val sumratings=data3.map(line=>(line(2),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct
val count=data3.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b).distinct
val mergecolums=sumratings.join(count)
val review=mergecolums.map(a=>(a._1,a._2._1/a._2._2))
val res=data2.join(review).distinct.collect()
val Mergesortres=res.sortWith(_._2._2>_._2._2).take(10)
Mergesortres.foreach(line=>println(line._1,line._2._1,line._2._2))


//Question 2

val data1=sc.textFile("/yelpdatafall/user/user.csv").map(line=>line.split("\\^"))
val data2=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
val sumratings=data2.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 
val count=data2.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct 
val mergecolums=sumratings.join(count) 
val review=mergecolums.map(a=>(a._1,a._2._1/a._2._2))
val cons=Console.readLine()
val check=data1.filter(line=>line(1).contains(a)).map(line=>(line(0).toString,line(1).toString))
val userData=data1.map(line=>(line(0).toString,line(1).toString))
val res=review.join(userData)
val finalres=res.join(check)
finalres.foreach(line=>println(line._1,line._2._2._1,line._2._2._2))

//Question 3:

val data1=sc.textFile("/yelpdatafall/business/business.csv").map(line=>line.split("\\^"))
val filter=data1.filter(line=>line(1).contains(" Stanford ")).map(line=>(line(0).toString,line(1).toString))
val data3=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
val jtable = data3.map(line=>(line(2).toString,(line(1).toString,line(3).toDouble)))
val res=jtable.join(filter)
res.foreach(line=>println(line._2._1))


//Question 4:

val data1=sc.textFile("/yelpdatafall/user/user.csv").map(line=>line.split("\\^"))
val data2=data1.map(line=>(line(0),line(1).toString))
val data3=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
val count=data3.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct
val Mergeres=data2.join(count).distinct.collect()
val Mergesortres=Mergeres.sortWith(_._2._2>_._2._2).take(10)
Mergesortres.foreach(line=>println(line._1,line._2._1,line._2._2))


//Question 5:

val data1=sc.textFile("/yelpdatafall/business/business.csv").map(line=>line.split("\\^"))
val filter=data1.filter(line=>line(1).contains("TX")).map(line=>(line(0).toString,line(1).toString))
val data3=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
val count=data3.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)
val Mergeres=filter.join(count)
Mergeres.foreach(line=>println(line._1,line._2._2))


