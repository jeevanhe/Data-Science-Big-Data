package TwitterSenti.TwitterSenti;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
public class TwitterTweetDownload 
{
	 public static void main( String[] args ) throws TwitterException, IOException
	    {
	        
	    	Twitter twitter = TwitterFactory.getSingleton();
	        twitter.setOAuthConsumer("PNkzn9CYX4mCZrDDetab3dZsD", "r3zJ1h4lCpmSGjhsK5SCdaPzJYXLxi68PvFt44VZ0vAbRDY4ZN");
	        //Customer Key and Customer Secret
	        RequestToken requestToken = twitter.getOAuthRequestToken();
	        AccessToken accessToken = null;
	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        while (null == accessToken) {
	          System.out.println("URL to get Twitter app access");
	          System.out.println(requestToken.getAuthorizationURL());
	          System.out.print("Enter authenication code to get tweets:");
	          String code = br.readLine();
	          try{
	             if(code.length() > 0){
	               accessToken = twitter.getOAuthAccessToken(requestToken, code);
	             }else{
	               accessToken = twitter.getOAuthAccessToken();
	             }
	          } catch (TwitterException tweetExp) {
	            if(401 == tweetExp.getStatusCode()){
	              System.out.println("Unable to get the access twiiter token.");
	            }else{
	            	tweetExp.printStackTrace();
	            }
	          }
	        }
	        Query query = new Query("newyear");
	        query.setSince("01/04/2016");
	        query.count(100);
	        QueryResult result = twitter.search(query);  
	        FileWriter fw = new FileWriter("tweetfile6.txt");
	        BufferedWriter buff = new BufferedWriter(fw);
	        for (twitter4j.Status status : result.getTweets()) {
	        	buff.write(status.getText());
	        	System.out.println("Displaying fetched tweets");
	            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
	        }
	        buff.close();
	    }
}
