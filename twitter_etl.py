import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "8VFSrRLoOP3285tkhgELjPtUd" 
    access_secret = "9ZiQInoavh09VoJFR55NpYrt0MQKO2ZCguoSETvmAR87oMClpi" 
    consumer_key = "1473875020426330112-OjbaCRYvnjb6iU0zOxzfdSHXICSWdA"
    consumer_secret = "MKhqOotyEi3ZkkHc6F5wKlsWqRlpK8N77cdHNTiHXViRp"

    # Connection btw this code and Twitter via authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 
    
    # Timeline function from the documentation to get the timeline of the account given
    api = tweepy.API(auth)
    # tweets = api.get_home_timeline()
    tweets = api.user_timeline(screen_name='@narendramodi', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://yash-twitter-airflow/refined_tweets.csv')

