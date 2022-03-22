# Twitter_Crawler
Easy-to-use tool for mining tweets with Twitter API. Support both Twitter API version 1 and academic API.

It will collect tweets based on your need, and store the result either in a json file (supported only for academic API) or in a mongo database (supported for both academic API and Twitter API version 1)

*Author*: hongbofang

*Contact*: fanghongdoublebo@gmail.com

# Get started
 - Download tw_crawler_release.py to your local machine
 - Import Twitter_Crawler_Version2 (for Twitter academic API ) or Twitter_Crawler (for Twitter API version 1) in your python script


# Examples

 - Task 1: Collect all tweets posted by a given users with Twitter Academic API, and save it in a json file.
    ```
    from tw_crawler_release import Twitter_Crawler_Version2
    
    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN')
    tw_crawler.crawl_tweets_from_user(['1169594598760562689'], # list of users to collect tweets from, user specified by their user id (in str) 
                                          './save_result.json', # the json file directory to which you want to save your result
                                          save_format = 'json' # indicate it's stored in json file
                                          )
    ```
    The result is stored as a list of tweets collected.
  - Task 2: Collect all tweets contain given keywords with Twitter Academic API, and save it in a json file.
    ```
    from tw_crawler_release import Twitter_Crawler_Version2
    
    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN')
    tw_crawler.crawl_tweets_contain_keyword(['Hello World'], # list of keywords as query 
                                          './save_result.json', # the json file directory to which you want to save your result
                                          save_format = 'json' # indicate it's stored in json file
                                          )
    ```
    The result is stored as a list of tweets collected.
# Advanced features
 - Specify the post date range of tweet collected with academic API.
   ```
    tw_crawler.crawl_tweets_from_user(['1169594598760562689'], # list of users to collect tweets from, user specified by their user id (in str) 
                                          './save_result.json', # the json file directory to which you want to save your result; the result is stored as a list of tweets collected 
                                          save_format = 'json', # indicate it's stored in json file
                                          start = '2006-03-21T00:00:00Z', 
                                          end = '2008-03-21T00:00:00Z', 
                                          )
   ```
   The parameter **start** or **end** is a string of time, and it has be in the format of "yyyy-mm-ddThh-mm-ssZ"

 - Specify the kind of information to be retrieved for collected tweets
   ```
    tw_crawler.crawl_tweets_from_user(['1169594598760562689'], # list of users to collect tweets from, user specified by their user id (in str) 
                                          './save_result.json', # the json file directory to which you want to save your result; the result is stored as a list of tweets collected 
                                          save_format = 'json', # indicate it's stored in json file
                                          field_list = 'author_id,public_metrics,in_reply_to_user_id,created_at,entities,referenced_tweets,geo,lang' 
                                          )
   ```
   The parameter **field_list** is a string of field seperated by comma, the current one shown is the default. See the complete list of fields [here](https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet).
   
 - Save the collected tweets into a mongo database instead of a json file
 ```
    import pymongo
    from tw_crawler_release import Twitter_Crawler_Version2
    
    
    client = pymongo.MongoClient(host='localhost', username = 'YOUR MONGO DB USERNAME', 
        password = 'YOUR MONGO DB PASSWORD', authSource = 'Authorized database', port=27017)
    db = client.twitter # a selected database (for example, mine is twitter)
    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN', db)
    
    tw_crawler.crawl_tweets_from_user(['1169594598760562689'], # list of users to collect tweets from, user specified by their user id (in str) 
                                          'mongocollection', # mongo collection to store the collected tweets
                                          save_format = 'mongo' # indicate it's stored in mongo file
                                          )
 ```
