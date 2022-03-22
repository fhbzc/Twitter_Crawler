# Twitter_Crawler
Easy-to-use tool for mining tweets with Twitter API. Support both Twitter API version 1 and 2 (academic API only).

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
                                          './save_result.json', # the json file directory to which you want to save your result; the result is stored as a list of tweets collected 
                                          save_format = 'json' # indicate it's stored in json file
                                          )
    ```
  - Task 2: Collect all tweets contain given keywords with Twitter Academic API, and save it in a json file.
    ```
    from tw_crawler_release import Twitter_Crawler_Version2
    
    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN')
    tw_crawler.crawl_tweets_contain_keyword(['Hello World'], # list of keywords as query 
                                          './save_result.json', # the json file directory to which you want to save your result; the result is stored as a list of tweets collected 
                                          save_format = 'json' # indicate it's stored in json file
                                          )
    ```
