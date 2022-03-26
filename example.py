
from tw_crawler_release import Twitter_Crawler, Twitter_Crawler_Version2

# Version 2 Test (supports academic API)

import pymongo
client = pymongo.MongoClient(host='YOUR MONGO HOST', username = 'YOUR MONGO DB USERNAME', 
    password = 'YOUR MONGO DB PASSWORD', authSource = 'Authorized database', port=27017)
db = client.twitter # a selected database (for example, mine is twitter)
tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN',
            db)
tw_crawler.crawl_tweets_replyto_tweet([ '1507319500684898317'], 'MONGO COLLECTION TO SAVE RESULT')



# test on json version

tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN')
tw_crawler.crawl_tweets_replyto_tweet([ '1507319500684898317'], 
                                      'JSON FILE TO SAVE RESULT', 
                                      save_format = 'json')

tw_crawler.crawl_tweets_contain_keyword(['github.com/vnpy/vnpy'], 
                                      'JSON FILE TO SAVE RESULT', 
                                      save_format = 'json')


tw_crawler.crawl_tweets_from_user(['1169594598760562689'], 
                                      'JSON FILE TO SAVE RESULT', 
                                      save_format = 'json')


tw_crawler.crawl_tweets_contain_url(['github.com/vnpy/vnpy'], 
                                      'JSON FILE TO SAVE RESULT', 
                                      save_format = 'json')

tw_crawler.crawl_tweets_given_id(['1507319500684898317',], 
                                    'JSON FILE TO SAVE RESULT', 
                                    save_format = 'json')

# Version 1 Test 

api_list_of_list = (
['YOUR API KEY 1', 'YOUR API SECRET KEY 1', 'YOUR ACCESS TOKEN 1', 'YOUR ACCESS SECRET 1'],
['YOUR API KEY 2', 'YOUR API SECRET KEY 2', 'YOUR ACCESS TOKEN 2', 'YOUR ACCESS SECRET 2'],
)

tw_crawler = Twitter_Crawler(mongo_host = 'YOUR MONGO HOST',
                            mongo_username = 'YOUR MONGO USER NAME',
                            mongo_password = 'YOUR MONGO PASSWORD',
                            mongo_authsource = 'YOUR MONGO DATABASE NAME',
                            mongo_port = 27017 # your mongo connection port. For example, mine is 27017
                            )



tw_crawler.get_tw_user_profile(['1169594598760562689'], # list of users to crawl their profile
                                          api_list_of_list, # list api bundles
                                          save_format = 'mongo',
                                          result_save_location = 'MONGO COLLECTION NAME TO SAVE RESULT')

tw_crawler.get_tw_user_tweets(['1169594598760562689'], # list of users to crawl their tweets (only the recent 3,200 accessible)
                                      api_list_of_list,
                                      save_format = 'mongo',
                                      result_save_location = 'MONGO COLLECTION NAME TO SAVE RESULT')

tw_crawler.get_tw_user_followers(['1169594598760562689'], # list of users to crawl their followers
                                      api_list_of_list,
                                      save_format = 'mongo',
                                      result_save_location = 'MONGO COLLECTION NAME TO SAVE RESULT')


tw_crawler.get_tw_tweets_by_tids(['1429155949789556739'], # list of tweet ids to crawl their profile
                                          api_list_of_list,
                                          save_format = 'mongo',
                                          result_save_location = 'MONGO COLLECTION NAME TO SAVE RESULT')


tw_crawler = Twitter_Crawler()


tw_crawler.get_tw_user_profile(['1169594598760562689'],
                                          api_list_of_list,
                                          save_format = 'json',
                                          result_save_location = 'JSON FILE TO SAVE RESULT')

tw_crawler.get_tw_user_tweets(['1169594598760562689'],
                                      api_list_of_list,
                                      save_format = 'json',
                                      result_save_location = 'JSON FILE TO SAVE RESULT')

tw_crawler.get_tw_user_followers(['1169594598760562689'],
                                      api_list_of_list,
                                      save_format = 'json',
                                      result_save_location = 'JSON FILE TO SAVE RESULT')


tw_crawler.get_tw_tweets_by_tids(['1429155949789556739'],
                                          api_list_of_list,
                                          save_format = 'json',
                                          result_save_location = 'JSON FILE TO SAVE RESULT')

