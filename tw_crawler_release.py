import progressbar
import time
from datetime import datetime
import requests
import json

MAX_AUTO_RECONNECT_ATTEMPTS = 5
TWITTER_CRAWLING_ERROR_401 = 'Twitter error response: status code = 401' 
TWITTER_CRAWLING_ERROR_404 = 'Twitter error response: status code = 404'
TWITTER_CRAWLING_ERROR_500 = 'Twitter error response: status code = 500'


TWEET_STARTING_TIME = '2006-03-21T00:00:00Z' # the time when Twitter started
SUPPORTED_SAVEFORMAT = ['mongo', 'json']
TW_API2_DEFAULT_FIELD = 'author_id,public_metrics,in_reply_to_user_id,created_at,entities,referenced_tweets,geo,lang' 

class Twitter_Crawler_Version2():

    def __init__(self,
                bearer_token,
                mongo_db = None):

        # initiate with your twitter api bearer_token (token as a string)
        # get your bearer_token at https://developer.twitter.com/en/products/twitter-api/academic-research
        self.headers = {"Authorization": "Bearer {}".format(bearer_token)}
        self.mongo_db = mongo_db

    def _crawl_tweets_replyto_tweet(self, 
                                tweet_id_str, 
                                end_time,
                                field_list,
                                start_time,
                                next_token = None):
        url = "https://api.twitter.com/2/tweets/search/all"
        query = 'conversation_id:%s'%(str(tweet_id_str))
        if end_time is not None:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'end_time': end_time,
                    'next_token': next_token}
        else:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'next_token': next_token}

        response = requests.request("GET", url, headers=self.headers, params=params)
        return response


    def _crawl_tweets_from_user(self, 
                                user_id, 
                                end_time,
                                field_list,
                                start_time,
                                next_token = None):
        url = "https://api.twitter.com/2/tweets/search/all"
        query = 'from:%s'%(str(user_id))
        if end_time is not None:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'end_time': end_time,
                    'next_token': next_token}
        else:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'next_token': next_token}
        response = requests.request("GET", url, headers=self.headers, params=params)
        return response



    def _crawl_tweets_contain_keyword(self, 
                                keyword, 
                                end_time,
                                field_list,
                                start_time,
                                next_token = None):
        url = "https://api.twitter.com/2/tweets/search/all"
        if end_time is not None:
          params = {'query': keyword, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'end_time': end_time,
                    'next_token': next_token}
        else:
          params = {'query': keyword, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'next_token': next_token}

        response = requests.request("GET", url, headers=self.headers, params=params)
        return response



    def _crawl_tweets_contain_url(self, 
                                url_input, 
                                end_time,
                                field_list,
                                start_time,
                                next_token = None):
        url = "https://api.twitter.com/2/tweets/search/all"

        query = 'url:"%s"'%url_input
        if end_time is not None:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'end_time': end_time,
                    'next_token': next_token}
        else:
          params = {'query': query, 
                    'tweet.fields': field_list,
                    'max_results': 500,
                    'start_time': start_time,
                    'next_token': next_token}
        response = requests.request("GET", url, headers=self.headers, params=params)
        return response


    def _crawl_tweets_to_json(self,
                      function,
                      data_list,
                      save_result_directory,
                      end_time,
                      start_time,
                      field_list,
                      verbose,
                      save_crawled_keyword_dierctory_json,
                      save_crawled_keyword_every,
                      crawled_keyword_list):
        if len(start_time) != 20:
            print("start_time has to be in format yyyy-mm-ddThh:mm:ssZ. For example, a correct input would be 2006-03-21T00:00:00Z, but the current one is %s"%start_time)

        assert save_crawled_keyword_every > 0, 'save_crawled_keyword_every should be a positive integer'
        if start_time < TWEET_STARTING_TIME:
            return set()

        try:
          with open(save_result_directory, 'r') as f:
            crawled_tweet_list = json.load(f)

        except:
            crawled_tweet_list = []

        crawled_tweet_id_str_set = set()
        for tweet_info in crawled_tweet_list:
            tweet_id_str = str(tweet_info['id'])
            crawled_tweet_id_str_set.add(tweet_id_str)



        range_ = range(len(data_list))
        if verbose == True:
            print("size of crawled tweet set", len(crawled_tweet_id_str_set))
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        error_data_set = set()
        for data_index in range_:
            data = data_list[data_index]
            next_token = None
            list_of_tweet_info = []
            while True:
                try:
                    results = function(data, end_time, field_list, start_time, next_token)

                    if results.text.strip().replace('/n', '').replace('/r', '') == 'Rate limit exceeded':
                        print("rate limit exceed")
                        time.sleep(60)
                        continue
                    results = json.loads(results.text)
                    if 'title' in results:
                        if results['title'] == 'Service Unavailable':
                            error_data_set.add(data)
                            break
                        if results['title'] == 'Too Many Requests':
                            time.sleep(1)
                            continue
                    if 'data' not in results and results['meta']['result_count'] == 0:
                        break
                    for tweet in results['data']:
                        tweet_id_str = str(tweet['id'])
                        if tweet_id_str in crawled_tweet_id_str_set:
                            continue

                        crawled_tweet_id_str_set.add(tweet_id_str)
                        tweet['query_key'] = data
                        tweet['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                        tweet['crawl_range_starttime_str'] = start_time if start_time is not None else 'NULL'
                        tweet['crawl_range_endtime_str'] = end_time if end_time is not None else 'NULL'
                        crawled_tweet_list.append(tweet)
                    if 'next_token' not in results['meta']:
                        break
                    next_token = results['meta']['next_token']

                except:
                    print("data", data)
                    print("results", results)
                    assert False

            crawled_keyword_list.append(data)
            if (data_index % save_crawled_keyword_every == 0):

              with open(save_result_directory, 'w') as f:
                json.dump(crawled_tweet_list, f)

              if save_crawled_keyword_dierctory_json is not None:
                  with open(save_crawled_keyword_dierctory_json, 'w') as f:
                      json.dump(crawled_keyword_list, f)


        with open(save_result_directory, 'w') as f:
          json.dump(crawled_tweet_list, f)

        if save_crawled_keyword_dierctory_json is not None:
            with open(save_crawled_keyword_dierctory_json, 'w') as f:
                json.dump(crawled_keyword_list, f)

        if verbose == True:
          p.finish()
        return error_data_set





    def _crawl_tweets_to_mongo(self,
                      function,
                      data_list,
                      mongo_collection,
                      end_time,
                      start_time,
                      field_list,
                      verbose,
                      save_crawled_keyword_dierctory_json,
                      save_crawled_keyword_every,
                      crawled_keyword_list):
        if len(start_time) != 20:
            print("start_time has to be in format yyyy-mm-ddThh:mm:ssZ. For example, a correct input would be 2006-03-21T00:00:00Z, but the current one is %s"%start_time)
        assert save_crawled_keyword_every > 0, 'save_crawled_keyword_every should be a positive integer'

        if start_time < TWEET_STARTING_TIME:
            return set()

        collection = self.mongo_db[mongo_collection]
        crawled_tweet_id_str_set = set()
        for tweet_info in collection.find():
            tweet_id_str = str(tweet_info['id'])
            crawled_tweet_id_str_set.add(tweet_id_str)
            # won't insert the same tweet twice in the same mongodb



        range_ = range(len(data_list))
        if verbose == True:
            print("size of crawled tweet set", len(crawled_tweet_id_str_set))
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        error_data_set = set()
        for data_index in range_:
            data = data_list[data_index]
            next_token = None
            list_of_tweet_info = []
            while True:
                try:
                    results = function(data, end_time, field_list, start_time, next_token)

                    if results.text.strip().replace('/n', '').replace('/r', '') == 'Rate limit exceeded':
                        print("rate limit exceed")
                        time.sleep(60)
                        continue
                    results = json.loads(results.text)
                    if 'title' in results:
                        if results['title'] == 'Service Unavailable':
                            error_data_set.add(data)
                            break
                        if results['title'] == 'Too Many Requests':
                            time.sleep(1)
                            continue
                    if 'data' not in results and results['meta']['result_count'] == 0:
                        break
                    for tweet in results['data']:
                        tweet_id_str = str(tweet['id'])
                        if tweet_id_str in crawled_tweet_id_str_set:
                            continue

                        crawled_tweet_id_str_set.add(tweet_id_str)
                        tweet['query_key'] = data
                        tweet['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                        tweet['crawl_range_starttime_str'] = start_time if start_time is not None else 'NULL'
                        tweet['crawl_range_endtime_str'] = end_time if end_time is not None else 'NULL'
                        list_of_tweet_info.append(tweet)
                    if 'next_token' not in results['meta']:
                        break
                    next_token = results['meta']['next_token']

                    if len(list_of_tweet_info) >= 500:
                        collection.insert(list_of_tweet_info)
                        list_of_tweet_info = []
                        

                except:
                    print("data", data)
                    print("results", results)
                    assert False

            crawled_keyword_list.append(data)
            if len(list_of_tweet_info) > 0:
                collection.insert(list_of_tweet_info)
                list_of_tweet_info = []
            if save_crawled_keyword_dierctory_json is not None and (data_index % save_crawled_keyword_every == 0):
                with open(save_crawled_keyword_dierctory_json, 'w') as f:
                    json.dump(crawled_keyword_list, f)

        if len(list_of_tweet_info) > 0:
            collection.insert(list_of_tweet_info)
            list_of_tweet_info = []

        if save_crawled_keyword_dierctory_json is not None:
            with open(save_crawled_keyword_dierctory_json, 'w') as f:
                json.dump(crawled_keyword_list, f)

        if verbose == True:
          p.finish()
        return error_data_set

    def crawl_tweets_contain_keyword(self,
                                     keyword_list,
                                     result_save_location,
                                     save_format = 'mongo',
                                     start_time = TWEET_STARTING_TIME,
                                     end_time = None,
                                     field_list = TW_API2_DEFAULT_FIELD,
                                     verbose = True,
                                     save_crawled_keyword_dierctory_json = None,
                                     save_crawled_keyword_every = 100,
                                     crawled_keyword_list = []):
        if save_format == 'mongo':
          return self._crawl_tweets_to_mongo(self._crawl_tweets_contain_keyword,
                                          keyword_list,
                                          result_save_location,
                                          end_time,
                                          start_time = start_time,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)
        elif save_format == 'json':
          return self._crawl_tweets_to_json(self._crawl_tweets_contain_keyword,
                                          keyword_list,
                                          result_save_location,
                                          end_time,
                                          start_time = start_time,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)
        else:

          print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)


    def crawl_tweets_replyto_tweet(self,
                               tweet_id_str_list,
                               result_save_location,
                               save_format = 'mongo',
                               start_time = TWEET_STARTING_TIME,
                               end_time = None,
                               field_list = TW_API2_DEFAULT_FIELD,
                               verbose = True,
                               save_crawled_keyword_dierctory_json = None,
                               save_crawled_keyword_every = 100,
                               crawled_keyword_list = []):
        if save_format == 'mongo':
          return self._crawl_tweets_to_mongo(self._crawl_tweets_replyto_tweet,
                                        tweet_id_str_list,
                                        result_save_location,
                                        end_time,
                                        start_time = start_time,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)
        elif save_format == 'json':

          return self._crawl_tweets_to_json(self._crawl_tweets_replyto_tweet,
                                        tweet_id_str_list,
                                        result_save_location,
                                        end_time,
                                        start_time = start_time,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)

        else:
          print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)

    def crawl_tweets_from_user(self,
                               user_id_list,
                               result_save_location,
                               save_format = 'mongo',
                               start_time = TWEET_STARTING_TIME,
                               end_time = None,
                               field_list = TW_API2_DEFAULT_FIELD,
                               verbose = True,
                               save_crawled_keyword_dierctory_json = None,
                               save_crawled_keyword_every = 100,
                               crawled_keyword_list = []):
        if save_format == 'mongo':
          return self._crawl_tweets_to_mongo(self._crawl_tweets_from_user,
                                          user_id_list,
                                          result_save_location,
                                          end_time,
                                          start_time = start_time,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)
        elif save_format == 'json':

          return self._crawl_tweets_to_json(self._crawl_tweets_from_user,
                                          user_id_list,
                                          result_save_location,
                                          end_time,
                                          start_time = start_time,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)
        else:
          print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)

    def crawl_tweets_contain_url(self,
                                 url_list,
                                 result_save_location,
                                 save_format = 'mongo',
                                 start_time = TWEET_STARTING_TIME,
                                 end_time = None,
                                 field_list = TW_API2_DEFAULT_FIELD,
                                 verbose = True,
                                 save_crawled_keyword_dierctory_json = None,
                                 save_crawled_keyword_every = 100,
                                 crawled_keyword_list = []):
        if save_format == 'mongo':

          return self._crawl_tweets_to_mongo(self._crawl_tweets_contain_url,
                                        url_list,
                                        result_save_location,
                                        end_time,
                                        start_time = start_time,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)
        elif save_format == 'json':
          return self._crawl_tweets_to_json(self._crawl_tweets_contain_url,
                                        url_list,
                                        result_save_location,
                                        end_time,
                                        start_time = start_time,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)
        else:
          print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)

class Twitter_Crawler():

    def __init__(self, mongodb, insert_user_every = 100):
        import tweepy
        import multiprocessing
        import pymongo
        import copy
        import math
        self.mongodb = mongodb
        self.insert_user_every = insert_user_every
    def _get_tweets_from_tids(self,input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]
        collection = self.mongodb[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_error_set = set()
        for row_index in range_:

            related_id = str(data[row_index])

            continue_signal = False
            full_tweet_list = []
            try:
                tweet = api.get_status(id = int(related_id),tweet_mode = 'extended')
                tweet_json = tweet._json
                tweet_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                full_tweet_list.append(tweet_json)
            except tweepy.TweepError:
                unidentified_error_set.add(related_id)
                continue_signal = True
            except Exception as e2:
                print("unexpected error", e2)
                assert False
            if continue_signal == True:
                continue
            if len(full_tweet_list) > 0:
                try: 
                    collection.insert(full_tweet_list)
                except pymongo.errors.AutoReconnect as py_error:
                    insert_success = False
                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                        try:
                            collection.insert(full_tweet_list)
                            insert_success = True
                            break
                        except pymongo.errors.AutoReconnect as py_error:
                            wait_t = 0.5 * pow(2, reconnect_attemp_index)
                            time.sleep(wait_t)
                    if insert_success == False:
                        unidentified_error_set.add(related_id)
                except Exception as unknown_error:
                    print("unknown error", unknown_error)
                    print("related_id", related_id)
                    assert False

        if process_sequence == 0:
            p.finish()

        return unidentified_error_set

    def _get_user_followers(self, input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]
        use_id_flag = input[7]
        collection = self.mongodb[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_error_set = set()
        for row_index in range_:
            related_id = str(data[row_index])
            continue_signal = False
            full_tweet_list = []
            try:
                if use_id_flag == True:
                    result_json = {}
                    result_json['followerid_str_list'] = []
                    result_json['focaluid_str'] = str(related_id)
                    for followers in tweepy.Cursor(api.followers_ids, user_id = related_id, count = 10000).pages(): 
                        for follower in followers:
                            result_json['followerid_str_list'].append(str(follower))

                            if len(result_json['followerid_str_list']) > 300000:
                                # in case it's too long
                                result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                                result_json['followerid_str_list'] = list(set(result_json['followerid_str_list']))
                                insert_result = [result_json]
                                try: 
                                    collection.insert(insert_result)
                                except pymongo.errors.AutoReconnect as py_error:
                                    insert_success = False
                                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                                        try:
                                            collection.insert(insert_result)
                                            insert_success = True
                                            break
                                        except pymongo.errors.AutoReconnect as py_error:
                                            wait_t = 0.5 * pow(2, reconnect_attemp_index)
                                            time.sleep(wait_t)
                                    if insert_success == False:
                                        unidentified_error_set.add(related_id)
                                except Exception as unknown_error:
                                    print("unknown error", unknown_error)
                                    print("related_id", related_id)
                                    assert False

                                result_json = {}
                                result_json['followerid_str_list'] = []
                                result_json['focaluid_str'] = str(related_id)


                    assert 'crawling_time_str' not in result_json
                    result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                    result_json['followerid_str_list'] = list(set(result_json['followerid_str_list']))
                    full_tweet_list.append(result_json)
                else:
                    assert False,'Has not been tested'

            except tweepy.error.TweepError as e1:
                if 'Not authorized' in str(e1):
                    # the user is protected
                    unidentified_error_set.add(related_id)
                    continue_signal = True
                
                elif e1.args[0][0]['code'] == 32:
                    # the API is not valid
                    print(e1.args[0][0]['message'])
                    assert False
                elif e1.args[0][0]['code'] == 34:
                    # the user doesn't exist
                    unidentified_error_set.add(related_id)
                    continue_signal = True
                else:
                    print(e1, related_id)
                    assert False
            except Exception as e2:
                print("unexpected error", e2)
                assert False
            if continue_signal == True:
                continue
            if len(full_tweet_list) > 0:
                try: 
                    collection.insert(full_tweet_list)
                except pymongo.errors.AutoReconnect as py_error:
                    insert_success = False
                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                        try:
                            collection.insert(full_tweet_list)
                            insert_success = True
                            break
                        except pymongo.errors.AutoReconnect as py_error:
                            wait_t = 0.5 * pow(2, reconnect_attemp_index)
                            time.sleep(wait_t)
                    if insert_success == False:
                        unidentified_error_set.add(related_id)
                except Exception as unknown_error:
                    print("unknown error", unknown_error)
                    print("related_id", related_id)
                    assert False

        if process_sequence == 0:
            p.finish()

        return unidentified_error_set

    def _get_user_tweets(self,input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]
        use_id_flag = input[7]
        collection = self.mongodb[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_error_set = set()
        for row_index in range_:

            related_id = str(data[row_index])

            continue_signal = False
            full_tweet_list = []
            try:
                if use_id_flag == True:
                    for page in tweepy.Cursor(api.user_timeline, user_id = related_id, count = 100, tweet_mode = 'extended').pages(): 
                        for status in page:
                            result_json = status._json
                            assert 'crawling_time_str' not in result_json
                            result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                            full_tweet_list.append(result_json)
                else:
                    assert use_id_flag == False
                    for page in tweepy.Cursor(api.user_timeline, screen_name = related_id, count = 100, tweet_mode = 'extended').pages(): 
                        for status in page:
                            result_json = status._json
                            assert 'crawling_time_str' not in result_json
                            result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                            full_tweet_list.append(result_json)
            except tweepy.error.TweepError as e1:
                str_e1 = str(e1)
                if TWITTER_CRAWLING_ERROR_401 in str_e1:
                    # protected tweets
                    pass
                elif TWITTER_CRAWLING_ERROR_404 in str_e1:
                    # unidentified user
                    pass
                elif TWITTER_CRAWLING_ERROR_500:
                    # internal server issue
                    pass
                elif e1.args[0][0]['code'] == 32:
                    # api key error
                    print(e1.args[0][0]['message'])
                    assert False
                unidentified_error_set.add(related_id)
                continue_signal = True
            except Exception as e2:
                print("unexpected error", e2)
                assert False
            if continue_signal == True:
                continue
            if len(full_tweet_list) > 0:
                try: 
                    collection.insert(full_tweet_list)
                except pymongo.errors.AutoReconnect as py_error:
                    insert_success = False
                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                        try:
                            collection.insert(full_tweet_list)
                            insert_success = True
                            break
                        except pymongo.errors.AutoReconnect as py_error:
                            wait_t = 0.5 * pow(2, reconnect_attemp_index)
                            time.sleep(wait_t)
                    if insert_success == False:
                        unidentified_error_set.add(related_id)
                except Exception as unknown_error:
                    print("unknown error", unknown_error)
                    print("related_id", related_id)
                    assert False

        if process_sequence == 0:
            p.finish()

        return unidentified_error_set


    def _get_user_profile(self,input):
        # input is a tuple of following information:
        # (key(4 items), data(1 item), process_index (1 item))

        
        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        insert_collection_name = input[5]
        process_sequence = input[6]

        collection = self.mongodb[insert_collection_name]
        range_ = range(len(data))
        if process_sequence == 0:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_user_id_int_set = set()
        for row_index in range_:
            if (len(result_list) + 1)% self.insert_user_every == 0:
                collection.insert(result_list)
                result_list = [] # empty the list

            tweet_user_id_int = data[row_index]

            continue_signal = False
            try:
                user = api.get_user(user_id = tweet_user_id_int)
            except tweepy.error.TweepError as e1:
                if e1.args[0][0]['code'] == 32:
                    print(e1.args[0][0]['message'])
                    assert False
                unidentified_user_id_int_set.add(tweet_user_id_int)
                continue_signal = True
            except Exception as e2:
                assert False
            if continue_signal == True:
                continue
            result_json = user.__dict__['_json']
            assert 'crawling_time_str' not in result_json
            result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
            result_list.append(result_json)

        if len(result_list) > 0:
            collection.insert(result_list)
            result_list = [] # empty the list

        if process_sequence == 0:
            p.finish()

        return unidentified_user_id_int_set


    def __pack_and_run_data_list(self,
                         data_list, 
                         tw_api_list_of_list,
                         insert_collection_name, 
                         multi_thread_function,
                         use_id_flag,
                         verbose = True):
        total_len_data = len(data_list)
        total_process_count = len(tw_api_list_of_list)
        batch_size = int(math.ceil(total_len_data * 1.0 / total_process_count))
        pool = multiprocessing.Pool(total_process_count) 

        
        if verbose == True:
            print("total_process_count",total_process_count)
            print("total data to be crawled", total_len_data)
        split_data = [[] for _ in range(total_process_count)]
        for data_batch_index in range(total_process_count):
            for data_index in range(batch_size*data_batch_index, batch_size*(data_batch_index + 1)):
                if data_index < total_len_data:
                    split_data[data_batch_index].append(data_list[data_index])

        for data_batch_index in range(total_process_count):
            tw_api_list_of_list[data_batch_index].append(split_data[data_batch_index])
            tw_api_list_of_list[data_batch_index].append(insert_collection_name)
            tw_api_list_of_list[data_batch_index].append(data_batch_index)
            tw_api_list_of_list[data_batch_index].append(use_id_flag)

        results = pool.map(multi_thread_function, tw_api_list_of_list)
        unidentified_data_set = set()
        for result in results:
            # print("unidentified_user_set_count", len(result), list(result))
            unidentified_data_set |= result

        pool.close()
        pool.join()
        return unidentified_data_set

    def get_tw_user_profile_to_mongo(self,
                                     user_id_list, 
                                     tw_api_list_of_list,
                                     insert_collection_name, 
                                     use_id_flag = True,
                                     verbose = True,
                                     test_mode = False):

        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        if test_mode == True:
            user_id_list = user_id_list[:10]
        unidentified_user_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,insert_collection_name, self._get_user_profile, use_id_flag, verbose)

        return unidentified_user_set



    def get_tw_user_tweets_to_mongo(self,
                                    user_id_list, 
                                    tw_api_list_of_list,
                                    insert_collection_name, 
                                    use_id_flag = True,
                                    verbose = True, 
                                    test_mode = False): # test only crawls one sample
        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        if test_mode == True:
            user_id_list = user_id_list[:10]
        unidentified_user_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,insert_collection_name, self._get_user_tweets, use_id_flag, verbose)

        return unidentified_user_set


    def get_tw_user_followers_tomongo(self,
                                     user_id_list,
                                     tw_api_list_of_list,
                                     insert_collection_name,
                                     use_id_flag = True,
                                     verbose = True,
                                     test_mode = False):
        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        if test_mode == True:
            user_id_list = user_id_list[:10]
        unidentified_tweet_id_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,insert_collection_name, self._get_user_followers, use_id_flag, verbose)

        return unidentified_tweet_id_set

    def get_tw_tweets_by_tids_to_mongo(self,
                                    tweet_id_list, 
                                    tw_api_list_of_list,
                                    insert_collection_name, 
                                    use_id_flag = True,
                                    verbose = True, 
                                    test_mode = False): # test only crawls one sample
        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        if test_mode == True:
            tweet_id_list = tweet_id_list[:10]
        unidentified_tweet_id_set = \
            self.__pack_and_run_data_list(tweet_id_list,tw_api_list_of_list_copied,insert_collection_name, self._get_tweets_from_tids, use_id_flag, verbose)

        return unidentified_tweet_id_set



# # Version 2 Test (supports academic API)
if __name__ == "__main__":

    # test on mongo version
    import pymongo
    client = pymongo.MongoClient(host='localhost', username = 'YOUR MONGO DB USERNAME', 
        password = 'YOUR MONGO DB PASSWORD', authSource = 'Authorized database', port=27017)
    db = client.twitter # a selected database (for example, mine is twitter)
    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN',
                db)
    tw_crawler.crawl_tweets_replyto_tweet([ '1506256297943048195', '1506256297943048195'], 'MONGO COLLECTION TO SAVE RESULT')



    # test on json version

    tw_crawler = Twitter_Crawler_Version2('YOUR TWITTER API BEARER_TOKEN')
    tw_crawler.crawl_tweets_replyto_tweet([ '1506256297943048195', '1506256297943048195'], 
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

