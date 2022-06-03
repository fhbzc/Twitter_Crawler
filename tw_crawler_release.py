import progressbar
import time
from datetime import datetime
import requests
import json

'''
reference: https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
'''
MAX_AUTO_RECONNECT_ATTEMPTS = 5
TWITTER_CRAWLING_ERROR_401 = 'Twitter error response: status code = 401' 
TWITTER_CRAWLING_ERROR_404 = 'Twitter error response: status code = 404'
TWITTER_CRAWLING_ERROR_500 = 'Twitter error response: status code = 500'


TWEET_STARTING_TIME = '2006-03-21T00:00:00Z' # the time when Twitter started
SUPPORTED_SAVEFORMAT = set(['mongo', 'json'])
TW_API2_DEFAULT_FIELD = 'author_id,public_metrics,in_reply_to_user_id,created_at,entities,referenced_tweets,geo,lang' 


TYPE_STRING = type('1')
TYPE_LIST = type([])

class Twitter_Crawler_Version2():

    def __init__(self,
                bearer_token,
                mongo_db = None):

        # initiate with your twitter api bearer_token (token as a string)
        # get your bearer_token at https://developer.twitter.com/en/products/twitter-api/academic-research
        self.headers = {"Authorization": "Bearer {}".format(bearer_token)}
        self.mongo_db = mongo_db



    def _crawl_tweets_given_id(self, 
                                tweet_id_str, 
                                field_list):
        url = "https://api.twitter.com/2/tweets/%s"%tweet_id_str
        params = {'tweet.fields': field_list}
        response = requests.request("GET", url, headers=self.headers, params =  params)
        return response


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


    def _crawl_tweets_search(self,
                      function,
                      data_list,
                      save_result_directory,
                      save_format = 'json',
                      end_time = None,
                      start_time = TWEET_STARTING_TIME,
                      limit = -1,
                      field_list = TW_API2_DEFAULT_FIELD,
                      verbose = True,
                      save_crawled_keyword_dierctory_json = None,
                      save_crawled_keyword_every = 100,
                      crawled_keyword_list = []):

        crawled_keyword_set = set(crawled_keyword_list)
        if type(start_time) == TYPE_LIST:
            list_start_time_flag = True
            assert len(start_time) == len(data_list), 'Length of start_time must be the same as the length of input data'
            for start_time_item in start_time:
                assert type(start_time_item) == TYPE_STRING and len(start_time_item) == 20, 'start_time_item error'
        elif type(start_time) == TYPE_STRING:
            list_start_time_flag = False
        else:
            assert False, 'start_time input error'



        if type(end_time) == TYPE_LIST:
            list_end_time_flag = True
            assert len(end_time) == len(data_list), 'Length of end_time must be the same as the length of input data'
            for end_time_item in end_time:
                assert type(end_time_item) == TYPE_STRING and len(end_time_item) == 20, 'end_time_item error'
                
        elif type(end_time) == TYPE_STRING:
            list_end_time_flag = False
        elif end_time is not None:
            assert False, 'end_time input error'
        assert save_crawled_keyword_every > 0, 'save_crawled_keyword_every should be a positive integer'


        if save_format == 'json':
            try:
              with open(save_result_directory, 'r') as f:
                crawled_tweet_list = json.load(f)

            except:
                crawled_tweet_list = []

            crawled_tweet_id_str_set = set()
            for tweet_info in crawled_tweet_list:
                tweet_id_str = str(tweet_info['id'])
                crawled_tweet_id_str_set.add(tweet_id_str)

        elif save_format == 'mongo':
            collection = self.mongo_db[save_result_directory]
            crawled_tweet_id_str_set = set()
            for tweet_info in collection.find():
                tweet_id_str = str(tweet_info['id'])
                crawled_tweet_id_str_set.add(tweet_id_str)
        else:
            print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)
            assert False

        range_ = range(len(data_list))
        if verbose == True:
            print("size of crawled tweet set", len(crawled_tweet_id_str_set))
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        error_data_set = set()
        list_of_tweet_info = []
        for data_index in range_:
            data = data_list[data_index]
            if data in crawled_keyword_set:
                continue

            valid_tweets_id_set = set()
            reach_limit_flag = False
            next_token = None
            error_flag = False
            while True:
                try:
                    
                    end_time_item = end_time[data_index] if list_end_time_flag == True else end_time
                    start_time_item = start_time[data_index] if list_start_time_flag == True else start_time
                    if start_time_item < TWEET_STARTING_TIME:
                        start_time_item = TWEET_STARTING_TIME
                    results = function(data, end_time_item, field_list, start_time_item, next_token)
                    if results.text.strip().replace('/n', '').replace('/r', '') == 'Rate limit exceeded':
                        print("rate limit exceed")
                        time.sleep(60)
                        continue
                    results = json.loads(results.text)

                    if 'title' in results:
                        if results['title'] == 'Service Unavailable':
                            error_data_set.add(data)
                            error_flag = True
                            break
                        if results['title'] == 'Too Many Requests':
                            time.sleep(1)
                            continue
                        if results['title'] == 'UsageCapExceeded':
                            time.sleep(60)
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
                        tweet['crawl_range_starttime_str'] = start_time_item if start_time_item is not None else 'NULL'
                        tweet['crawl_range_endtime_str'] = end_time_item if end_time_item is not None else 'NULL'
                        list_of_tweet_info.append(tweet)
                        valid_tweets_id_set.add(tweet['id'])

                        if limit > 0 and len(valid_tweets_id_set) >= limit:
                            reach_limit_flag = True
                            break
                    if reach_limit_flag == True:
                        break

                    # print(results['meta'])
                    # assert False
                    if 'next_token' not in results['meta']:
                        break


                    next_token = results['meta']['next_token']

                except:
                    print("data", data)
                    print("results", results)
                    print("results.text", results.text)

                    assert False
            if error_flag == False:
                crawled_keyword_set.add(data)
            if (data_index % save_crawled_keyword_every == 0):
                if save_format == 'json':
                    crawled_tweet_list.extend(list_of_tweet_info)
                    with open(save_result_directory, 'w') as f:
                        json.dump(crawled_tweet_list, f)
                    list_of_tweet_info = []
                    if save_crawled_keyword_dierctory_json is not None:
                        with open(save_crawled_keyword_dierctory_json, 'w') as f:
                            json.dump(list(crawled_keyword_set), f)

                elif save_format == 'mongo':
                    if len(list_of_tweet_info) > 0:
                        collection.insert_many(list_of_tweet_info)
                    list_of_tweet_info = []
                    if save_crawled_keyword_dierctory_json is not None:
                        with open(save_crawled_keyword_dierctory_json, 'w') as f:
                            json.dump(list(crawled_keyword_set), f)

                else:
                    assert False, 'error 260'
        if len(list_of_tweet_info) > 0:
            if save_format == 'json':
                crawled_tweet_list.extend(list_of_tweet_info)
                with open(save_result_directory, 'w') as f:
                    json.dump(crawled_tweet_list, f)
                list_of_tweet_info = []
            elif save_format == 'mongo':
                if len(list_of_tweet_info) > 0:
                    collection.insert_many(list_of_tweet_info)
                list_of_tweet_info = []
            else:
                assert False, 'error 274'


        if save_crawled_keyword_dierctory_json is not None:
            with open(save_crawled_keyword_dierctory_json, 'w') as f:
                json.dump(list(crawled_keyword_set), f)

        if verbose == True:
            p.finish()
        return error_data_set


    def _crawl_tweets_byid(self,
                      function,
                      data_list,
                      save_result_directory,
                      save_format = 'json',
                      end_time = None,
                      start_time = TWEET_STARTING_TIME,
                      limit = -1,
                      field_list = TW_API2_DEFAULT_FIELD,
                      verbose = True,
                      save_crawled_keyword_dierctory_json = None,
                      save_crawled_keyword_every = 100,
                      crawled_keyword_list = []):
        if len(start_time) != 20:
            print("start_time has to be in format yyyy-mm-ddThh:mm:ssZ. For example, a correct input would be 2006-03-21T00:00:00Z, but the current one is %s"%start_time)

        assert save_crawled_keyword_every > 0, 'save_crawled_keyword_every should be a positive integer'
        if start_time < TWEET_STARTING_TIME:
            return set()

        if save_format == 'json':
            try:
              with open(save_result_directory, 'r') as f:
                crawled_tweet_list = json.load(f)

            except:
                crawled_tweet_list = []

            crawled_tweet_id_str_set = set()
            for tweet_info in crawled_tweet_list:
                tweet_id_str = str(tweet_info['id'])
                crawled_tweet_id_str_set.add(tweet_id_str)

        elif save_format == 'mongo':
            collection = self.mongo_db[save_result_directory]
            crawled_tweet_id_str_set = set()
            for tweet_info in collection.find():
                tweet_id_str = str(tweet_info['id'])
                crawled_tweet_id_str_set.add(tweet_id_str)
        else:
            print("save format not supported, please enter one of", SUPPORTED_SAVEFORMAT)
            assert False

        range_ = range(len(data_list))
        if verbose == True:
            print("size of crawled tweet set", len(crawled_tweet_id_str_set))
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        error_data_set = set()
        list_of_tweet_info = []
        for data_index in range_:
            data = data_list[data_index]
            tweet_id_str = str(data)
            if tweet_id_str in crawled_tweet_id_str_set:
                continue
            try:
                results = function(data, field_list)

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
                    if results['title'] == 'UsageCapExceeded':
                        time.sleep(60)
                        continue
                if 'data' not in results:
                    break
                tweet = results['data']

                crawled_tweet_id_str_set.add(tweet_id_str)
                tweet['query_key'] = data
                tweet['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                tweet['crawl_range_starttime_str'] = start_time if start_time is not None else 'NULL'
                tweet['crawl_range_endtime_str'] = end_time if end_time is not None else 'NULL'
                list_of_tweet_info.append(tweet)

            except:
                print("data", data)
                print("results", results)
                assert False
            crawled_keyword_list.append(data)
            if (data_index % save_crawled_keyword_every == 0):
                if save_format == 'json':
                    crawled_tweet_list.extend(list_of_tweet_info)
                    with open(save_result_directory, 'w') as f:
                        json.dump(crawled_tweet_list, f)
                    list_of_tweet_info = []
                    if save_crawled_keyword_dierctory_json is not None:
                        with open(save_crawled_keyword_dierctory_json, 'w') as f:
                            json.dump(crawled_keyword_list, f)

                elif save_format == 'mongo':
                    
                    if len(list_of_tweet_info) > 0: collection.insert_many(list_of_tweet_info)
                    list_of_tweet_info = []
                    if save_crawled_keyword_dierctory_json is not None:
                        with open(save_crawled_keyword_dierctory_json, 'w') as f:
                            json.dump(crawled_keyword_list, f)

                else:
                    assert False, 'error 260'

        if len(list_of_tweet_info) > 0:
            if save_format == 'json':
                crawled_tweet_list.extend(list_of_tweet_info)
                with open(save_result_directory, 'w') as f:
                    json.dump(crawled_tweet_list, f)
                list_of_tweet_info = []
            elif save_format == 'mongo':
                if len(list_of_tweet_info) > 0: collection.insert_many(list_of_tweet_info)
                list_of_tweet_info = []
            else:
                assert False, 'error 274'


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
                                     limit = -1,
                                     field_list = TW_API2_DEFAULT_FIELD,
                                     verbose = True,
                                     save_crawled_keyword_dierctory_json = None,
                                     save_crawled_keyword_every = 100,
                                     crawled_keyword_list = []):
        return self._crawl_tweets_search(self._crawl_tweets_contain_keyword,
                                          keyword_list,
                                          result_save_location,
                                          save_format,
                                          end_time,
                                          start_time = start_time,
                                          limit = limit,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)


    def crawl_tweets_replyto_tweet(self,
                               tweet_id_str_list,
                               result_save_location,
                               save_format = 'mongo',
                               start_time = TWEET_STARTING_TIME,
                               end_time = None,
                               limit = -1,
                               field_list = TW_API2_DEFAULT_FIELD,
                               verbose = True,
                               save_crawled_keyword_dierctory_json = None,
                               save_crawled_keyword_every = 100,
                               crawled_keyword_list = []):
        return self._crawl_tweets_search(self._crawl_tweets_replyto_tweet,
                                        tweet_id_str_list,
                                        result_save_location,
                                        save_format,
                                        end_time,
                                        start_time = start_time,
                                        limit = limit,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)

    def crawl_tweets_from_user(self,
                               user_id_list,
                               result_save_location,
                               save_format = 'mongo',
                               start_time = TWEET_STARTING_TIME,
                               end_time = None,
                               limit = -1,
                               field_list = TW_API2_DEFAULT_FIELD,
                               verbose = True,
                               save_crawled_keyword_dierctory_json = None,
                               save_crawled_keyword_every = 100,
                               crawled_keyword_list = []):
        return self._crawl_tweets_search(self._crawl_tweets_from_user,
                                          user_id_list,
                                          result_save_location,
                                          save_format,
                                          end_time,
                                          start_time = start_time,
                                          limit = limit,
                                          field_list = field_list,
                                          verbose = verbose,
                                          save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                          save_crawled_keyword_every = save_crawled_keyword_every,
                                          crawled_keyword_list = crawled_keyword_list)

    def crawl_tweets_contain_url(self,
                                 url_list,
                                 result_save_location,
                                 save_format = 'mongo',
                                 start_time = TWEET_STARTING_TIME,
                                 end_time = None,
                                 limit = -1,
                                 field_list = TW_API2_DEFAULT_FIELD,
                                 verbose = True,
                                 save_crawled_keyword_dierctory_json = None,
                                 save_crawled_keyword_every = 100,
                                 crawled_keyword_list = []):

        return self._crawl_tweets_search(self._crawl_tweets_contain_url,
                                        url_list,
                                        result_save_location,
                                        save_format,
                                        end_time,
                                        start_time = start_time,
                                        limit = limit,
                                        field_list = field_list,
                                        verbose = verbose,
                                        save_crawled_keyword_dierctory_json = save_crawled_keyword_dierctory_json,
                                        save_crawled_keyword_every = save_crawled_keyword_every,
                                        crawled_keyword_list = crawled_keyword_list)


    def crawl_tweets_given_id(self,
                              tw_id_list,
                              result_save_location,
                              save_format = 'mongo',
                              limit = -1,
                              field_list = TW_API2_DEFAULT_FIELD,
                              verbose = True):

        return self._crawl_tweets_byid(self._crawl_tweets_given_id,
                                        tw_id_list,
                                        result_save_location,
                                        save_format,
                                        limit = limit,
                                        field_list = field_list,
                                        verbose = verbose)


class Twitter_Crawler():

    def __init__(self, 
                save_every = 100,
                mongo_host = 'localhost',
                mongo_username = None,
                mongo_password = None, 
                mongo_authsource = None,
                mongo_port = 27017):
        global tweepy
        global multiprocessing
        global pymongo
        global copy
        global math
        import tweepy
        import multiprocessing
        import pymongo
        import copy
        import math
        self.save_every = save_every

        self.mongo_host = mongo_host
        self.mongo_username = mongo_username
        self.mongo_password = mongo_password
        self.mongo_authsource = mongo_authsource
        self.mongo_port = mongo_port

    def _get_tweets_from_tids(self,input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        result_save_location = input[5]
        process_sequence = input[6]
        save_format = input[7]
        use_id_flag = input[8]
        verbose = input[9]
        if save_format == 'json':
            try:
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_sequence])
                with open(result_save_location_temp, 'r') as f:
                    collected_tweet_list = json.load(f)
            except:
                collected_tweet_list = []

        elif save_format == 'mongo':

            client = pymongo.MongoClient(host=self.mongo_host, username = self.mongo_username, 
                password = self.mongo_password, authSource = self.mongo_authsource, port=self.mongo_port)
            db = client[self.mongo_authsource]
            collection = db[result_save_location]
        else:
            assert False, 'error 793, save format string error (should not happen)'
        range_ = range(len(data))
        if process_sequence == 0 and verbose == True:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        unidentified_error_set = set()
        valid_crawled_tweet_count = 0
        full_tweet_list = []
        for row_index in range_:

            related_id = str(data[row_index])

            continue_signal = False
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
            valid_crawled_tweet_count += 1
            if len(full_tweet_list) > 0:
                if save_format == 'mongo':
                    try: 
                        collection.insert_many(full_tweet_list)
                    except pymongo.errors.AutoReconnect as py_error:
                        insert_success = False
                        for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                            try:
                                collection.insert_many(full_tweet_list)
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

                    full_tweet_list = []
                elif save_format == 'json':
                    if valid_crawled_tweet_count % self.save_every == 0:
                        collected_tweet_list.extend(full_tweet_list)
                        with open(result_save_location_temp, 'w') as f:
                            json.dump(collected_tweet_list, f)

                        full_tweet_list = []
                else:
                    assert False, 'error 585'

        if len(full_tweet_list) > 0:
            assert save_format != 'mongo', 'error 588, internal error save_format should not be mongo'

            if save_format == 'json':
                collected_tweet_list.extend(full_tweet_list)
                with open(result_save_location_temp, 'w') as f:
                    json.dump(collected_tweet_list, f)

                full_tweet_list = []
            else:
                assert False, 'error 585'
        if process_sequence == 0 and verbose == True:
            p.finish()

        return unidentified_error_set

    def _get_user_followers(self, input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        result_save_location = input[5]
        process_sequence = input[6]
        save_format = input[7]
        use_id_flag = input[8]
        verbose = input[9]
        if save_format == 'json':
            try:
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_sequence])
                with open(result_save_location_temp, 'r') as f:
                    collected_user_list = json.load(f)
            except:
                collected_user_list = []

        elif save_format == 'mongo':
            client = pymongo.MongoClient(host=self.mongo_host, username = self.mongo_username, 
                password = self.mongo_password, authSource = self.mongo_authsource, port=self.mongo_port)
            db = client[self.mongo_authsource]
            collection = db[result_save_location]
        else:
            assert False, 'error 793, save format string error (should not happen)'
        range_ = range(len(data))
        if process_sequence == 0 and verbose == True:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        unidentified_error_set = set()
        valid_crawled_uid_count = 0
        full_follower_list = []
        for row_index in range_:
            related_id = str(data[row_index])
            continue_signal = False
            
            try:
                result_json = {}
                result_json['followerid_str_list'] = []
                result_json['focaluid_str'] = str(related_id)
                if use_id_flag == True:
                    iterator = tweepy.Cursor(api.followers_ids, user_id = related_id, count = 10000).pages()
                else:
                    iterator = tweepy.Cursor(api.followers_ids, screen_name = related_id, count = 10000).pages()
                for followers in iterator: 
                    for follower in followers:
                        result_json['followerid_str_list'].append(str(follower))
                        if save_format == 'mongo' and len(result_json['followerid_str_list']) > 300000:
                            # in case it's too long
                            result_json['crawled_time_str'] = datetime.utcnow().isoformat() + 'Z'
                            result_json['followerid_str_list'] = list(set(result_json['followerid_str_list']))
                            insert_result = [result_json]
                            try: 
                                collection.insert_many(insert_result)
                            except pymongo.errors.AutoReconnect as py_error:
                                insert_success = False
                                for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                                    try:
                                        collection.insert_many(insert_result)
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
                full_follower_list.append(result_json)

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

            valid_crawled_uid_count += 1
            if len(full_follower_list) > 0:
                if save_format == 'mongo':
                    try: 
                        collection.insert_many(full_follower_list)
                    except pymongo.errors.AutoReconnect as py_error:
                        insert_success = False
                        for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                            try:
                                collection.insert_many(full_follower_list)
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
                    full_follower_list = []
                elif save_format == 'json':
                    if valid_crawled_uid_count % self.save_every == 0:
                        collected_user_list.extend(full_follower_list)
                        with open(result_save_location_temp, 'w') as f:
                            json.dump(collected_user_list, f)

                        full_follower_list = []
                else:
                    assert False, 'error 702'

        if len(full_follower_list) > 0:
            if save_format == 'mongo':
                try: 
                    collection.insert_many(full_follower_list)
                except pymongo.errors.AutoReconnect as py_error:
                    insert_success = False
                    for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                        try:
                            collection.insert_many(full_follower_list)
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
                full_follower_list = []
            elif save_format == 'json':
                collected_user_list.extend(full_follower_list)
                with open(result_save_location_temp, 'w') as f:
                    json.dump(collected_user_list, f)

                full_follower_list = []
            else:
                assert False, 'error 733'
        if process_sequence == 0 and verbose == True:
            p.finish()

        return unidentified_error_set

    def _get_user_tweets(self,input):


        auth = tweepy.OAuthHandler(input[0], input[1])
        auth.set_access_token(input[2], input[3])
        api = tweepy.API(auth, wait_on_rate_limit = True)
        assert api.verify_credentials() != False, 'credentials invalid'
        data = input[4]
        result_save_location = input[5]
        process_sequence = input[6]
        save_format = input[7]
        use_id_flag = input[8]
        verbose = input[9]
        if save_format == 'json':
            try:
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_sequence])
                with open(result_save_location_temp, 'r') as f:
                    collected_user_list = json.load(f)
            except:
                collected_user_list = []

        elif save_format == 'mongo':
            client = pymongo.MongoClient(host=self.mongo_host, username = self.mongo_username, 
                password = self.mongo_password, authSource = self.mongo_authsource, port=self.mongo_port)
            db = client[self.mongo_authsource]
            collection = db[result_save_location]
        else:
            assert False, 'error 793, save format string error (should not happen)'
        range_ = range(len(data))
        if process_sequence == 0 and verbose == True:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        full_tweet_list = []
        unidentified_error_set = set()
        valid_crawled_uid_count = 0
        for row_index in range_:

            related_id = str(data[row_index])

            continue_signal = False
            
            try:
                if use_id_flag == True: crawled_page = tweepy.Cursor(api.user_timeline, user_id = related_id, count = 100, tweet_mode = 'extended').pages()
                else: crawled_page = tweepy.Cursor(api.user_timeline, screen_name = related_id, count = 100, tweet_mode = 'extended').pages()
                for page in crawled_page: 
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

            valid_crawled_uid_count += 1
            if len(full_tweet_list) > 0:
                if save_format == 'mongo':
                    try: 
                        collection.insert_many(full_tweet_list)
                    except pymongo.errors.AutoReconnect as py_error:
                        insert_success = False
                        for reconnect_attemp_index in range(MAX_AUTO_RECONNECT_ATTEMPTS):
                            try:
                                collection.insert_many(full_tweet_list)
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

                    full_tweet_list = []
                elif save_format == 'json':
                    if valid_crawled_uid_count % self.save_every == 0:
                        collected_user_list.extend(full_tweet_list)
                        with open(result_save_location_temp, 'w') as f:
                            json.dump(collected_user_list, f)

                        full_tweet_list = []

                else:
                    assert False, 'error 807'

        if len(full_tweet_list) > 0:
            assert save_format != 'mongo', 'error 810, internal bug, save format should not be mongo'
            if save_format == 'json':
                collected_user_list.extend(full_tweet_list)
                with open(result_save_location_temp, 'w') as f:
                    json.dump(collected_user_list, f)
                full_tweet_list = []

            else:
                assert False, 'error 820'
        if process_sequence == 0 and verbose == True:
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
        result_save_location = input[5]
        process_sequence = input[6]
        save_format = input[7]
        use_id_flag = input[8]
        verbose = input[9]
        if save_format == 'json':
            try:
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_sequence])
                with open(result_save_location_temp, 'r') as f:
                    collected_user_list = json.load(f)
            except:
                collected_user_list = []

        elif save_format == 'mongo':
            client = pymongo.MongoClient(host=self.mongo_host, username = self.mongo_username, 
                password = self.mongo_password, authSource = self.mongo_authsource, port=self.mongo_port)
            db = client[self.mongo_authsource]
            collection = db[result_save_location]
        else:
            assert False, 'error 793, save format string error (should not happen)'

        range_ = range(len(data))
        if process_sequence == 0 and verbose == True:
            p = progressbar.ProgressBar()
            p.start()
            range_ = p(range_)

        result_list = []
        unidentified_user_set = set()
        for row_index in range_:
            if (len(result_list) + 1)% self.save_every == 0:
                if save_format == 'json':
                    collected_user_list.extend(result_list)
                    with open(result_save_location_temp, 'w') as f:
                        json.dump(collected_user_list, f)
                    result_list = []
                elif save_format == 'mongo':
                    if len(result_list)>0: collection.insert_many(result_list)
                    result_list = [] # empty the list
                else:
                    assert False, 'error 816'

            tweet_user = data[row_index]

            continue_signal = False
            try:
                if use_id_flag == True:
                    user = api.get_user(user_id = tweet_user)
                else:
                    user = api.get_user(screen_name = tweet_user)
            except tweepy.error.TweepError as e1:
                if e1.args[0][0]['code'] == 32:
                    print(e1.args[0][0]['message'])
                    assert False
                unidentified_user_set.add(tweet_user)
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
            if save_format == 'mongo':
                collection.insert_many(result_list)
                result_list = [] # empty the list

            elif save_format == 'json':
                collected_user_list.extend(result_list)
                with open(result_save_location_temp, 'w') as f:
                    json.dump(collected_user_list, f)
                result_list = []
        if process_sequence == 0 and verbose == True:
            p.finish()

        return unidentified_user_set


    def __pack_and_run_data_list(self,
                         data_list, 
                         tw_api_list_of_list,
                         result_save_location, 
                         multi_thread_function,
                         save_format,
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
            tw_api_list_of_list[data_batch_index].append(result_save_location)
            tw_api_list_of_list[data_batch_index].append(data_batch_index)
            tw_api_list_of_list[data_batch_index].append(save_format)
            tw_api_list_of_list[data_batch_index].append(use_id_flag)
            tw_api_list_of_list[data_batch_index].append(verbose)

        results = pool.map(multi_thread_function, tw_api_list_of_list)
        unidentified_data_set = set()
        for result in results:
            # print("unidentified_user_set_count", len(result), list(result))
            unidentified_data_set |= result

        pool.close()
        pool.join()
        return unidentified_data_set

    def get_tw_user_profile(self,
                             user_id_list, 
                             tw_api_list_of_list,
                             result_save_location, 
                             save_format = 'json',
                             use_id_flag = True,
                             verbose = True):
        assert save_format in SUPPORTED_SAVEFORMAT, 'save format unrecognized string'
        if save_format == 'json': assert result_save_location[-5:] == '.json', 'Directory must end with .json if the save format is json'


        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        unidentified_user_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,result_save_location, self._get_user_profile, save_format, use_id_flag, verbose)

        if save_format == 'json':
            collected_user_list_overall = []
            for process_index in range(len(tw_api_list_of_list_copied)):
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_index])
                try:
                    with open(result_save_location_temp, 'r') as f:
                        collected_user_list = json.load(f)
                    collected_user_list_overall.extend(collected_user_list)
                except:
                    continue
            with open(result_save_location, 'w') as f:
                json.dump(collected_user_list_overall, f)
        return unidentified_user_set



    def get_tw_user_tweets(self,
                            user_id_list, 
                            tw_api_list_of_list,
                            result_save_location, 
                            save_format = 'json',
                            use_id_flag = True,
                            verbose = True): 
        assert save_format in SUPPORTED_SAVEFORMAT, 'save format unrecognized string'
        if save_format == 'json': assert result_save_location[-5:] == '.json', 'Directory must end with .json if the save format is json'

        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        unidentified_user_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,result_save_location, self._get_user_tweets, save_format, use_id_flag, verbose)

        if save_format == 'json':
            collected_tweet_list_overall = []
            for process_index in range(len(tw_api_list_of_list_copied)):
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_index])
                try:
                    with open(result_save_location_temp, 'r') as f:
                        collected_tweet_list = json.load(f)
                    collected_tweet_list_overall.extend(collected_tweet_list)
                except:
                    continue
            with open(result_save_location, 'w') as f:
                json.dump(collected_tweet_list_overall, f)

        return unidentified_user_set


    def get_tw_user_followers(self,
                             user_id_list,
                             tw_api_list_of_list,
                             result_save_location,
                             save_format = 'json',
                             use_id_flag = True,
                             verbose = True):
        assert save_format in SUPPORTED_SAVEFORMAT, 'save format unrecognized string'
        if save_format == 'json': assert result_save_location[-5:] == '.json', 'Directory must end with .json if the save format is json'

        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        unidentified_tweet_id_set = \
            self.__pack_and_run_data_list(user_id_list,tw_api_list_of_list_copied,result_save_location, self._get_user_followers, save_format, use_id_flag, verbose)
        if save_format == 'json':
            collected_user_list_overall = []
            for process_index in range(len(tw_api_list_of_list_copied)):
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_index])
                try:
                    with open(result_save_location_temp, 'r') as f:
                        collected_user_list = json.load(f)
                    collected_user_list_overall.extend(collected_user_list)
                except:
                    continue
            with open(result_save_location, 'w') as f:
                json.dump(collected_user_list_overall, f)

        return unidentified_tweet_id_set

    def get_tw_tweets_by_tids(self,
                              tweet_id_list, 
                              tw_api_list_of_list,
                              result_save_location, 
                              save_format = 'json',
                              use_id_flag = True,
                              verbose = True):
        assert save_format in SUPPORTED_SAVEFORMAT, 'save format unrecognized string'
        if save_format == 'json': assert result_save_location[-5:] == '.json', 'Directory must end with .json if the save format is json'

        tw_api_list_of_list_copied = copy.deepcopy(tw_api_list_of_list)
        unidentified_tweet_id_set = \
            self.__pack_and_run_data_list(tweet_id_list,tw_api_list_of_list_copied,result_save_location, self._get_tweets_from_tids, save_format, use_id_flag, verbose)
        if save_format == 'json':
            collected_tweet_list_overall = []
            for process_index in range(len(tw_api_list_of_list_copied)):
                result_save_location_temp = ''.join([result_save_location[:-5], 'process%s.json'%process_index])
                try:
                    with open(result_save_location_temp, 'r') as f:
                        collected_tweet_list = json.load(f)
                    collected_tweet_list_overall.extend(collected_tweet_list)
                except:
                    continue
            with open(result_save_location, 'w') as f:
                json.dump(collected_tweet_list_overall, f)

        return unidentified_tweet_id_set

