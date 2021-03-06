######################################################
# Getting started with tweepy and twitter streaming
######################################################

# Import tweepy libraries
import json
import tweepy
import twitter_credentials # python file you store your twitter credentials (in the same directory)
import pandas as pd
import re


def decode_hashtags(hashtags):
    if hashtags:
        return ', '.join(list(map(lambda x: x["text"], hashtags)))
    else:
        return ""

# Define how you are going to parse the JSON response from twitter API in the on_data function below
class TweetStreamListener(tweepy.Stream):
    """
    Notes: the current implementation of tweepy uses the twitter API v1.1
    """

    # on success
    def on_data(self, data):
        # decode json
        # load the tweet object into json format
        dict_data = json.loads(data)

        # for test only
        # print("take a look at the raw data")
        # print(dict_data)

        # A test to collect the data fields you want

        if "extended_tweet" in dict_data.keys():
            # print(dict_data["created_at"],  dict_data["extended_tweet"]["full_text"])
            # TODO: rewrite it into a function to process the tweet text
            tweet_text = dict_data["extended_tweet"]['full_text'].replace('\n', ' ').replace('\r', ' ')
            hashtags = dict_data["extended_tweet"]["entities"]["hashtags"]
        elif "text" in dict_data.keys():
            tweet_text = dict_data["text"].replace('\n', ' ').replace('\r', ' ')
            hashtags = dict_data["entities"]["hashtags"]
        else:
            return

        retweet = False

        # special handle to retweet
        if "retweeted_status" in dict_data.keys():

            if "extended_tweet" in dict_data["retweeted_status"]:
                # print("this is a retweet and there is 'extended_tweet' file")
                tweet_text = dict_data["retweeted_status"]["extended_tweet"]['full_text'].replace('\n', ' ').replace('\r', ' ')
                hashtags = dict_data["retweeted_status"]["extended_tweet"]["entities"]["hashtags"]
            elif "text" in dict_data["retweeted_status"]:
                tweet_text = dict_data["retweeted_status"]["text"].replace('\n', ' ').replace('\r', ' ')
                hashtags = dict_data["retweeted_status"]["entities"]["hashtags"]
            else:
                return

            retweet = True

        # decide what information about the tweet to extract for your tweeter project
        message_lst = [tweet_text,
                       decode_hashtags(hashtags),
                       str(dict_data['created_at']),
                       str(dict_data["retweet_count"]),
                       str(dict_data["favorite_count"]),
                       str(retweet),
                       str(dict_data["truncated"]),
                       str(dict_data['id']),
                       str(dict_data['user']['name']),
                       str(dict_data['user']['screen_name']),
                       str(dict_data['user']['followers_count']),
                       str(dict_data['user']['location']),
                       str(dict_data['geo']),
                       '\n'
                       ]

        message = "\t".join(message_lst)

        with open("../twitter_data/twitter_data_fetched.txt", 'a+') as tf:
            tf.write(message)
            print(message)

        return True

    # on failure
    def on_error(self, status):
        print(status)


def load_to_csv(fName):
    cols_name = ["tweet", "hashtags", "date", "retweet_count", "favorite_count",
                 "is_retweet", "truncated",
                 "user_id", "user_name", "user_screen_name", "user_followers_count",
                 "user_loc", "geo"]

    df = pd.read_csv(fName, comment=cols_name, sep="\t", header=None)

    return df


# Provide the filtering criteria below and start the stream listener
if __name__ == '__main__':
    # set twitter keys/tokens
    # secrets stored in credential file twitter_credentials.py with
    # consumer_key, consumer_secret, access_token, access_token_secret
    # create instance of the tweepy stream
    stream = TweetStreamListener(
        twitter_credentials.consumer_key, twitter_credentials.consumer_secret,
        twitter_credentials.access_token, twitter_credentials.access_token_secret
    )
    # tweet_mode = "extended"
    # search twitter for "tweet" keyword
    stream.filter(track=["#AI", "#MachineLearning"], languages=["en"]) # language = "English"
