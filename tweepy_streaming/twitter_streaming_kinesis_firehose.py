#from tweepy.streaming import StreamListener
#from tweepy import OAuthHandler
#from tweepy import Stream
import tweepy
import json
import boto3
import time
import twitter_credentials
import sys
sys.path.append('..')
from helper.extract_tweet_info import extract_tweet_info


class TweetStreamListener(tweepy.Stream):
    # on success
    def on_data(self, data):
        """Summary

        Args:
            data (TYPE): Description

        Returns:
            TYPE: Description
        """
        tweet = json.loads(data)
        try:
            message = extract_tweet_info(tweet)
            print(message)
            # only put the record when message is not None
            if (message):
                kinesis_client.put_record(
                    StreamName=delivery_stream_name,
                    Data=json.dumps(message),
                    PartitionKey="partitionkey"
                )
        except (AttributeError, Exception) as e:
            print(e)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session(profile_name='python')

    # create the kinesis kinesis client
    kinesis_client = session.client('kinesis', region_name='us-east-1')

    # Set kinesis data stream name
    delivery_stream_name = "twitter-data-kinesis"

    while True:
        try:
            print('Twitter streaming...')

            # create instance of the tweepy stream
            stream = TweetStreamListener(
                twitter_credentials.consumer_key, twitter_credentials.consumer_secret,
                twitter_credentials.access_token, twitter_credentials.access_token_secret
            )

            # search twitter for the keyword
            stream.filter(track=["#AI", "#MachineLearning"], languages=['en'])
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
