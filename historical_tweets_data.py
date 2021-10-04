#! /Users/tfuku/Tools/miniconda3/bin/python3.7
#! /home/kioxpace/tools/miniconda3/envs/kioxpace/bin/python3

import sys
import os
import tweepy
import json
import subprocess
import time
import datetime
import argparse
from pytz import timezone
import sqlite3
import pandas as pd
import numpy as np
from os.path import expanduser
import yaml
import pytz



#     _________________________________
#____/ [*] Check Platform              \_________________________
#
import platform
cloud_type = str()
if "kxtweetana" in platform.uname():
    cloud_type = "gcp"
elif "Darwin" in platform.system():
    cloud_type = "mac"
else:
    cloud_type = "colab"



#     _________________________________
#____/ [*] Functions                   \_________________________
#
def get_config(plt):
    home_dir = expanduser("~")
    cfg = None
    if plt == "gcp":
        cfg = json.load(open(f"{home_dir}/.twitter/twitter.json","r"))
    elif plt == "mac":
        cfg = json.load(open(f"{home_dir}/.twitter/twitter.json","r"))
    elif plt == "colab":
        cfg = json.load(open("./drive/MyDrive/.config/twitter.json","r"))
    return cfg


def check_tweet_type(status):
    type_   =   str()
    if  "retweeted_status" in status._json.keys():
        if "quoted_status" in status.retweeted_status._json.keys():
            type_ = "retweeted_quoted"
        else :
            type_ = "retweet"
    elif "quoted_status" in status._json.keys():
        type_ = "quoted"
    elif status.in_reply_to_user_id != None:
        type_ = "reply"
    else:
        type_ = "normal"
    return type_


def distill_useful_data(status,tw_type):
    text                =   str()
    created_at          =   str()
    user_id             =   int()
    user_id_str         =   str()
    tw_id               =   int()
    tw_id_str           =   str()
    user_screen_name    =   str()
    tw_urls             =   str()
    tw_expanded_url     =   str()
    hashtags            =   str()
    tw_geo              =   str()
    tw_place            =   str()
    tw_lang             =   str()

    s_  =   None
    if tw_type == "retweet":
        s_ = status.retweeted_status

    elif tw_type == "retweeted_quoted":
        s_ = status.retweeted_status.quoted_status

    elif tw_type == "quoted":
        s_ = status.quoted_status

    elif tw_type == "reply":
        s_ = status

    elif tw_type == "normal":
        s_ = status


    if "extended_tweet" in s_._json.keys():
        text = s_.extended_tweet["full_text"]
    elif "full_text" in s_._json.keys():
        text = s_.full_text
    else:
        text = s_.text

    if s_.geo is not None:
        tw_geo              =   ",".join([str(p) for p in s_.geo["coordinates"]])

    if s_.place is not None:
        if s_.place.bounding_box is not None:
            tw_place            =   ",".join([str(j) for n in s_.place.bounding_box.coordinates[0] for j in n])

    created_at          =   s_.created_at
    user_id             =   s_.user.id
    #user_id_str         =   s_.user.id_str
    tw_id               =   s_.id
    tw_lang             =   s_.lang
    #tw_id_str           =   s_.id_str
    user_screen_name    =   s_.user.screen_name
    if len(s_.entities["urls"]) != 0:
        tw_urls             =   s_.entities["urls"][0]["url"]
        tw_expanded_url     =   s_.entities["urls"][0]["expanded_url"]
    hashtags            =   ",".join([tags["text"] for tags in s_.entities["hashtags"]])

    return text,created_at,user_id,tw_id,user_screen_name,tw_urls,tw_expanded_url,hashtags,tw_geo,tw_place,tw_lang



#     _________________________________
#____/ [*] Setup Tweepy                \_________________________
#
config  =   get_config(cloud_type)
auth    =   tweepy.OAuthHandler(config["api_key"], config["api_secret_key"])
auth.set_access_token(config["access_token"], config["access_token_secret"])
api     =   tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify=True)




#     _________________________________
#____/ [*] Aarg parse                  \_________________________
#
parser  =   argparse.ArgumentParser(description="Example ::")
parser.add_argument('-y', '--yaml', type=str, help='Specify monitor tweets words list yaml file path.', required=True)
args    =   parser.parse_args()




#     _________________________________
#____/ [*] Main                        \_________________________
#
pickup_words_set    =   yaml.safe_load(open(args.yaml))["search_words"]
#pickup_words_set    =   " OR ".join(pickup_words_set)
#for tweet in tweepy.Cursor(api.search, q=pickup_words_set, tweet_mode='extended', result_type="mixed").items():
#for tweet in tweepy.Cursor(api.search, q=pickup_words_set, tweet_mode='extended').items():


def tweet_hist_status(words=list(),status_list=list(),until=None):
    if until == None:
        until   =   datetime.date.today()

    since   =   until - datetime.timedelta(days=1)
    status  =   [s for s in tweepy.Cursor(api.search, q=words, tweet_mode='extended',include_entities=True,until=until,since=since).items()]
    status_list += status
    if len(status) != 0:
        until = until - datetime.timedelta(days=3)
        tweet_hist_status(words=words,status_list=status_list,until=until)

    return status_list

#status_list = tweet_hist_status()
status_list = list()

for word in pickup_words_set:
    word    =   [word]
    statuses = tweet_hist_status(words=word)
    status_list += statuses


df_columns           =   ["date","text","tw_type","created_at","user_id","tw_id","user_screen_name","tw_urls","tw_expanded_url","hashtags","tw_geo","tw_place","tw_lang"]

tweet_df    =   pd.DataFrame()

for tweet in status_list:
    '''
    content of tweet
    * 67 items
    ['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__getstate__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_api', '_json', 'author', 'contributors', 'coordinates', 'created_at', 'destroy', 'display_text_range', 'entities', 'extended_entities', 'favorite', 'favorite_count', 'favorited', 'full_text', 'geo', 'id', 'id_str', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status', 'lang', 'metadata', 'parse', 'parse_list', 'place', 'possibly_sensitive', 'quoted_status', 'quoted_status_id', 'quoted_status_id_str', 'retweet', 'retweet_count', 'retweeted', 'retweets', 'source', 'source_url', 'truncated', 'user']
    * 64 items
    ['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__getstate__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_api', '_json', 'author', 'contributors', 'coordinates', 'created_at', 'destroy', 'display_text_range', 'entities', 'favorite', 'favorite_count', 'favorited', 'full_text', 'geo', 'id', 'id_str', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status', 'lang', 'metadata', 'parse', 'parse_list', 'place', 'possibly_sensitive', 'quoted_status_id', 'quoted_status_id_str', 'retweet', 'retweet_count', 'retweeted', 'retweeted_status', 'retweets', 'source', 'source_url', 'truncated', 'user']
    '''

    dt = tweet.created_at.astimezone(datetime.timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    dt = dt.astimezone(jst)
    text    =   str()
    if "full_text" in dir(tweet):
        text = tweet.full_text
    else:
        text = tweet.text

    text    =   text.lower()

    user_id             =   tweet.user.id
    tw_id               =   tweet.id
    tw_lang             =   tweet.lang
    user_screen_name    =   tweet.user.screen_name
    tw_geo              =   str()
    tw_place            =   str()
    tw_urls             =   str()
    tw_type             =   str()
    tw_expanded_url     =   str()
    date                =   str()
    if tweet.geo is not None:
        tw_geo              =   ",".join([str(p) for p in tweet.geo["coordinates"]])

    if tweet.place is not None:
        if tweet.place.bounding_box is not None:
            tw_place            =   ",".join([str(j) for n in tweet.place.bounding_box.coordinates[0] for j in n])
    if len(tweet.entities["urls"]) != 0:
        tw_urls             =   tweet.entities["urls"][0]["url"]
        tw_expanded_url     =   tweet.entities["urls"][0]["expanded_url"]
    hashtags            =   ",".join([tags["text"] for tags in tweet.entities["hashtags"]])

    se = pd.Series([dt,text,tw_type,dt,user_id,tw_id,user_screen_name,tw_urls,tw_expanded_url,hashtags,tw_geo,tw_place,tw_lang],index=df_columns)
    tweet_df = tweet_df.append(se,ignore_index=True)
    ## check original tweet / retweet, etc
    #tw_type     =   check_tweet_type(tweet)
    #text,created_at,user_id,tw_id,user_screen_name,tw_urls,tw_expanded_url,hashtags,tw_geo,tw_place,tw_lang = distill_useful_data(tweet,tw_type)




conn =   sqlite3.connect("./tweets_hist.sqlite3")
tweet_df.to_sql("pickup_tweets",conn,if_exists='append')
conn.close()
