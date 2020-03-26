# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 19:46:55 2020

@author: irene
"""
import tweepy
import pandas as pd
from tweepy.parsers import JSONParser
import json
#import json


#Twitter credentials
API_KEY = '5Hqg6JTZ0cC89hUThySd5yZcL'
API_SECRET = 'Ncp1oi5tUPbZF19Vdp8Jp8pNHBBfPdXGFtXqoKd6Cqn87xRj0c'
TOKEN_KEY = '3272304896-ZTGUZZ6QsYKtZqXAVMLaJzR8qjrPW22iiu9ko4w'
TOKEN_SECRET = 'nsNY13aPGWdm2QcgOl0qwqs5bwLBZ1iUVS2OE34QsuR4C'

auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
auth.set_access_token(TOKEN_KEY, TOKEN_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True)

#place = api.geo_search(query="Mexico", granularity="country") ## neighborhood , city , admin or country
#place_id = place[0].id

#place_id = '72ee18b0510eb501' ### Ciudad de México
place_id = '25530ba03b7d90c6' ### México Country
result_type = "mixed"
date_since = "2020-03-15"
date_until = "2020-03-23"
#keywords = "drinkup,martini,shots,cheers,happyhour,foodporn,cocktailporn,ladiesnight,gin,drinks,foodie,sushitime,cocktails,yumm,instafood,nightout,foodies,deli,delicious,instadrink,foodblogger,bartender,dinner,speakeasy,gingin,hankypanky,foodgram,cocktailculture,foodandtravel,foodiecdmx,foodiechilango,foodiegram,foodphotography,foodpic,foodpost,gintonic,hankypankycocktail,hankypankydf,ladocena,mixologia,mixologist,mixology,restaurante,restaurants,soba,wearehankypanky,winelovers,chef"
keywords = "covid"
final_tweets = [];
for query in keywords.split(","):
    searched_tweets = [status._json for status in tweepy.Cursor(api.search, q=query + '+place:%s' % place_id, result_type = result_type, since= date_since, until= date_until, lang = "es").items(1000)]
    final_tweets.append(searched_tweets)

data = pd.DataFrame(columns = ['created_at', 'id', 'id_str', 'text', 'truncated', 'source', 
                               'in_reply_to_status_id', 'in_reply_to_status_id_str', 
                               'in_reply_to_user_id', 'in_reply_to_user_id_str', 
                               'in_reply_to_screen_name', 'protected', 'followers_count', 
                               'friends_count', 'favourites_count', 'utc_offset', 'time_zone', 
                               'geo_enabled', 'verified', 'statuses_count', 'lang', 
                               'contributors_enabled', 'is_translator', 'is_translation_enabled', 
                               'profile_background_color', 'profile_background_image_url', 
                               'profile_background_image_url_https', 'profile_background_tile', 
                               'profile_image_url', 'profile_image_url_https', 
                               'profile_banner_url', 'profile_link_color', 
                               'profile_sidebar_border_color', 'profile_sidebar_fill_color', 
                               'profile_text_color', 'profile_use_background_image', 
                               'has_extended_profile', 'default_profile', 'default_profile_image', 
                               'following', 'follow_request_sent', 'notifications', 'geo', 
                               'coordinates', 'listed_count', 
                               'verified', 'contributors', 'is_quote_status', 'retweet_count', 
                               'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive',
                               'attributes', 'bounding_box', 'contained_within', 'country', 
                               'country_code', 'full_name', 'tweet_place_id', 'place_name', 
                               'place_type', 'place_url', 'user_created_at', 'description', 
                               'user_id', 'user_id_str', 'location', 'name', 'screen_name', 'hashtags'])

for searched_tweets in final_tweets[0]:
    created_at= '' 
    id= '' 
    id_str= '' 
    text= '' 
    truncated= '' 
    source= '' 
    in_reply_to_status_id= '' 
    in_reply_to_status_id_str= '' 
    in_reply_to_user_id= '' 
    in_reply_to_user_id_str= '' 
    in_reply_to_screen_name= '' 
    protected= '' 
    followers_count= '' 
    friends_count= '' 
    favourites_count= '' 
    utc_offset= '' 
    time_zone= '' 
    geo_enabled= '' 
    verified= '' 
    statuses_count= '' 
    lang= '' 
    contributors_enabled= '' 
    is_translator= '' 
    is_translation_enabled= '' 
    profile_background_color= '' 
    profile_background_image_url= '' 
    profile_background_image_url_https= '' 
    profile_background_tile= '' 
    profile_image_url= '' 
    profile_image_url_https= '' 
    profile_banner_url= '' 
    profile_link_color= '' 
    profile_sidebar_border_color= '' 
    profile_sidebar_fill_color= '' 
    profile_text_color= '' 
    profile_use_background_image= '' 
    has_extended_profile= '' 
    default_profile= '' 
    default_profile_image= '' 
    following= '' 
    follow_request_sent= '' 
    notifications= '' 
    geo= '' 
    coordinates= '' 
    listed_count= '' 
    verified= '' 
    #place= '' 
    contributors= '' 
    is_quote_status= '' 
    retweet_count= '' 
    favorite_count= '' 
    favorited= '' 
    retweeted= '' 
    possibly_sensitive= ''
    
    # PLACE ATTRIBUTES
    attributes = ''
    bounding_box = ''
    contained_within = ''
    country = ''
    country_code = ''
    full_name = ''
    tweet_place_id = ''
    place_name = ''
    place_type = ''
    place_url = ''
    
    # USER ATTRIBUTES
    user_created_at = ''
    description = ''
    user_id = ''
    user_id_str = ''
    location = ''
    name = ''
    screen_name = ''
    
    # ENTITIES ATTRIBUTES
    hashtags = ''
    
    if ('created_at' in searched_tweets):
        created_at = searched_tweets['created_at'] 
    if ('id' in searched_tweets): 
        id = searched_tweets['id'] 
    if ('id_str' in searched_tweets): 
        id_str = searched_tweets['id_str'] 
    if ('text' in searched_tweets): 
        text = searched_tweets['text'] 
    if ('truncated' in searched_tweets): 
        truncated = searched_tweets['truncated'] 
    if ('source' in searched_tweets): 
        source = searched_tweets['source'] 
    if ('in_reply_to_status_id' in searched_tweets): 
        in_reply_to_status_id = searched_tweets['in_reply_to_status_id'] 
    if ('in_reply_to_status_id_str' in searched_tweets): 
        in_reply_to_status_id_str = searched_tweets['in_reply_to_status_id_str'] 
    if ('in_reply_to_user_id' in searched_tweets): 
        in_reply_to_user_id = searched_tweets['in_reply_to_user_id'] 
    if ('in_reply_to_user_id_str' in searched_tweets): 
        in_reply_to_user_id_str = searched_tweets['in_reply_to_user_id_str'] 
    if ('in_reply_to_screen_name' in searched_tweets): 
        in_reply_to_screen_name = searched_tweets['in_reply_to_screen_name'] 
    if ('protected' in searched_tweets['user']): 
        protected = searched_tweets['user']['protected'] 
    if ('followers_count' in searched_tweets['user']): 
        followers_count = searched_tweets['user']['followers_count'] 
    if ('friends_count' in searched_tweets['user']): 
        friends_count = searched_tweets['user']['friends_count'] 
    if ('favourites_count' in searched_tweets['user']): 
        favourites_count = searched_tweets['user']['favourites_count'] 
    if ('utc_offset' in searched_tweets['user']): 
        utc_offset = searched_tweets['user']['utc_offset']
    if ('time_zone' in searched_tweets['user']): 
        time_zone = searched_tweets['user']['time_zone'] 
    if ('geo_enabled' in searched_tweets['user']): 
        geo_enabled = searched_tweets['user']['geo_enabled'] 
    if ('verified' in searched_tweets): 
        verified = searched_tweets['verified'] 
    if ('statuses_count' in searched_tweets['user']): 
        statuses_count = searched_tweets['user']['statuses_count']
    if ('lang' in searched_tweets): 
        lang = searched_tweets['lang']
        
    if ('contributors_enabled' in searched_tweets['user']): 
        contributors_enabled = searched_tweets['user']['contributors_enabled'] 
    if ('is_translator' in searched_tweets['user']): 
        is_translator = searched_tweets['user']['is_translator'] 
    if ('is_translation_enabled' in searched_tweets): 
        is_translation_enabled = searched_tweets['is_translation_enabled'] 
    if ('profile_background_color' in searched_tweets['user']): 
        profile_background_color = searched_tweets['user']['profile_background_color']
    if ('profile_background_image_url' in searched_tweets['user']): 
        profile_background_image_url = searched_tweets['user']['profile_background_image_url'] 
    if ('profile_background_image_url_https' in searched_tweets['user']): 
        profile_background_image_url_https = searched_tweets['user']['profile_background_image_url_https'] 
    if ('profile_background_tile' in searched_tweets['user']): 
        profile_background_tile = searched_tweets['user']['profile_background_tile'] 
    if ('profile_image_url' in searched_tweets['user']): 
        profile_image_url = searched_tweets['user']['profile_image_url'] 
    if ('profile_image_url_https' in searched_tweets['user']): 
        profile_image_url_https = searched_tweets['user']['profile_image_url_https'] 
    if ('profile_banner_url' in searched_tweets['user']): 
        profile_banner_url = searched_tweets['user']['profile_banner_url'] 
    if ('profile_link_color' in searched_tweets['user']): 
        profile_link_color = searched_tweets['user']['profile_link_color'] 
    if ('profile_sidebar_border_color' in searched_tweets['user']): 
        profile_sidebar_border_color = searched_tweets['user']['profile_sidebar_border_color'] 
    if ('profile_sidebar_fill_color' in searched_tweets['user']): 
        profile_sidebar_fill_color = searched_tweets['user']['profile_sidebar_fill_color'] 
    if ('profile_text_color' in searched_tweets['user']): 
        profile_text_color = searched_tweets['user']['profile_text_color'] 
    if ('profile_use_background_image' in searched_tweets['user']): 
        profile_use_background_image = searched_tweets['user']['profile_use_background_image'] 
    if ('has_extended_profile' in searched_tweets['user']): 
        has_extended_profile = searched_tweets['user']['has_extended_profile'] 
        
    if ('default_profile' in searched_tweets['user']): 
        default_profile = searched_tweets['user']['default_profile'] 
    if ('default_profile_image' in searched_tweets['user']): 
        default_profile_image = searched_tweets['user']['default_profile_image'] 
    if ('following' in searched_tweets['user']): 
        following = searched_tweets['user']['following'] 
    if ('follow_request_sent' in searched_tweets['user']): 
        follow_request_sent = searched_tweets['user']['follow_request_sent'] 
    if ('notifications' in searched_tweets['user']): 
        notifications = searched_tweets['user']['notifications'] 
    if ('geo' in searched_tweets): 
        geo = searched_tweets['geo'] 
    if ('coordinates' in searched_tweets): 
        coordinates = searched_tweets['coordinates'] 
    if ('listed_count' in searched_tweets['user']): 
        listed_count = searched_tweets['user']['listed_count'] 
    if ('verified' in searched_tweets['user']): 
        verified = searched_tweets['user']['verified'] 
    #if ('place' in searched_tweets): 
    #    place = searched_tweets['place']
        
    if ('contributors' in searched_tweets): 
        contributors = searched_tweets['contributors'] 
    if ('is_quote_status' in searched_tweets): 
        is_quote_status = searched_tweets['is_quote_status']
    if ('retweet_count' in searched_tweets): 
        retweet_count = searched_tweets['retweet_count'] 
    if ('favorite_count' in searched_tweets): 
        favorite_count = searched_tweets['favorite_count'] 
    if ('favorited' in searched_tweets): 
        favorited = searched_tweets['favorited'] 
    if ('retweeted' in searched_tweets): 
        retweeted = searched_tweets['retweeted'] 
    if ('possibly_sensitive' in searched_tweets):
        possibly_sensitive = searched_tweets['possibly_sensitive']
        
    if('attributes' in searched_tweets['place']):
        attributes = searched_tweets['place']['attributes']
    if('bounding_box' in searched_tweets['place']):
        bounding_box = searched_tweets['place']['bounding_box']
    if('contained_within' in searched_tweets['place']):
        contained_within = searched_tweets['place']['contained_within']
    if('country' in searched_tweets['place']):
        country = searched_tweets['place']['country']
    if('country_code' in searched_tweets['place']):
        country_code = searched_tweets['place']['country_code']
    if('full_name' in searched_tweets['place']):
        full_name = searched_tweets['place']['full_name']
    if('id' in searched_tweets['place']):
        tweet_place_id = searched_tweets['place']['id']
    if('name' in searched_tweets['place']):
        place_name = searched_tweets['place']['name']
    if('place_type' in searched_tweets['place']):
        place_type = searched_tweets['place']['place_type']
    if('url' in searched_tweets['place']):
        place_url = searched_tweets['place']['url']
        
    if('created_at' in searched_tweets['user']):
        user_created_at = searched_tweets['user']['created_at']
    if('description' in searched_tweets['user']):
        description = searched_tweets['user']['description']
    if('id' in searched_tweets['user']):
        user_id = searched_tweets['user']['id']
    if('id_str' in searched_tweets['user']):
        user_id_str = searched_tweets['user']['id_str']
    if('location' in searched_tweets['user']):
        location = searched_tweets['user']['location']
    if('name' in searched_tweets['user']):
        name = searched_tweets['user']['name']
    if('screen_name' in searched_tweets['user']):
        screen_name = searched_tweets['user']['screen_name']
    if('hashtags' in searched_tweets['entities']):
        for hashtag in searched_tweets['entities']['hashtags']:
            if 'text' in hashtag:
                hashtags = hashtags + ',' + hashtag['text']

    data = data.append({'created_at': created_at, 'id': id, 'id_str': id_str, 'text': text, 'truncated': truncated, 
                     'source': source, 'in_reply_to_status_id': in_reply_to_status_id, 'in_reply_to_status_id_str': in_reply_to_status_id_str, 
                     'in_reply_to_user_id': in_reply_to_user_id, 'in_reply_to_user_id_str': in_reply_to_user_id_str, 'in_reply_to_screen_name': in_reply_to_screen_name, 
                     'protected': protected, 'followers_count': followers_count, 'friends_count': friends_count, 'listed_count': listed_count, 
                     'favourites_count': favourites_count, 'utc_offset': utc_offset, 'time_zone': time_zone, 'geo_enabled': geo_enabled, 'verified': verified, 'statuses_count': statuses_count, 'lang': lang, 'contributors_enabled': contributors_enabled, 
                     'is_translator': is_translator, 'is_translation_enabled': is_translation_enabled, 'profile_background_color': profile_background_color, 
                     'profile_background_image_url': profile_background_image_url, 'profile_background_image_url_https': profile_background_image_url_https, 
                     'profile_background_tile': profile_background_tile, 'profile_image_url': profile_image_url, 'profile_image_url_https': profile_image_url_https, 
                     'profile_banner_url': profile_banner_url, 'profile_link_color': profile_link_color, 'profile_sidebar_border_color': profile_sidebar_border_color, 
                     'profile_sidebar_fill_color': profile_sidebar_fill_color, 'profile_text_color': profile_text_color, 'profile_use_background_image': profile_use_background_image, 
                     'has_extended_profile': has_extended_profile, 'default_profile': default_profile, 'default_profile_image': default_profile_image, 'following': following, 
                     'follow_request_sent': follow_request_sent, 'notifications': notifications, 'geo': geo, 'coordinates': coordinates, 
                     'contributors': contributors, 'is_quote_status': is_quote_status, 
                     'retweet_count': retweet_count, 'favorite_count': favorite_count, 'favorited': favorited, 'retweeted': retweeted, 
                     'possibly_sensitive': possibly_sensitive, 'attributes': attributes, 
                     'bounding_box': bounding_box, 'contained_within': contained_within, 'country': country, 
                     'country_code': country_code, 'full_name': full_name, 'tweet_place_id': tweet_place_id, 'place_name': place_name, 
                     'place_type': place_type, 'place_url': place_url, 'user_created_at': user_created_at, 'description': description, 
                     'user_id': user_id, 'user_id_str': user_id_str, 'location': location, 'name': name, 'screen_name': screen_name, 
                     'hashtags': hashtags}, ignore_index = True)
 
"""    	data = data.append({'created_at': searched_tweets['created_at'], 'id_str': searched_tweets['id_str'], 'text': searched_tweets['text'], 'truncated': searched_tweets['truncated'], 
                         'source': searched_tweets['source'], 'in_reply_to_status_id': searched_tweets['in_reply_to_status_id'], 'in_reply_to_status_id_str': searched_tweets['in_reply_to_status_id_str'], 
                         'in_reply_to_user_id': searched_tweets['in_reply_to_user_id'], 'in_reply_to_user_id_str': searched_tweets['in_reply_to_user_id_str'], 'in_reply_to_screen_name': searched_tweets['in_reply_to_screen_name'], 
                         'protected': searched_tweets['protected'], 'followers_count': searched_tweets['followers_count'], 'friends_count': searched_tweets['friends_count'], 'listed_count': searched_tweets['listed_count'], 
                         'favourites_count': searched_tweets['favourites_count'], 'utc_offset': searched_tweets['utc_offset'], 'time_zone': searched_tweets['time_zone'], 'geo_enabled': searched_tweets['geo_enabled'], 'verified': searched_tweets['verified'], 'statuses_count': searched_tweets['statuses_count'], 'lang': searched_tweets['lang'], 'contributors_enabled': searched_tweets['contributors_enabled'], 
                         'is_translator': searched_tweets['is_translator'], 'is_translation_enabled': searched_tweets['is_translation_enabled'], 'profile_background_color': searched_tweets['profile_background_color'], 
                         'profile_background_image_url': searched_tweets['profile_background_image_url'], 'profile_background_image_url_https': searched_tweets['profile_background_image_url_https'], 
                         'profile_background_tile': searched_tweets['profile_background_tile'], 'profile_image_url': searched_tweets['profile_image_url'], 'profile_image_url_https': searched_tweets['profile_image_url_https'], 
                         'profile_banner_url': searched_tweets['profile_banner_url'], 'profile_link_color': searched_tweets['profile_link_color'], 'profile_sidebar_border_color': searched_tweets['profile_sidebar_border_color'], 
                         'profile_sidebar_fill_color': searched_tweets['profile_sidebar_fill_color'], 'profile_text_color': searched_tweets['profile_text_color'], 'profile_use_background_image': searched_tweets['profile_use_background_image'], 
                         'has_extended_profile': searched_tweets['has_extended_profile'], 'default_profile': searched_tweets['default_profile'], 'default_profile_image': searched_tweets['default_profile_image'], 'following': searched_tweets['following'], 
                         'follow_request_sent': searched_tweets['follow_request_sent'], 'notifications': searched_tweets['notifications'], 'geo': searched_tweets['geo'], 'coordinates': searched_tweets['coordinates'], 
                         'place': searched_tweets['place'], 'contributors': searched_tweets['contributors'], 'is_quote_status': searched_tweets['is_quote_status'], 
                         'retweet_count': searched_tweets['retweet_count'], 'favorite_count': searched_tweets['favorite_count'], 'favorited': searched_tweets['favorited'], 'retweeted': searched_tweets['retweeted'], 'possibly_sensitive': searched_tweets['possibly_sensitive']}, ignore_index = True)
"""