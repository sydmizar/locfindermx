# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 16:17:39 2020

@author: BALAMLAPTOP2
"""
import numpy as np

# Tuple means: key, literal_eval, null_value
KEY_NAME, NULL_VAL, SQL_TYPE = 0, 1, 2

# Tablas para Twitter
# tb_tw_alldata

tw_columns_alldata = np.array([
        ('created_at',                              '',     'VARCHAR(50)'), 
        ('id',                                      '',     'numeric PRIMARY KEY'), 
        ('id_str',                                  '',     'VARCHAR(25)'), 
        ('text',                                    '',     'text'), 
        ('truncated',                               False,  'boolean'), 
        ('source',                                  '',     'VARCHAR(50)'), 
        ('in_reply_to_status_id',                   None,   'numeric'), 
        ('in_reply_to_status_id_str',               '',     'VARCHAR(25)'), 
        ('in_reply_to_user_id',                     None,   'numeric'), 
        ('in_reply_to_user_id_str',                 '',     'VARCHAR(25)'), 
        ('in_reply_to_screen_name',                 '',     'VARCHAR(25)'), 
        ('protected',                               False,  'boolean'), 
        ('followers_count',                         None,   'numeric'), 
        ('friends_count',                           None,   'numeric'), 
        ('favourites_count',                        None,   'numeric'), 
        ('utc_offset',                              None,   'numeric'), 
        ('time_zone',                               None,   'numeric'), 
        ('geo_enabled',                             False,  'boolean'), 
        ('verified',                                False,  'boolean'), 
        ('statuses_count',                          None,   'numeric'), 
        ('lang',                                    '',     'VARCHAR(5)'), 
        ('contributors_enabled',                    False,  'boolean'), 
        ('is_translator',                           False,  'boolean'), 
        ('profile_background_color',                '',     'VARCHAR(6)'), 
        ('profile_background_image_url',            '',     'VARCHAR(50)'), 
        ('profile_background_image_url_https',      '',     'VARCHAR(50)'), 
        ('profile_background_tile',                 False,  'boolean'), 
        ('profile_image_url',                       '',     'VARCHAR(50)'), 
        ('profile_image_url_https',                 '',     'VARCHAR(50)'), 
        ('profile_banner_url',                      '',     'VARCHAR(50)'), 
        ('profile_link_color',                      '',     'VARCHAR(6)'), 
        ('profile_sidebar_border_color',            '',     'VARCHAR(6)'), 
        ('profile_sidebar_fill_color',              '',     'VARCHAR(6)'), 
        ('profile_text_color',                      '',     'VARCHAR(6)'), 
        ('profile_use_background_image',            False,  'boolean'), 
        ('has_extended_profile',                    False,  'boolean'), 
        ('default_profile',                         False,  'boolean'), 
        ('default_profile_image',                   False,  'boolean'), 
        ('following',                               False,  'boolean'), 
        ('follow_request_sent',                     False,  'boolean'), 
        ('notifications',                           False,  'boolean'), 
#        ('geo',                                     None,   'numeric'), 
#        ('coordinates',                             None,   'numeric'), 
        ('listed_count',                            None,   'numeric'), 
        ('verified',                                False,  'boolean'), 
        ('contributors',                            None,   'numeric'), 
        ('is_quote_status',                         False,  'boolean'), 
        ('retweet_count',                           None,   'numeric'), 
        ('favorite_count',                          None,   'numeric'), 
        ('favorited',                               False,  'boolean'), 
        ('retweeted',                               False,  'boolean'), 
        ('possibly_sensitive',                      False,  'boolean'),
#        ('bounding_box',                            '{}',   'json'), 
        ('country',                                 '',     'VARCHAR(25)'), 
        ('country_code',                            '',     'VARCHAR(2)'), 
        ('full_name',                               '',     'VARCHAR(50)'), 
        ('tweet_place_id',                          '',     'VARCHAR(50)'), 
        ('place_name',                              '',     'VARCHAR(25)'), 
        ('place_type',                              '',     'VARCHAR(5)'), 
        ('place_url',                               '',     'VARCHAR(50)'), 
        ('user_created_at',                         '',     'VARCHAR(50)'), 
        ('description',                             '',     'text'), 
        ('user_id',                                 None,   'numeric'), 
        ('user_id_str',                             '',     'VARCHAR(50)'), 
        ('location',                                '',     'VARCHAR(50)'), 
        ('name',                                    '',     'VARCHAR(50)'), 
        ('screen_name',                             '',     'VARCHAR(25)'), 
        ('hashtags',                                '',     'text'), 
        ('user_mentions_name',                      '',     'text'), 
        ('user_mentions_screen_name',               '',     'text')
        ])
