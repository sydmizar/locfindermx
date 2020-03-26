from geopy.geocoders import Nominatim
import numpy as np

geolocator = Nominatim(user_agent='Equivalency App')

# Tuple means: key, literal_eval, null_value
KEY_NAME, NULL_VAL, SQL_TYPE = 0, 1, 2

# Tablas para Foursquare
# tb_frsq_venues
frsq_columns_venues = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'), # String
    ('name',            '',         'text'),                    # String
    ('storeId',         '',         'VARCHAR(25)'),             # String
    ('contact',         {},         'json'),                    # Dict
    ('location',        {},         'json'),                    # Dict
    ('categories',      [],         'json'),                    # List of Dicts
    ('verified',        False,      'boolean'),                 # Boolean
    ('stats',           {},         'json'),                    # Dict
    ('url',             '',         'text'),                    # Sting
    ('hours',           {},         'json'),                    # Dict
    ('popular',         {},         'json'),                    # Dict
    ('menu',            {},         'json'),                    # Dict
    ('price',           {},         'json'),                    # Dict
    ('rating',          None,       'numeric'),                 # Value
    ('description',     '',         'text'),                    # String
    ('createdAt',       None,       'numeric'),                 # Value
    ('shortUrl',        '',         'text'),                    # String
    ('canonicalUrl',    '',         'text'),                    # String
    ('likes',           {},         'json'),                    # Dict
    ('attributes',      {},         'json'),                    # Dict with list of Dicts
    ('page',            {},         'json'),                    # Dict
    ('bestPhoto',       {},         'json')                     # Dict
])
# tb_frsq_tips
frsq_columns_tips = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'),
    ('type',            '',         'text'),
    ('createdAt',       None,       'numeric'),
    ('user',            {},         'json'),
    ('venue',           '',         'VARCHAR(25)'),
    ('canonicalUrl',    '',         'text'),
    ('lang',            '',         'text'),
    ('text',            '',         'text'),
    ('photo',           {},         'json')
])
# tb_frsq_lists
frsq_columns_lists = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'),  # String
    ('name',            '',         'text'),                     # String
    ('description',     '',         'text'),
    ('user',            {},         'json'),
    ('followers',       {},         'json'),
    ('editable',        False,      'boolean'),
    ('collaborative',   False,      'boolean'),
    ('canonicalUrl',    '',         'text'),
    ('photo',           {},         'json'),
    ('updatedAt',       None,       'numeric'),
    ('venue',           '',         'VARCHAR(25)')
])
# tb_frsq_photos
frsq_columns_photos = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'),
    ('createdAt',       None,       'numeric'),
    ('venue',           '',         'VARCHAR(25)'),
    ('prefix',          '',         'text'),
    ('suffix',          '',         'text'),
    ('height',          None,       'numeric'),
    ('width',           None,       'numeric'),
    ('user',            {},         'json')
])

# Tablas para Yelp
columns = np.array([
    ('id',              '',         'VARCHAR(25) Primary KEY'), # String varchar 255
    ('alias',           '',         'text'),                    # String
    ('name',            '',         'text'),                    # String
    ('image_url',       '',         'text'),                    # String
    ('is_claimed',      False,      'boolean'),                 # Boolean
    ('is_closed',       False,      'boolean'),                 # Boolean
    ('url',             '',         'text'),                    # String
    ('phone',           '',         'text'),                    # Sting
    ('display_phone',   '',         'text'),                    # String
    ('review_count',    None,       'numeric'),                 # Integer
    ('categories',      '',         'json'),                    # Dict
    ('rating',          None,       'numeric'),                 # Float
    ('location',        '',         'json'),                    # Dict json
    ('coordinates',     '',         'json'),                    # Dict
    ('photos',          '',         'text'),                    # String
    ('price',           '',         'text'),                    # String
    ('hours',           '',         'json'),                    # Dict
    ('transactions',    '',         'text'),                    # String
    ('special_hours',   '',         'json')                     # Dict
])
columns1 = np.array([
    ('bussinessid',     '',         'VARCHAR(25) Primary Key'),
    ('id',              '',         'text'),
    ('rating',          None,       'numeric'),
    ('user',            '',         'json'),
    ('text',            '',         'text'),
    ('time_created',    '',         'text'),
    ('url',             '',         'text')
])

# Tablas para Denue

# Tablas de Tumblr
# tb_tmbr_users
tmbl_columns_User = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'), # String
    ('name',            '',         'text'),                    # String
    ('storeId',         '',         'VARCHAR(25)'),             # String
    ('contact',         {},         'json'),                    # Dict
    ('location',        {},         'json'),                    # Dict
    ('categories',      [],         'json'),                    # List of Dicts
    ('verified',        False,      'boolean'),                 # Boolean
    ('stats',           {},         'json'),                    # Dict
    ('url',             '',         'text'),                    # Sting
    ('hours',           {},         'json'),                    # Dict
    ('popular',         {},         'json'),                    # Dict
    ('menu',            {},         'json'),                    # Dict
    ('price',           {},         'json'),                    # Dict
    ('rating',          None,       'numeric'),                 # Value
    ('description',     '',         'text'),                    # String
    ('createdAt',       None,       'numeric'),                 # Value
    ('shortUrl',        '',         'text'),                    # String
    ('canonicalUrl',    '',         'text'),                    # String
    ('likes',           {},         'json'),                    # Dict
    ('attributes',      {},         'json'),                    # Dict with list of Dicts
    ('page',            {},         'json'),                    # Dict
    ('bestPhoto',       {},         'json')                     # Dict
])
# tb_tmbr_blog_info
tmbl_columns_blog_Info = np.array([
    ('ask',             '',         'boolean'),                 # Boolean
    ('ask_anon	',      '',         'boolean'),                 # Boolean
    ('ask_page_title',  '',         'VARCHAR(25)'),             # String
    ('followed	',      '',         'boolean'),                 # Boolean
    ('is_blocked_from_primary', False,'boolean'),               # Boolean
    ('is_nsfw',       False,        'boolean'),                 # Boolean
    ('is_optout_ads',    '',        'boolean'),                 # Boolean
    ('likes',           None,       'numeric'),                 # Value
    ('name',             '',        'text'),                    # Sting
    ('posts',           None,       'numeric'),                 # Value
    ('share_likes',     '',         'boolean'),                 # Boolean
    ('title',           '',         'text'),                    # String
    ('total_posts',     None,       'numeric'),                 # Value
    ('updated',         '',         'text'),                    # String
    ('url',             '',         'text'),                    # String
    ('uuid',             '',        'text')                    # String
])
# tb_tmbr_blog_tags
tmbl_columns_blog_tags = np.array([
    ('type',            '',         'VARCHAR(25)'),             # String
    ('blog_name',       '',         'text'),                    # String
    ('blog	   ',       {},         'json'),                    # Dict
    ('id',              None,       'numeric'),                 # Value
    ('id_string	',      '',         'VARCHAR(25) PRIMARY KEY'),  # String
    ('post_url  ',      '',         'text'),                    # String
    ('slug',            '',         'text'),                     # String
    ('date',            '',         'text'),                     # String
    ('timestamp',       None,       'numeric'),                  # Value
    ('state',           '',         'text'),                     # Sting
    ('format',          '',         'text'),                     # Sting
    ('reblog_key',      '',         'text'),                     # Sting
    ('tags',            {},         'json'),                     # Dict
    ('short_url',       '',         'text'),                     # Sting
    ('recommended_source',  '',     'text'),                     # Sting
    ('followed',        False,      'boolean'),                  # Boolean
    ('liked',           False,      'boolean'),                  # Boolean
    ('reblog',          '',         'text'),                     # String
    ('trail',           '',         'text'),                     # String
    ('source_url',      '',         'text'),                     # String
    ('source_title',    '',         'text'),                     # String
    ('html5_capable',   False,      'boolean'),                  # Boolean
    ('thumbnail_url',   '',         'text'),                     # String
    ('title',           '',         'text'),                     # String
    ('body',            '',         'text')                     # String
])

# Tablas Google Places
# tb_gp_places
gp_column_places = np.array([
    ('id',              '',         'VARCHAR(25) PRIMARY KEY'),  #1 String
    ('place_id',        '',         'text'),  #2 String
    ('plus_code',       {},         'json'),  #3 Dict
    ('name',            '',         'text'),  #4 String
    ('price_level',     None,       'numeric'),  #5 Int
    ('rating',          None,       'numeric'), #6 Value
    ('user_ratings_total', None,    'numeric'),  # Int
    ('icon',            'string',   'text'),  #7 String
    ('photos',          [],         'json'),  #8 List of Dicts
    ('reference',       '',         'text'),  #9 String
    ('types',           [],         'json'),  #10 List
    ('vicinity',        '',         'text'),  #11 String
    ('geometry',        {},         'json'),  #12 Dictionary
])
