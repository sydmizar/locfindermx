from geopy.geocoders import Nominatim
import numpy as np

geolocator = Nominatim(user_agent='LocMapApp')

# Tuple means: key, literal_eval, null_value
KEY_NAME, NULL_VAL, SQL_TYPE = 0, 1, 2
FS_LIMIT = 18
YP_LIMIT = 17
GP_LIMIT = 11

# Tablas para Foursquare
# tb_frsq_venues
frsq_columns_venues = np.array([
    ('id',              '',         'VARCHAR(40) PRIMARY KEY'), # String
    ('name',            '',         'text'),                    # String
    ('storeId',         '',         'VARCHAR(40)'),             # String
    ('categories',      '[]',       'json'),                    # List of Dicts
    ('verified',        False,      'boolean'),                 # Boolean
    ('url',             '',         'text'),                    # Sting
    ('rating',          None,       'numeric'),                 # Value
    ('description',     '',         'text'),                    # String
    ('createdAt',       None,       'numeric'),                 # Value
    ('shortUrl',        '',         'text'),                    # String
    ('canonicalUrl',    '',         'text'),                    # String
    ('attributes',      '{}',       'json'),                    # Dict with list of Dicts
    ('contact',         '{}',       'json'),                    # Dict
    ('hours',           '{}',       'json'),                    # Dict
    ('popular',         '{}',       'json'),                    # Dict
    ('menu',            '{}',       'json'),                    # Dict
    ('page',            '{}',       'json'),                    # Dict
    ('bestPhoto',       '{}',       'json'),                     # Dict
    ('cc',              '',         'VARCHAR(5)'),     # UNRAVELED FROM INTERNAL DICTS
    ('neighborhood',    '',         'text'),
    ('city',            '',         'text'),
    ('state',           '',         'text'),
    ('country',         '',         'text'),
    ('tipCount',        None,       'numeric'),
    ('likeCount',       None,       'numeric'),
    ('address',         '',         'text'),
    ('crossStreet',     '',         'text'),
    ('lat',             '',         'text'),
    ('lng',             '',         'text'),
    ('postalCode',      '',         'text')
])
# tb_frsq_tips
frsq_columns_tips = np.array([
    ('id',              '',         'VARCHAR(40) PRIMARY KEY'),
    ('type',            '',         'text'),
    ('createdAt',       None,       'numeric'),
    ('user',            '{}',       'json'),
    ('venue',           '',         'VARCHAR(40)'),
    ('canonicalUrl',    '',         'text'),
    ('lang',            '',         'text'),
    ('text',            '',         'text'),
    ('photo',           '{}',       'json')
])
# tb_frsq_lists
frsq_columns_lists = np.array([
    ('id',              '',         'VARCHAR(40) PRIMARY KEY'),  # String
    ('name',            '',         'text'),                     # String
    ('description',     '',         'text'),
    ('user',            '{}',       'json'),
    ('followers',       '{}',       'json'),
    ('editable',        False,      'boolean'),
    ('collaborative',   False,      'boolean'),
    ('canonicalUrl',    '',         'text'),
    ('photo',           '{}',       'json'),
    ('updatedAt',       None,       'numeric'),
    ('venue',           '',         'VARCHAR(40)')
])
# tb_frsq_photos
frsq_columns_photos = np.array([
    ('id',              '',         'VARCHAR(40) PRIMARY KEY'),
    ('createdAt',       None,       'numeric'),
    ('venue',           '',         'VARCHAR(40)'),
    ('prefix',          '',         'text'),
    ('suffix',          '',         'text'),
    ('height',          None,       'numeric'),
    ('width',           None,       'numeric'),
    ('user',            '{}',       'json')
])

# Tablas para Yelp
yelp_columns = np.array([
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
    ('categories',      '{}',       'json'),                    # Dict
    ('rating',          None,       'numeric'),                 # Float
    ('photos',          '',         'text'),                    # String
    ('price',           '',         'text'),                    # String
    ('hours',           '{}',       'json'),                    # Dict
    ('transactions',    '',         'text'),                    # String
    ('special_hours',   '{}',       'json'),                    # Dict
    ('address1',        '',         'text'),            # YELP Unraveled Internal Dicts
    ('city',            '',         'text'),
    ('zip_code',        '',         'VARCHAR(6)'),
    ('country',         '',         'VARCHAR(3)'),
    ('state',           '',         'VARCHAR(4)'),
    ('latitude',        None,       'numeric'),
    ('longitude',       None,       'numeric')
])
yelp_columns1 = np.array([
    ('id',              '',         'VARCHAR(25) Primary Key'),
    ('bussinessid',     '',         'VARCHAR(25)'),
    ('rating',          None,       'numeric'),
    ('user',            '{}',       'json'),
    ('text',            '',         'text'),
    ('time_created',    '',         'text'),
    ('url',             '',         'text')
])

# Tablas para Denue
denu_columns_Extract = np.array([
    ('Id',              '',         'VARCHAR(40) PRIMARY KEY'), # String
    ('Nombre',          '',         'text'),                    # String
    ('Razon_social',    '',         'VARCHAR(40)'),             # String
    ('Clase_actividad', '',         'text'),                    # String
    ('Estrato',        '',         'text'),                    # String
    ('Tipo_vialidad',   '',         'text'),                    # String
    ('Calle',           '',         'text'),                    # String
    ('Num_Exterior',    None,       'numeric'),                 # Value
    ('Num_Interior',    None,       'numeric'),                 # Value
    ('Colonia',         '',         'text'),                    # String
    ('CP',             None,       'numeric'),                 # Value
    ('Ubicacion',       None,       'numeric'),                 # Value
    ('Telefono',        None,       'numeric'),                 # Value
    ('Correo_e',        None,       'numeric'),                 # Value
    ('Sitio_internet',    '',       'text'),                    # String
    ('Tipo',            None,       'numeric'),                 # Value
    ('Longitud',          '',        'text'),                   # String
    ('Latitud',           '',        'text'),                   # String
    ('CentroComercial', '',         'text'),                    # String
    ('TipoCentroComercial', '',     'text'),                    # String
    ('NumLocal',        None,       'numeric'),                 # Value
    ('tipo_corredor_industrial','', 'text'),                    # String
    ('nom_corredor_industrial','',  'text'),                    # String
    ('numero_local',    None,       'numeric'),                 # Value
])

# Tablas de Tumblr
# tb_tmbr_users
tmbl_columns_User = np.array([
    ('id',              '',         'VARCHAR(40) PRIMARY KEY'), # String
    ('name',            '',         'text'),                    # String
    ('storeId',         '',         'VARCHAR(40)'),             # String
    ('contact',         '{}',       'json'),                    # Dict
    ('location',        '{}',       'json'),                    # Dict
    ('categories',      '[]',       'json'),                    # List of Dicts
    ('verified',        False,      'boolean'),                 # Boolean
    ('stats',           '{}',       'json'),                    # Dict
    ('url',             '',         'text'),                    # Sting
    ('hours',           '{}',       'json'),                    # Dict
    ('popular',         '{}',       'json'),                    # Dict
    ('menu',            '{}',       'json'),                    # Dict
    ('price',           '{}',       'json'),                    # Dict
    ('rating',          None,       'numeric'),                 # Value
    ('description',     '',         'text'),                    # String
    ('createdAt',       None,       'numeric'),                 # Value
    ('shortUrl',        '',         'text'),                    # String
    ('canonicalUrl',    '',         'text'),                    # String
    ('likes',           '{}',       'json'),                    # Dict
    ('attributes',      '{}',       'json'),                    # Dict with list of Dicts
    ('page',            '{}',       'json'),                    # Dict
    ('bestPhoto',       '{}',       'json')                     # Dict
])
# tb_tmbr_blog_info
tmbl_columns_blog_Info = np.array([
    ('ask',             '',         'boolean'),                 # Boolean
    ('ask_anon	',      '',         'boolean'),                 # Boolean
    ('ask_page_title',  '',         'VARCHAR(40)'),             # String
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
    ('type',            '',         'VARCHAR(40)'),             # String
    ('blog_name',       '',         'text'),                    # String
    ('blog	   ',       '{}',       'json'),                    # Dict
    ('id',              None,       'numeric'),                 # Value
    ('id_string	',      '',         'VARCHAR(40) PRIMARY KEY'),  # String
    ('post_url  ',      '',         'text'),                    # String
    ('slug',            '',         'text'),                     # String
    ('date',            '',         'text'),                     # String
    ('timestamp',       None,       'numeric'),                  # Value
    ('state',           '',         'text'),                     # Sting
    ('format',          '',         'text'),                     # Sting
    ('reblog_key',      '',         'text'),                     # Sting
    ('tags',            '{}',       'json'),                     # Dict
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
gp_columns_places = np.array([
    ('id',              '',         'VARCHAR(40)'),             # 0 String
    ('place_id',        '',         'VARCHAR(40) PRIMARY KEY'), # 1 String
    ('name',            '',         'text'),                    # 2 String
    ('price_level',     None,       'numeric'),                 # 3 Int
    ('rating',          None,       'numeric'),                 # 4 Value
    ('user_ratings_total', None,    'numeric'),                 # 5 Int
    ('icon',            'string',   'text'),                    # 6 String
    ('photos',          '[]',       'json'),                    # 7 List of Dicts
    ('reference',       '',         'text'),                    # 8 String
    ('types',           '[]',       'json'),                    # 9 List
    ('vicinity',        '',         'text'),                    # 10 String
    ('compound_code',   '',         'text'),        # Unraveled Data
    ('global_code',     '',         'text'),
    ('latitude',        None,       'numeric'),
    ('longitude',       None,       'numeric')
])
