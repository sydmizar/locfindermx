"""
locmap_APIs Version Control:
    Version = 3.0
    Date = 27/03/20
"""
import requests
import json
import pandas as pd
from geopy.geocoders import Nominatim
from global_vars import *
from pprint import pprint
import sqlalchemy
import pandas as pd
import numpy as np
import requests
import geopy
import json
import time
import sys

################################################################################
#  >  TABLE EDITOR AND CUSTOM ERROR
################################################################################


class TableEditor():
    def __init__(self, conn):
        self.conn = conn
        self.tables_content = {
            'tb_frsq_venues': frsq_columns_venues,
            'tb_frsq_tips': frsq_columns_tips,
            'tb_frsq_photos': frsq_columns_photos,
            'tb_frsq_lists': frsq_columns_lists,
            'tb_yelp_businesses': yelp_columns,
            'tb_yelp_reviews': yelp_columns1,
            'tb_tmbl_users': tmbl_columns_User,
            'tb_tmbl_info': tmbl_columns_blog_Info,
            'tb_tmbl_tags': tmbl_columns_blog_tags,
            'tb_gp_places': gp_columns_places,
            'tb_denu_venues': denu_columns_Extract
        }

    def create_table(self, table_name):
        c_names = self.tables_content[table_name]
        columns_query = list()
        for name, sql_type in zip(c_names[:, KEY_NAME], c_names[:, SQL_TYPE]):
            columns_query.append('"{}" {}'.format(name, sql_type))
        SQL_query = """
        CREATE TABLE {}(
            {}
        );
        """.format(table_name, ',\n'.join(columns_query))
        try:
            self.conn.execute(SQL_query)
        except sqlalchemy.exc.ProgrammingError:
            return

    def create_tables(self, apis='all'):
        if apis == 'all' or 'foursquare' in apis:
            self.create_table('tb_frsq_venues')
            self.create_table('tb_frsq_tips')
            self.create_table('tb_frsq_lists')
            self.create_table('tb_frsq_photos')
        if apis == 'all' or 'yelp' in apis:
            self.create_table('tb_yelp_businesses')
            self.create_table('tb_yelp_reviews')
        if apis == 'all' or 'denue' in apis:
            self.create_table('tb_denu_venues')
        if apis == 'all' or 'googleplaces' in apis:
            self.create_table('tb_gp_places')
        if apis == 'all' or 'tumblr' in apis:
            self.create_table('tb_tmbl_users')
            self.create_table('tb_tmbl_info')
            self.create_table('tb_tmbl_tags')

    def alltables2csv(self):
        tb_names = list(self.tables_content.keys())
        for tb in tb_names:
            DF = pd.read_sql(sql=f"SELECT * FROM {tb}", con=self.conn)
            DF.to_csv(f"{tb}.csv", index=False)

    def erase_tables(self, apis='all'):
        if apis == 'all' or 'foursquare' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_frsq_venues')
            self.conn.execute('DROP TABLE IF EXISTS tb_frsq_tips')
            self.conn.execute('DROP TABLE IF EXISTS tb_frsq_lists')
            self.conn.execute('DROP TABLE IF EXISTS tb_frsq_photos')
        if apis == 'all' or 'yelp' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_yelp_businesses')
            self.conn.execute('DROP TABLE IF EXISTS tb_yelp_reviews')
        if apis == 'all' or 'tumblr' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_tmbl_users')
            self.conn.execute('DROP TABLE IF EXISTS tb_tmbl_info')
            self.conn.execute('DROP TABLE IF EXISTS tb_tmbl_tags')
        if apis == 'all' or 'denue' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_denu_venues')
        if apis == 'all' or 'googleplaces' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_gp_places')


class API_Error(Exception):
    pass


################################################################################
#  >  GENERAL USE FUNCTIONS DEFINITION
################################################################################


def get_table_data(conn, primary_keys, table_name):
    SQL_query = "SELECT * FROM {table} WHERE id IN ('{keys}')".format(table=table_name,
                                                                      keys="','".join(primary_keys))
    df = pd.read_sql_query(SQL_query, conn)
    return df


def get_latlon(near):
    while True:
        try:
            geocod = geolocator.geocode(near)
            return geocod.latitude, geocod.longitude
        except geopy.exc.GeocoderTimedOut:
            print('Geocodier Timed Out, Retry')
            time.sleep(1)
            continue


def row_builder(data, columns_data):
    row = dict()
    for sec in columns_data:
        try:
            if sec[SQL_TYPE] == 'json':
                row[sec[KEY_NAME]] = json.dumps(data[sec[KEY_NAME]])
            else:
                row[sec[KEY_NAME]] = [data[sec[KEY_NAME]]]
        except KeyError:
            data[sec[KEY_NAME]] = [sec[NULL_VAL]]
    return row


################################################################################
#  >  API OBJECTS DEFINITION
################################################################################


class Foursquare():
    def __init__(self,
                 engine,
                 conn,
                 client_id='4ZK4GZQS4N31KZX20GOSQZHV0TH5OHOT5014NQTVLDWDVP3J',
                 client_secret='2LDTDYXDL32UIQITBO20XY4VUA3HQHYX2ACBGA4WJ3AP5NZ0',
                 version='20200228'):
        self.engine = engine
        self.conn = conn
        self.client_id = client_id
        self.client_secret = client_secret
        self.version = version
        self.LimitReached = False

    def meta_handler(self, meta):
        if meta['code'] in [429, 403]:
            self.LimitReached = True
        error = f"\t\t<[ERROR {meta['code']}]> - {meta['errorType']}\n\t\tDetails: {meta['errorDetail']}"
        raise API_Error(error)

    def explore_venuesEP(self, near=None, ll=None, query=None, radius=250):
        endpoint_url = 'https://api.foursquare.com/v2/venues/explore'
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'radius': radius,
            'query': query,
            'limit': 50,
            'offset': 0,
            'v': self.version
        }
        if ll:
            params['ll'] = ll
        elif near:
            params['near'] = near
        else:
            print('No has introducido coordenadas ni ubicación!')
            return

        venues_id = list()
        while True:
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta'])
                break
            for item in res['response']['groups'][0]['items']:
                if item['venue']['id'] not in venues_id:
                    venues_id.append(item['venue']['id'])
            if len(res['response']['groups'][0]['items']) < 50:
                break
            params['offset'] += 50
        print(f'\t> FS Venues found: {len(venues_id)}')
        self.dt_fr_db = 0
        for venue_id in venues_id:
            try:
                self.venues_DFbuilder(venue_id)
            except API_Error as text:
                print('\n', text)
        print(f'\t\t\t>> {self.dt_fr_db} Venues already in Database')

    def venues_DFbuilder(self, venue_id):

        def details_venueEP(venue_id):
            endpoint_url = f'https://api.foursquare.com/v2/venues/{venue_id}'
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.meta_handler(res['meta'])
            return res['response']

        def unravel_fs_dicts(venue):
            data = dict()
            for tag in ['location', 'stats']:
                for key in venue[tag].keys():
                    data[key] = [venue[tag][key]]
            if 'likes' in venue.keys():
                data['likeCount'] = [venue['likes']['count']]
            del data['formattedAddress']
            if 'labeledLatLngs' in data.keys():
                del data['labeledLatLngs']
            return data

        # DataFrame Initialization
        display_df = pd.DataFrame(columns=frsq_columns_venues[:, KEY_NAME])
        registered_ids = list(pd.read_sql_query("SELECT id FROM tb_frsq_venues", self.conn).id)
        if venue_id not in registered_ids:
            # If Venue not in Database, request data from Foursquare
            if self.LimitReached:
                return
            print(f'\t\t\tVenue {venue_id} data from ', end='')
            venue = details_venueEP(venue_id)
            data = row_builder(venue['venue'], frsq_columns_venues[:FS_LIMIT, :])
            data.update(unravel_fs_dicts(venue['venue']))
            print('Foursquare - Uploading Data...', end='')

            self.lists_DFbuilder(venue_id)
            self.tips_DFbuilder(venue_id)
            self.photos_DFbuilder(venue_id)
            display_df.append(pd.DataFrame(data), ignore_index=True).to_sql(
                'tb_frsq_venues',
                con=self.engine,
                index=False,
                if_exists='append'
            )
            print('SUCCESS')
        else:
            # If Venue already in Database, continue.
            self.dt_fr_db += 1

    def lists_DFbuilder(self, venue_id):

        def list_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/listed"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'group': 'other',
                'limit': 30,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.meta_handler(res['meta'])
            return res['response']

        lists = list()
        offset = 0
        while True:
            res = list_endpoint(venue_id, offset)
            lists.extend(res['lists']['items'])
            if res['lists']['count'] == 30:
                offset += 30
                continue
            break
        print(f"{len(lists)} lists, ", end='')
        SQL_query = "SELECT id FROM tb_frsq_lists"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        upload_df = pd.DataFrame(columns=frsq_columns_lists[:, KEY_NAME])
        for listed in lists:
            listed['venue'] = venue_id
            data = row_builder(listed, frsq_columns_lists)
            if (data['id'][0] not in registered_ids) and (data['id'][0] not in list(upload_df.id)):
                upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql('tb_frsq_lists',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')

    def tips_DFbuilder(self, venue_id):

        def tips_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/tips"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'sort': 'recent',
                'limit': 500,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.meta_handler(res['meta'])
            return res['response']

        offset, tips = 0, list()
        while True:
            res = tips_endpoint(venue_id, offset)
            tips.extend(res['tips']['items'])
            if res['tips']['count'] == 500:
                offset += 500
                continue
            break
        print(f"{len(tips)} tips, ", end='')
        SQL_query = "SELECT id FROM tb_frsq_tips"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        upload_df = pd.DataFrame(columns=frsq_columns_tips[:, KEY_NAME])
        for listed in tips:
            listed['venue'] = venue_id
            data = row_builder(listed, frsq_columns_tips)
            if data['id'][0] not in registered_ids and data['id'][0] not in list(upload_df.id):
                upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql(
            'tb_frsq_tips',
            con=self.engine,
            index=False,
            if_exists='append',
            method='multi')

    def photos_DFbuilder(self, venue_id):

        def photos_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/photos"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'limit': 200,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.meta_handler(res['meta'])
            return res['response']

        offset, photos = 0, list()
        while True:
            res = photos_endpoint(venue_id, offset)
            photos.extend(res['photos']['items'])
            if res['photos']['count'] == 200:
                offset += 200
                continue
            break
        print(f"{len(photos)} photos - ", end='')
        SQL_query = "SELECT id FROM tb_frsq_photos"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        upload_df = pd.DataFrame(columns=frsq_columns_photos[:, KEY_NAME])
        for listed in photos:
            listed['venue'] = venue_id
            data = row_builder(listed, frsq_columns_photos)
            if data['id'][0] not in registered_ids and data['id'][0] not in list(upload_df.id):
                upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql(
            'tb_frsq_photos',
            con=self.engine,
            index=False,
            if_exists='append',
            method='multi')


# Foursquare Backup
class Foursquare_BACKUP():
    def __init__(self,
                 engine,
                 conn,
                 client_id='4ZK4GZQS4N31KZX20GOSQZHV0TH5OHOT5014NQTVLDWDVP3J',
                 client_secret='2LDTDYXDL32UIQITBO20XY4VUA3HQHYX2ACBGA4WJ3AP5NZ0',
                 version='20200228'):
        self.engine = engine
        self.conn = conn
        self.client_id = client_id
        self.client_secret = client_secret
        self.version = version
        self.LimitReached = False

    def explore_venuesEP(self, near=None, ll=None, query=None, radius=250):
        """
        This function obtains all venues valid within parameter's scope and
        returns a full list of each venue's id.
            Each call to this function takes exactly one Userless request, and
            one REGULAR Endpoint call.
        Input:
            > near :: 'Benito Juarez' - As long as the string can be asociated with
                                        a geocode, it will automatically turn it
                                        into its lat & long to set the center point
                                        If BOTH 'near' and 'll' are set, only
                                        'near' will be used.
            > ll :: '1.32546,-0.6516' - Instead of searching for a geocode with a
                                        string, 'll' directly gives the center
                                        point of the search.
            > query :: 'Alitas' - Input word to be searched on locals of set area,
                                this value is a must to filter what kind of
                                places are desired to be found.
            > radius :: 250 - Sets the radius of search in meters from the center
                            point given by 'll' or 'near's geocode. Max value
                            is 10,000.
        Output:
            > venues_id :: list - The result list gives back all venues obtained
                                from the query in the given area. Each element
                                in the list is a string of the respective
                                venue's id.
        """
        if self.LimitReached:
            return

        endpoint_url = 'https://api.foursquare.com/v2/venues/explore'
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'radius': radius,
            'query': query,
            'limit': 50,
            'offset': 0,
            'v': self.version
        }
        if ll:
            params['ll'] = ll
        elif near:
            params['near'] = near
        else:
            print('No has introducido coordenadas ni ubicación!')
            return
        venues_id = list()
        while True:
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta']['errorDetail'])
                break
            for item in res['response']['groups'][0]['items']:
                if item['venue']['id'] not in venues_id:
                    venues_id.append(item['venue']['id'])
            if len(res['response']['groups'][0]['items']) < 50:
                break
            params['offset'] += 50
        print(f'\t> FS Venues found: {len(venues_id)}')
        if self.LimitReached:
            return
        request_list = self.venues_DFbuilder(venues_id)
        if not request_list:
            return
        self.lists_DFbuilder(request_list)
        self.tips_DFbuilder(request_list)
        self.photos_DFbuilder(request_list)

    def venues_DFbuilder(self, venue_list):
        """
        This functions gets full details of each venue id in the input list.
        If the id already exists in the Venues Data Base, it gets the venue
        info from there instead of making a request to Foresquare. If it does
        not exist already, after the data request, it saves the data to the
        Data Base and prepares to display it too.
        Input:
            > venue_list :: <list> - List with venues_id as string.
        Output:
            > display_df :: <DataFrame> - A dataframe composed of all data from
                                          each venue in the venue_list.
        """

        def details_venueEP(venue_id):
            endpoint_url = f'https://api.foursquare.com/v2/venues/{venue_id}'
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta']['errorDetail'])
                return
            return res['response']

        def unravel_fs_dicts(venue):
            data = dict()
            for tag in ['location', 'stats']:
                for key in venue[tag].keys():
                    data[key] = [venue[tag][key]]
            if 'likes' in venue.keys():
                data['likeCount'] = [venue['likes']['count']]
            del data['formattedAddress']
            if 'labeledLatLngs' in data.keys():
                del data['labeledLatLngs']
            return data

        display_df = pd.DataFrame(columns=frsq_columns_venues[:, KEY_NAME])
        SQL_query = "SELECT id FROM tb_frsq_venues"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        request_list = list()
        continue_list = list()
        for venue_id in venue_list:
            print(f'\t\t\tVenue with ID {venue_id} obtained from: ', end='')
            if venue_id not in registered_ids:
                venue = details_venueEP(venue_id)
                if self.LimitReached:
                    return
                data = row_builder(venue['venue'], frsq_columns_venues[:FS_LIMIT, :])
                data.update(unravel_fs_dicts(venue['venue']))
                display_df = display_df.append(pd.DataFrame(data), ignore_index=True)
                continue_list.append(venue_id)
                print('Foursquare')
            else:
                request_list.append(venue_id)
                print('Database')
        # Upload new Venues to Database
        if len(display_df) > 0:
            display_df.to_sql('tb_frsq_venues',
                              con=self.engine,
                              index=False,
                              if_exists='append',
                              method='multi')
            print('\t\tFS DATA loaded to FS DB')
        # Get Venues from Database
        display_df = display_df.append(other=get_table_data(conn=self.conn,
                                                            primary_keys=request_list,
                                                            table_name='tb_frsq_venues'),
                                       ignore_index=True)
        return continue_list

    def lists_DFbuilder(self, venue_list):
        print('\t\t > FS loading lists table')

        def list_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/listed"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'group': 'other',
                'limit': 30,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta']['errorDetail'])
                return
            return res['response']

        for venue_id in venue_list:
            offset, lists = 0, list()
            while True:
                res = list_endpoint(venue_id, offset)
                if self.LimitReached:
                    return
                lists.extend(res['lists']['items'])
                if res['lists']['count'] == 30:
                    offset += 30
                    continue
                break
            print(f"\t\t\t FS Venue {venue_id} found {len(lists)} lists", end='')
            SQL_query = "SELECT id FROM tb_frsq_lists"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_lists[:, KEY_NAME])
            for listed in lists:
                listed['venue'] = venue_id
                data = row_builder(listed, frsq_columns_lists)
                if (data['id'][0] not in registered_ids) and (data['id'][0] not in list(upload_df.id)):
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
            upload_df.to_sql(
                'tb_frsq_lists',
                con=self.engine,
                index=False,
                if_exists='append',
                method='multi')
            print(f'\t UPLOADED SUCCESFULLY')

    def tips_DFbuilder(self, venue_list):
        print('\t\t > FS loading tips table')

        def tips_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/tips"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'sort': 'recent',
                'limit': 500,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta']['errorDetail'])
                return
            return res['response']

        for venue_id in venue_list:
            offset, tips = 0, list()
            while True:
                try:
                    res = tips_endpoint(venue_id, offset)
                    if self.LimitReached:
                        return
                    tips.extend(res['tips']['items'])
                    if res['tips']['count'] == 500:
                        offset += 500
                        continue
                    break
                except KeyError:
                    raise KeyError
            print(f"\t\t\t FS Venue {venue_id} found {len(tips)} tips", end='')
            SQL_query = "SELECT id FROM tb_frsq_tips"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_tips[:, KEY_NAME])
            for listed in tips:
                listed['venue'] = venue_id
                data = row_builder(listed, frsq_columns_tips)
                if data['id'][0] not in registered_ids and data['id'][0] not in list(upload_df.id):
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
            upload_df.to_sql('tb_frsq_tips',
                             con=self.engine,
                             index=False,
                             if_exists='append',
                             method='multi')
            print(f'\t UPLOADED SUCCESFULLY')

    def photos_DFbuilder(self, venue_list):
        print('\t\t > FS loading photos table')

        def photos_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/photos"
            params = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'v': self.version,
                'limit': 200,
                'offset': offset
            }
            res = requests.get(endpoint_url, params=params).json()
            if res['meta']['code'] != 200:
                self.LimitReached = True
                print(res['meta']['errorDetail'])
                return
            return res['response']

        for venue_id in venue_list:
            offset, photos = 0, list()
            while True:
                res = photos_endpoint(venue_id, offset)
                if self.LimitReached:
                    return
                photos.extend(res['photos']['items'])
                if res['photos']['count'] == 200:
                    offset += 200
                    continue
                break
            print(f"\t\t\t FS Venue {venue_id} found {len(photos)} photos", end='')
            SQL_query = "SELECT id FROM tb_frsq_photos"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_photos[:, KEY_NAME])
            for listed in photos:
                listed['venue'] = venue_id
                data = row_builder(listed, frsq_columns_photos)
                if data['id'][0] not in registered_ids and data['id'][0] not in list(upload_df.id):
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
            upload_df.to_sql('tb_frsq_photos',
                             con=self.engine,
                             index=False,
                             if_exists='append',
                             method='multi')
            print('\tUPLOADED SUCCESFULLY')


class Yelp():
    def __init__(self, dbengine, conn, api_key="xE7CYuOc8w6Nx8gIIaHqn98bO9VJv4oiWDviCpDAg7skiAa_q4fDnfbSM2v5RfKd6xVqvJ0VJ9OiN1KhE4HUMmX3tEYmHc4a7Z2PtNYBQynSQAdwU0bLwVPV7vg5XnYx"):
        self.dbengine = dbengine
        self.conn = conn
        self.api_key = api_key

    def business_Tab(self, term='bar', limit=50, location='Guadalajara', offset=0, radius=10000):

        def unravel_yp_dicts(venue):
            data = dict()
            for tag in ['location', 'coordinates']:
                for key in venue[tag].keys():
                    data[key] = [venue[tag][key]]
            del data['address2']
            del data['address3']
            del data['display_address']
            return data

        endpoint1 = 'https://api.yelp.com/v3/businesses/search'
        HEADERS = {'Authorization': 'bearer %s' % self.api_key}
        PARAMETERS = {'term': term,
                      'limit': limit,
                      'location': location,
                      'offset': offset,
                      'radius': radius}
        response = requests.get(url=endpoint1,
                                params=PARAMETERS,
                                headers=HEADERS)
        business_Tab = response.json()['businesses']

        id_tab = [business['id'] for business in business_Tab]
        SQL_query = 'SELECT id FROM tb_yelp_businesses'
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        Business_DF = pd.DataFrame(columns=yelp_columns[:, KEY_NAME])
        to_upload = pd.DataFrame(columns=yelp_columns[:, KEY_NAME])
        print(f'\t> Yelp found: {len(business_Tab)}')
        for business in business_Tab:
            fila = row_builder(business, yelp_columns[:YP_LIMIT, :])
            fila.update(unravel_yp_dicts(business))
            row = pd.DataFrame(fila)
            if row.id[0] not in registered_ids:
                to_upload = to_upload.append(row, ignore_index='True', sort=False)
            Business_DF = Business_DF.append(row, ignore_index='True', sort=False)
        to_upload.to_sql("tb_yelp_businesses",
                         con=self.dbengine,
                         index=False,
                         if_exists='append',
                         method='multi')

        self.review_Tab(id_tab)
        # return Business_DF

    def review_Tab(self, id_tab):
        reviews_tab = []

        for id in id_tab:
            endpoint2 = 'https://api.yelp.com/v3/businesses/{}/reviews'.format(id)
            HEADERS = {'Authorization': 'bearer %s' % self.api_key}

            PARAMETERS = {'locale': 'es_MX'}
            response = requests.get(url=endpoint2.format(id),
                                    params=PARAMETERS,
                                    headers=HEADERS)
            reviews_tab.append(response.json())
        SQL_query = 'SELECT id FROM tb_yelp_reviews'
        registered_ids = list(pd.read_sql(SQL_query, self.conn).id)
        reviews_DF = pd.DataFrame(columns=yelp_columns1[:, KEY_NAME])
        for reviews, bussid in zip(reviews_tab, id_tab):
            for review in reviews['reviews']:
                review['bussinessid'] = bussid
                fila = row_builder(review, yelp_columns1)
                row = pd.DataFrame(fila)
                if row.id[0] not in registered_ids:
                    reviews_DF = reviews_DF.append(row, ignore_index='True', sort=False)

        reviews_DF.to_sql("tb_yelp_reviews",
                          con=self.dbengine,
                          index=False,
                          if_exists='append',
                          method='multi')


class GooglePlaces():
    def __init__(self, dbengine, conn, api_key="AIzaSyBKsZ5sZW_1VouFlIxGGeZgCUDjPAG_6sI"):
        self.api_key = api_key
        self.engine = dbengine
        self.conn = conn

    def search_places(self, keyword, gtype=None, ll=None, radius=1000):
        """
        type :: <string> 'restaurant' - Restringe la búsqueda a quienes cumplan con el tipo.
        ll :: <string> '1.32426,-90.1532' - Latitud y Longitud en forma de string separados por una coma.
        radius :: <integer> 1000 - Radio en metros del punto de búsqueda, MAX = 50,000
        """

        def unravel_yp_dicts(venue):
            data = dict()
            try:
                for key in venue['plus_code'].keys():
                    data[key] = [venue['plus_code'][key]]
            except KeyError:
                pass
            data['latitude'] = [venue['geometry']['location']['lat']]
            data['longitude'] = [venue['geometry']['location']['lng']]
            return data

        endpoint_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            'key': self.api_key,
            'keyword': keyword,
            'radius': radius,
            'location': ll
        }
        if gtype:
            params['type'] = gtype
            
        places = list()
        while True:
            res = requests.get(endpoint_url, params=params).json()
            places.extend(res['results'])
            if res['status'] != 'OK':
                print('\t> Google Places error: ', res['status'])
                return
            if 'next_page_token' in res.keys():
                params['pagetoken'] = res['next_page_token']
                time.sleep(1.8)
                continue
            break
        print('\t> Google Places found: ', len(places))
        if len(places) == 0:
            return
        SQL_query = "SELECT place_id FROM tb_gp_places"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).place_id)
        upload_df = pd.DataFrame(columns=gp_columns_places[:, KEY_NAME])
        for place in places:
            if place['place_id'] not in registered_ids:
                data = row_builder(place, gp_columns_places[:GP_LIMIT, :])
                data.update(unravel_yp_dicts(place))
                upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True)
        upload_df.to_sql('tb_gp_places',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')


class DENUE():
    def __init__(self, dbengine, conn):
        self.dbengine = dbengine
        self.conn = conn
        self.geolocator = Nominatim(user_agent='App de prueba')
        self.claves_entidad = {'entidad':['Aguascalientes',
        'Baja California','Baja California Sur','Campeche','Coahuila',
        'Colima','Chiapas','Chihuahua','Ciudad de México','Durango','Guanajuato',
        'Guerrero','Hidalgo','Jalisco','México','Michoacán','Morelos',
        'Nayarit','Nuevo León','Oaxaca','Puebla','Querétaro','Quintana Roo',
        'San Luis Potosí','Sinaloa','Sonora','Tabasco','Tamaulipas','Tlaxcala',
        'Veracruz','Yucatán','Zacatecas'],
        'clave_ent':['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17',
        '18','19','20','21','22','23','24','25','26','27','28','29','30','31','32']}

    
    """
    Request Status Code:

        200: Everything went okay, and the result has been returned (if any).
        301: The server is redirecting you to a different endpoint. This can happen when a company switches domain names, or an endpoint name is changed.
        400: The server thinks you made a bad request. This can happen when you don’t send along the right data, among other things.
        401: The server thinks you’re not authenticated. Many APIs require login ccredentials, so this happens when you don’t send the right credentials to access an API.
        403: The resource you’re trying to access is forbidden: you don’t have the right permissions to see it.
        404: The resource you tried to access wasn’t found on the server.
        503: The server is not ready to handle the request.

    """

    # BUSCAR
    def Buscar(self,cond,lat,lon,radio):
        """
        Realiza una consulta de todos los establecimientos que cumplan 
        las condiciones definidas.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        cond = str(cond) # Condición (nombre)
        lat = str(lat) # Latitud
        lon = str(lon) # Longitud
        radio = str(radio) # Radio de búsqueda (metros)
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/Buscar/' + cond +'/'+ lat +',' + lon + '/' + radio +'/'+ TOKEN
        
        # Creating JSON an Dataframe
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            jsonObj= requests.get(url).json()
            dfItem = pd.DataFrame.from_records(jsonObj)
            # print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Estatus de la petición: {}'.format(response.status_code))
            print('Algo salió mal con Denue. Intentalo de nuevo')
            dfItem = pd.DataFrame
            jsonObj = {}
            print('No existen resultados para su búsqueda.')
            return
        
        #### Noah Edit ####
        print(f'\t> Denue found: {len(dfItem)}')
        obj = set(list(dfItem.columns) + list(denu_columns_Extract[:, KEY_NAME]))
        to_upload = pd.DataFrame(columns=denu_columns_Extract[:, KEY_NAME])
        to_upload = to_upload.append(dfItem, ignore_index=True)
        to_upload.to_sql("tb_denu_venues",
                         con=self.dbengine,
                         index=False,
                         if_exists='append',
                         method='multi')
        #### ######### ####    
        #return dfItem, jsonObj

    # FICHA
    def Ficha(self,id):
        """
        Obtiene la información de un establecimiento en específico.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        id = str(id) # id del establecimiento
        
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/Ficha/' + id +'/'+ TOKEN
        
        # Creating JSON an Dataframe
        jsonObj= requests.get(url).json()
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Algo salió mal. Intentalo de nuevo')


        return dfItem, jsonObj

    # NOMBRE
    def Nombre(self,NoRS, Ri, Rf,entidad='00'):
        """
        Realiza una consulta de todos los establecimientos por nombre o razón social 
        y puede ser acotado por entidad federativa.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        NoRS = str(NoRS) # Nombre o Razón Social
        entidad = str(entidad) # Entidad Federativa
        Ri = str(Ri) # Registro Inical
        Rf = str(Rf) # Registro Final
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/Nombre/'+NoRS+'/'+entidad+'/'+Ri+'/'+Rf+'/'+TOKEN
        print(url)
        # Creating JSON an Dataframe
        
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            jsonObj= requests.get(url).json()
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Estatus de la petición: {}'.format(response.status_code))
            print('Algo salió mal. Intentalo de nuevo')
            dfItem = pd.DataFrame
            jsonObj = {}
            print('No existen resultados para su búsqueda.')
            
        return dfItem, jsonObj

    # BUSCAR ENTIDAD
    def BuscarEntidad(self,cond, Ri, Rf,entidad=00):
        """
        Realiza una consulta de todos los establecimientos y puede ser acotada por entidad federativa.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        cond = str(cond) # Palabra(s) a buscar
        entidad = str(entidad) # Entidad Federativa
        Ri = str(Ri) # Registro Inical
        Rf = str(Rf) # Registro Final
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarEntidad/' + cond +'/'+ entidad +',' + Ri + '/' + Rf +'/'+ TOKEN
        
        # Creating JSON an Dataframe
        jsonObj= requests.get(url).json()
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Algo salió mal. Intentalo de nuevo')
        return dfItem, jsonObj

    #BUSCAR_AREA_ACOTADA
    def BuscarAreaAct(self,nom,Ri,Rf,entidad=00, muni=0,loc=0,AGEB=0,mza=0,sctr=0,subsctr=0,rama=0,clase=0,id=0 ):
        """
        Realiza una consulta de todos los establecimientos con la opción de acotar la búsqueda por
        área geográfica, actividad económica, nombre y clave del establecimiento.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        nom = str(nom) # Palabra(s) a buscar
        entidad = str(entidad) # Clave de dos dígitos de la entidad federativa (01 a 32), todas = 00.
        muni = str(muni) # Clave de tres dígitos del municipio (ej. 001),todos = 0.
        loc = str(loc) # Clave de cuatro dígitos de la localidad (ej. 0001 ), todas = 0.
        AGEB = str(AGEB) # Clave de cuatro dígitos AGEB(ej. 2000 ),todas = 0.
        mza = str(mza) # Clave de tres dígitos de la manzana (ej. 043 ),todas = 0. 
        sctr = str(sctr) # Clave de dos dígitos del sector de la actividad económica (ej. 46 ),todos = 0.
        subsctr = str(subsctr) # Clave de tres dígitos del subsector de la actividad económica ( ej. 464 ), todos = 0.
        rama = str(rama) # Clave de cuatro dígitos de la rama de la actividad económica (ej. 4641 ),todas = 0.
        clase = str(clase) # Clave de seis dígitos de la clase (ej. 464112 ), todas = 0.

        Ri = str(Ri) # Registro Inical
        Rf = str(Rf) # Registro Final
        id = str(id) # id del establecimiento
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarAreaAct/' + entidad+'/'+ muni +'/'+ loc +'/'+ AGEB +'/' + mza +'/'+ sctr +'/'+ rama +'/'+ clase+'/'+ nom +'/'+ Ri + '/' + Rf +'/'+ id +'/'+ TOKEN
        
        # Creating JSON an Dataframe
        jsonObj= requests.get(url).json()
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Algo salió mal. Intentalo de nuevo')
        return dfItem, jsonObj

    #BUSCAR_AREA_ACOTADA_ ESTR
    def Buscar_Area_ActEstr(self,nom,Ri,Rf,entidad=00, muni=0,loc=0,AGEB=0,mza=0,sctr=0,subsctr=0,rama=0,clase=0,id=0,estr=0):
        """
        Realiza una consulta de todos los establecimientos con la opción de acotar la búsqueda por 
        área geográfica, actividad económica, nombre,clave del establecimiento y estrato.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'
        nom = str(nom) # Palabra(s) a buscar
        entidad = str(entidad) # Clave de dos dígitos de la entidad federativa (01 a 32), todas = 00.
        muni = str(muni) # Clave de tres dígitos del municipio (ej. 001),todos = 0.
        loc = str(loc) # Clave de cuatro dígitos de la localidad (ej. 0001 ), todas = 0.
        AGEB = str(AGEB) # Clave de cuatro dígitos AGEB(ej. 2000 ),todas = 0.
        mza = str(mza) # Clave de tres dígitos de la manzana (ej. 043 ),todas = 0. 
        sctr = str(sctr) # Clave de dos dígitos del sector de la actividad económica (ej. 46 ),todos = 0.
        subsctr = str(subsctr) # Clave de tres dígitos del subsector de la actividad económica ( ej. 464 ), todos = 0.
        rama = str(rama) # Clave de cuatro dígitos de la rama de la actividad económica (ej. 4641 ),todas = 0.
        clase = str(clase) # Clave de seis dígitos de la clase (ej. 464112 ), todas = 0.
        estr = str(estr) # Clave de un dígito del estrato. todos = 0.
        """ 1. Para incluir de 0 a 5 personas.
            2. Para incluir de 6 a 10 personas.
            3. Para incluir de 11 a 30 personas.
            4. Para incluir de 31 a 50 personas.
            5. Para incluir de 51 a 100 personas.
            6. Para incluir de 101 a 250 personas.
            7. Para incluir de 251 y más personas.
        """
        Ri = str(Ri) # Registro Inical
        Rf = str(Rf) # Registro Final
        id = str(id) # id del establecimiento
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/BuscarAreaActEstr/' + entidad+'/'+ muni +'/'+ loc +'/'+ AGEB +'/' + mza +'/'+ sctr +'/'+ rama +'/'+ clase+'/'+ nom +'/'+ Ri + '/' + Rf +'/'+ id +'/'+ estr + '/'+ TOKEN
        
        # Creating JSON an Dataframe
        jsonObj= requests.get(url).json()
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Algo salió mal. Intentalo de nuevo')
        return dfItem, jsonObj

    # CUANTIFICAR
    def Cuantificar(self,act, area=0, estrat=0):
        """
        Realiza un conteo de todos los establecimientos con la opción de acotar la búsqueda por
        área geográfica, actividad económica y estrato.
        """
        TOKEN = '10b813e7-3b7a-4d66-8588-2404e83c7734'

        act = str(act) 
        """
        Clave de dos a cinco dígitos de la actividad económica.
        Para considerar más de una clave deberás separarlas con coma. 
        Para incluir todas las actividades se especifica 0.
        Dos dígitos para incluir nivel sector (ej.46).
        Tres dígitos para incluir nivel subsector (ej. 464).
        Cuatro dígitos para incluir nivel rama (ej. 4641).
        Cinco dígitos para incluir nivel subrama (ej. 46411).
        Seis dígitos para incluir nivel clase (ej. 464111).
        """
        area = str(area)
        """
        Clave de dos a nueve dígitos del área geográfica. 
        Para considerar más de una clave deberás separarlas con coma. 
        Para incluir todo el país se especifica 0.
        Dos dígitos para incluir nivel estatal (ej.01 a 32).
        Cinco dígitos dígitos para incluir nivel municipal (ej. 01001).
        Nueve dígitos para incluir nivel localidad (ej. 010010001).
        """
        estrat = str(estrat)
        """
        Clave de un dígito del estrato. 
        Para incluir todos los tamaños se especifica 0.
        1. Para incluir de 0 a 5 personas.
        2. Para incluir de 6 a 10 personas.
        3. Para incluir de 11 a 30 personas.
        4. Para incluir de 31 a 50 personas.
        5. Para incluir de 51 a 100 personas.
        6. Para incluir de 101 a 250 personas.
        7. Para incluir de 251 y más personas.
        """
        url = 'https://www.inegi.org.mx/app/api/denue/v1/consulta/Cuantificar/'+ act+'/'+ area +'/'+ estrat +'/'
        # Creating JSON an Dataframe
        jsonObj= requests.get(url).json()
        response = requests.get(url)
        status = response.status_code
        if (status == 200): 
            dfItem = pd.DataFrame.from_records(jsonObj)
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Algo salió mal. Intentalo de nuevo')
        return dfItem, jsonObj

    def Extract_denue(self,keyword,entidad,radio):
        near = str(entidad)
        geocod = self.geolocator.geocode(near)
        location = str(geocod.latitude)+','+str(geocod.longitude)
        loc = location.split(',')
        lat = loc[0]
        lon = loc[1]
        clave_entidad = self.claves_entidad['clave_ent'][self.claves_entidad['entidad'].index(entidad)]

        # Método BUSCAR
        dfBuscar, jsonBuscar = self.Buscar(keyword,lat,lon,radio)
        dfNom, jsonNom = self.Nombre(keyword, 1, 50, clave_entidad)
        print(dfNom)
        dfConcat = pd.concat([dfBuscar,dfNom], axis=0, ignore_index=True)
        return  dfConcat

