from global_vars import *
from pprint import pprint
import sqlalchemy
import pandas as pd
import numpy as np
import requests
import json
import sys

################################################################################
#  >  TABLE EDITOR
################################################################################


class TableEditor():
    def __init__(self, conn):
        self.conn = conn
        self.tables_content = {
            'tb_frsq_venues': frsq_columns_venues,
            'tb_frsq_tips': frsq_columns_tips,
            'tb_frsq_photos': frsq_columns_photos,
            'tb_frsq_lists': frsq_columns_lists,
            'tb_yelp_businesses': columns,
            'tb_yelp_reviews': columns1,
            'tb_tmbl_users': tmbl_columns_User, 
            'tb_tmbl_info': tmbl_columns_blog_Info,
            'tb_tmbl_tags': tmbl_columns_blog_tags,
            'tb_gp_places': gp_column_places
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
        if apis == 'all' or 'tumblr' in apis:
            self.create_table('tb_tmbl_users')
            self.create_table('tb_tmbl_info')
            self.create_table('tb_tmbl_tags')
        if apis == 'all' or 'googleplaces' in apis:
            self.create_table('tb_gp_places')

    def alltables2csv():
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
        if apis == 'all' or 'googleplaces' in apis:
            self.conn.execute('DROP TABLE IF EXISTS tb_gp_places')


################################################################################
#  >  GENERAL USE FUNCTIONS DEFINITION
################################################################################


def get_table_data(conn, primary_keys, table_name):
    SQL_query = "SELECT * FROM {table} WHERE id IN ('{keys}')".format(table=table_name,
                                                                      keys="','".join(primary_keys))
    df = pd.read_sql_query(SQL_query, conn)
    return df


def get_latlon(near):
    geocod = geolocator.geocode(near)
    return geocod.latitude, geocod.longitude


def make_row(input, columns):
    pass

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
            try:
                res = requests.get(endpoint_url, params=params).json()
                for item in res['response']['groups'][0]['items']:
                    if item['venue']['id'] not in venues_id:
                        venues_id.append(item['venue']['id'])
                if len(res['response']['groups'][0]['items']) < 50:
                    break
                params['offset'] = 50
            except KeyError:
                pprint(res)
                raise KeyError
        print('\tFS Venues Loaded')
        display_df = self.venues_DFbuilder(venues_id)
        self.lists_DFbuilder(venues_id)
        self.tips_DFbuilder(venues_id)
        self.photos_DFbuilder(venues_id)
        return display_df

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
            return requests.get(endpoint_url, params=params).json()['response']

        display_df = pd.DataFrame(columns=frsq_columns_venues[:, KEY_NAME])
        SQL_query = "SELECT id FROM tb_frsq_venues"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        request_list = list()
        for venue_id in venue_list:
            print(f'Venue with ID {venue_id} obtained from: ', end='')
            venue = details_venueEP(venue_id)
            if venue_id not in registered_ids:
                data = dict()
                for sec in frsq_columns_venues:
                    try:
                        if type(sec[NULL_VAL]) in [dict, list]:
                            data[sec[KEY_NAME]] = json.dumps(venue['venue'][sec[KEY_NAME]])
                        else:
                            # data.append(venue['venue'][sec[KEY_NAME]])
                            data[sec[KEY_NAME]] = [venue['venue'][sec[KEY_NAME]]]
                    except KeyError:
                        # data.append(None)
                        data[sec[KEY_NAME]] = [sec[NULL_VAL]]
                display_df = display_df.append(pd.DataFrame(data), ignore_index=True)
                print('Foursquare')
            else:
                request_list.append(venue_id)
                print('Database')
        # Upload new Venues to Database
        if len(display_df) < 0:
            display_df.to_sql('tb_frsq_venues',
                              con=self.engine,
                              index=False,
                              if_exists='append',
                              method='multi')
        # Get Venues from Database
        display_df = display_df.append(other=get_table_data(conn=self.conn,
                                                            primary_keys=request_list,
                                                            table_name='tb_frsq_venues'),
                                       ignore_index=True)
        return display_df

    def lists_DFbuilder(self, venue_list):

        def list_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/listed"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'v': version,
                'group': 'other',
                'limit': 30,
                'offset': offset
            }
            return requests.get(endpoint_url, params=params).json()['response']

        for venue_id in venue_list:
            offset, lists = 0, list()
            while True:
                res = list_endpoint(venue_id, offset)
                lists.extend(res['lists']['items'])
                if res['lists']['count'] == 30:
                    offset += 30
                    continue
                break

            SQL_query = "SELECT id FROM tb_frsq_lists"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_lists[:, KEY_NAME])
            for listed in lists:
                listed['venue'] = venue_id
                data = dict()
                for sec in frsq_columns_lists:
                    try:
                        if type(sec[NULL_VAL]) in [list, dict]:
                            data[sec[KEY_NAME]] = json.dumps(listed[sec[KEY_NAME]])
                        else:
                            data[sec[KEY_NAME]] = [listed[sec[KEY_NAME]]]
                    except KeyError:
                        data[sec[KEY_NAME]] = [sec[NULL_VAL]]
                if data['id'] not in registered_ids:
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql('tb_frsq_lists',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')

    def tips_DFbuilder(self, venue_list):

        def tips_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/tips"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'v': version,
                'sort': 'recent',
                'limit': 500,
                'offset': offset
            }
            return requests.get(endpoint_url, params=params).json()['response']

        for venue_id in venue_list:
            offset, tips = 0, list()
            while True:
                res = tips_endpoint(venue_id, offset)
                tips.extend(res['tips']['items'])
                if res['tips']['count'] == 500:
                    offset += 500
                    continue
                break

            SQL_query = "SELECT id FROM tb_frsq_tips"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_tips[:, KEY_NAME])
            for listed in tips:
                listed['venue'] = venue_id
                data = dict()
                for sec in frsq_columns_tips:
                    try:
                        if type(sec[NULL_VAL]) in [list, dict]:
                            data[sec[KEY_NAME]] = json.dumps(listed[sec[KEY_NAME]])
                        else:
                            data[sec[KEY_NAME]] = [listed[sec[KEY_NAME]]]
                    except KeyError:
                        data[sec[KEY_NAME]] = [sec[NULL_VAL]]
                if data['id'] not in registered_ids:
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql('tb_frsq_tips',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')

    def photos_DFbuilder(self, venue_list):

        def photos_endpoint(venue_id, offset=0):
            endpoint_url = f"https://api.foursquare.com/v2/venues/{venue_id}/photos"
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'v': version,
                'limit': 200,
                'offset': offset
            }
            return requests.get(endpoint_url, params=params).json()['response']

        for venue_id in venue_list:
            offset, photos = 0, list()
            while True:
                res = photos_endpoint(venue_id, offset)
                photos.extend(res['photos']['items'])
                if res['photos']['count'] == 200:
                    offset += 200
                    continue
                break

            SQL_query = "SELECT id FROM tb_frsq_photos"
            registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
            upload_df = pd.DataFrame(columns=frsq_columns_photos[:, KEY_NAME])
            for listed in photos:
                listed['venue'] = venue_id
                data = dict()
                for sec in frsq_columns_photos:
                    try:
                        if type(sec[NULL_VAL]) in [list, dict]:
                            data[sec[KEY_NAME]] = json.dumps(listed[sec[KEY_NAME]])
                        else:
                            data[sec[KEY_NAME]] = [listed[sec[KEY_NAME]]]
                    except KeyError:
                        data[sec[KEY_NAME]] = [sec[NULL_VAL]]
                if data['id'] not in registered_ids:
                    upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True, sort=False)
        upload_df.to_sql('tb_frsq_photos',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')


class Yelp():
    def __init__(self, dbengine, conn, api_key):
        self.dbengine = dbengine
        self.conn = conn
        self.api_key = api_key

    def business_Tab(self, term='bar', limit=50, location='Guadalajara', offset=0, radius=10000):
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
        Business_DF = pd.DataFrame(columns=yelp_columns)
        to_upload = pd.DataFrame(columns=yelp_columns)
        for business in business_Tab:
            fila = dict()
            for sec in yelp_columns:
                try:
                    if type(sec[NULL_VAL]) is dict or type(sec[NULL_VAL]) is list:
                        fila[sec[KEY_NAME]] = json.dumps(business[sec[KEY_NAME]])
                    else:
                        fila[sec[KEY_NAME]] = business[sec[KEY_NAME]]
                except KeyError:
                    fila[sec[KEY_NAME]] = [sec[NULL_VAL]]
            row = pd.DataFrame(fila)
            if row.id not in registered_ids:
                to_upload = to_upload.append(row, ignore_index='True')
            Business_DF = Business_DF.append(row, ignore_index='True')
        to_upload.to_sql("tb_yelp_businesses",
                         con=self.dbengine,
                         index=False,
                         if_exists='append',
                         method='multi')

        self.review_Tab(id_tab)
        return Business_DF

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
        reviews_DF = pd.DataFrame(columns=yelp_columns1)
        to_upload = pd.DataFrame(columns=yelp_columns1)
        for reviews, bussid in zip(reviews_tab, id_tab):
            for review in reviews['reviews']:
                fila = {'businessid': bussid}
                for sec in yelp_columns1[1:]:
                    try:
                        if type(sec[NULL_VAL]) is dict or type(sec[NULL_VAL]) is list:
                            fila[sec[KEY_NAME]] = json.dumps(review[sec[KEY_NAME]])
                        else:
                            fila[sec[KEY_NAME]] = review[sec[KEY_NAME]]
                    except KeyError:
                        fila[sec[KEY_NAME]] = [sec[NULL_VAL]]
                row = pd.DataFrame(fila)
                reviews_DF = reviews_DF.append(row1, ignore_index='True')

        reviews_DF.to_sql("tb_yelp_reviews",
                          con=self.dbengine,
                          if_exists='append',
                          method='multi')


class GooglePlaces():
    def __init__(self, dbengine, conn, api_key="AIzaSyBKsZ5sZW_1VouFlIxGGeZgCUDjPAG_6sI"):
        self.api_key = api_key
        self.engine = dbengine
        self.conn = conn

    def search_places(self, keyword, gtype=None, ll=None, near=None, radius=1000):
        """
        type :: <string> 'restaurant' - Restringe la búsqueda a quienes cumplan con el tipo.
        ll :: <string> '1.32426,-90.1532' - Latitud y Longitud en forma de string separados por una coma.
        radius :: <integer> 1000 - Radio en metros del punto de búsqueda, MAX = 50,000
        """

        endpoint_url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            'key': self.api_key,
            'keyword': keyword,
            'radius': radius
        }
        if gtype:
            params['type'] = gtype
        if near:
            lat, lon = get_latlon(near)
            params['location'] = f'{lat},{lon}'
        elif ll:
            params['location'] = ll
        else:
            print('Debe definir una ubicación para la búsqueda.')
            raise NoLocationDefined

        places = list()
        while True:
            try:
                res = requests.get(endpoint_url, params=params).json()
                places.extend(res['results'])
                params['pagetoken'] = res['next_page_token']
            except KeyError:
                break
        SQL_query = "SELECT id FROM tb_gp_places"
        registered_ids = list(pd.read_sql_query(SQL_query, self.conn).id)
        upload_df = pd.DataFrame(columns=gp_columns_places[:, KEY_NAME])
        for place in places:
            if place['id'] not in registered_ids:
                data = dict()
                for sec in gp_columns_places:
                    try:
                        if type(sec[NULL_VAL]) in [list, dict]:
                            data[sec[KEY_NAME]] = json.dumps(place[sec[KEY_NAME]])
                        else:
                            data[sec[KEY_NAME]] = [place[sec[KEY_NAME]]]
                    except KeyError:
                        data[sec[KEY_NAME]] = [sec[NULL_VAL]]
                upload_df = upload_df.append(pd.DataFrame(data), ignore_index=True)
        print(upload_df)
        upload_df.to_sql('tb_gp_places',
                         con=self.engine,
                         index=False,
                         if_exists='append',
                         method='multi')
