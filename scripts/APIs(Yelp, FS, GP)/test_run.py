# In[]
from sqlalchemy import create_engine

import locmap_APIs as apis
import configparser
import os
#os.system('clear')


def init_apis(database='local'):
    """
    Opciones: 
        local :: Base de datos local, datos intercambiables
        amazon :: Base de datos tipo postgresql en amazon web service (AWS)
    """
    global fs_client, yp_client, gp_client, dn_client, tb_client
    global dbengine, conn

    config = configparser.ConfigParser()
    config.read("location_mapper.ini")

    # __SQL CONNECTION SETUP__
    user, password, database, host = [
        config[database]['user'],
        config[database]['password'],
        config[database]['database'],
        config[database]['host']]
    dbengine = create_engine(f'postgresql://{user}:{password}@{host}/{database}',
                             echo=False)
    conn = dbengine.connect()

    # API OBJECTS INITIALIZATION
    # Foursquare
    client_id, client_secret, version = [
        config['foursquare']['client_id'],
        config['foursquare']['client_secret'],
        config['foursquare']['version']
    ]
    fs_client = apis.Foursquare(dbengine, conn, client_id, client_secret, version)
    # Yelp
    api_key = config['yelp']['api_key']
    yp_client = apis.Yelp(dbengine, conn, api_key)
    # Google Places
    api_key = config['googleplaces']
    gp_client = apis.GooglePlaces(dbengine, conn, api_key)


def search_apis(near=None, query=None, radius=1000):
    global fs_client, yp_client, gp_client, dn_client, tb_client
    print('Starting Search\nFoursquare...', end='')
    fs_df = fs_client.explore_venuesEP(near=near, query=query, radius=radius)
    print('Finished\nYelp...', end='')
    yp_df = yp_client.business_Tab(term=query, location=near, radius=radius)
    print('Finished\nGoogle Places...', end='')
    gp_df = gp_client.search_places(keyword=query, near=near, radius=radius)
    print('Finished')
    # dn_df = dn_client
    # tb_df = tb_client

    # return pd.concat([fs_df, yp_df], axis=0, ignore_index=True)
    return fs_df


if __name__ == "__main__":
    init_apis('local')
    # editor = apis.TableEditor(conn)
    # editor.erase_tables('all')
    # editor.create_tables('all')

    kwords = [
        'cerveza',
        'bar',
        'food',
        'sushi',
        'cocktail',
        'ramen',
        'restaurante',
        'mezcal',
        'ceviche',
        'alitas',
        'tequila',
        'vino',
        'postres',
        'mariscos',
        'hamburguesas',
        'tacos',
        'botana',
        'pizza',
        'alitas'
    ]

    for kw in kwords:
        search_apis(near='Ciudad de Mexico', query='hamburguesas', radius=2000)

    # Si todo corre bien, la siguiente linea carga los datos a csv por tabla.
    apis.get_table_data()
