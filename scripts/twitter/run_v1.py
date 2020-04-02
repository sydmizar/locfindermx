# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 00:50:49 2020

@author: BALAMLAPTOP2
"""

from sqlalchemy import create_engine
import locfin_APIS_v1 as apis


def init_apis(database='local'):
    """
    Opciones:
        local :: Base de datos local, datos intercambiables
        amazon :: Base de datos tipo postgresql en amazon web service (AWS)
    """
    global tw_client
    global dbengine, conn

    # __SQL CONNECTION SETUP__
    if database == 'local':
        user = "postgres"
        password = "postgres"
        database = "postgres"
        host = "127.0.0.1"

    elif database == 'amazon':
        user = "pgcslab"
        password = "Cslab1y."
        database = "postgres"
        host = "cslab.cokyn0ewjjku.us-east-2.rds.amazonaws.com"

    dbengine = create_engine(f'postgresql://{user}:{password}@{host}/{database}',
                             echo=False)
    conn = dbengine.connect()

    # API OBJECTS INITIALIZATION

    # Foursquare
    API_KEY = '5Hqg6JTZ0cC89hUThySd5yZcL'
    API_SECRET = 'Ncp1oi5tUPbZF19Vdp8Jp8pNHBBfPdXGFtXqoKd6Cqn87xRj0c'
    TOKEN_KEY = '3272304896-ZTGUZZ6QsYKtZqXAVMLaJzR8qjrPW22iiu9ko4w'
    TOKEN_SECRET = 'nsNY13aPGWdm2QcgOl0qwqs5bwLBZ1iUVS2OE34QsuR4C'
    tw_client = apis.Twitter(dbengine, conn, API_KEY, API_SECRET, TOKEN_KEY, TOKEN_SECRET)

#    # Yelp
#    api_key = "xE7CYuOc8w6Nx8gIIaHqn98bO9VJv4oiWDviCpDAg7skiAa_q4fDnfbSM2v5RfKd6xVqvJ0VJ9OiN1KhE4HUMmX3tEYmHc4a7Z2PtNYBQynSQAdwU0bLwVPV7vg5XnYx"
#    yp_client = apis.Yelp(dbengine, conn, api_key)
#
#    # Google Places
#    api_key = "AIzaSyBKsZ5sZW_1VouFlIxGGeZgCUDjPAG_6sI"
#    gp_client = apis.GooglePlaces(dbengine, conn, api_key)
#
#    # Denue
#    token = "10b813e7-3b7a-4d66-8588-2404e83c7734sebasti"
#    # dn_client = 


def search_apis(near=None, query=None, radius=1000):
    global tw_client, yp_client, gp_client, dn_client

    tw_client.search_tweets(keyword=query, near=near, radius=radius)
#    yp_client.business_Tab(term=query, location=near, radius=radius)
#    gp_client.search_places(keyword=query, near=near, radius=radius)


if __name__ == "__main__":
    init_apis('local')
    editor = apis.TableEditor(conn)
    # editor.erase_tables('all')
    editor.create_tables('all')
    search_apis(near='Ciudad de Mexico', query='', radius=2000)

#    kwords = [
#        'cerveza',
#        'bar',
#        'food',
#        'sushi',
#        'cocktail',
#        'ramen',
#        'restaurante',
#        'mezcal',
#        'ceviche',
#        'alitas',
#        'tequila',
#        'vino',
#        'postres',
#        'mariscos',
#        'hamburguesas',
#        'tacos',
#        'botana',
#        'pizza',
#        'alitas'
#    ]
#
#    for kw in kwords:
        

    # Si todo corre bien, la siguiente linea carga los datos a csv por tabla.
    editor.alltables2csv()
