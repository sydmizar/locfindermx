from sqlalchemy import create_engine
import locmap_APIs_v2 as apis
import configparser
import os
from global_vars import gp_column_places
import pandas as pd
#os.system('clear')


def init_apis_nomas(database='local'):
    global fs_client, yp_client, gp_client

    # API OBJECTS INITIALIZATION
    # Foursquare
    # fs_client = apis.Foursquare(dbengine, conn, client_id, client_secret, version)
    # Yelp
    # yp_client = apis.Yelp(dbengine, conn, api_key)
    # Google Places
    gp_client = apis.GooglePlaces()


def search_apis(near=None, query=None, radius=1000):
    global fs_client, yp_client, gp_client, dn_client, tb_client

    # fs_df = fs_client.explore_venuesEP(near=near, query=query, radius=radius)
    # yp_df = yp_client.business_Tab(term=query, location=near, radius=radius)
    gp_df = gp_client.search_places(keyword=query, near=near, radius=radius)
    return gp_df


if __name__ == "__main__":
    init_apis_nomas('local')

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

    df=pd.DataFrame(columns=gp_column_places[:, 0])
    for kw in kwords:
        print(kw)
        df=df.append(search_apis(near='Ciudad de Mexico', query=kw, radius=2000))
    
    
    df.to_csv('/home/juanripper/Downloads/data.csv',index=False)

