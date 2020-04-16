from sqlalchemy import create_engine
import sys
import locmap_APIs_v4 as apis


def init_apis(database='local'):
    """
    Opciones:
        local :: Base de datos local, datos intercambiables
        amazon :: Base de datos tipo postgresql en amazon web service (AWS)
    """
    global fs_client, yp_client, gp_client, dn_client
    global dbengine, conn

    # __SQL CONNECTION SETUP__
    if database == 'local':
        user = "postgres"
        password = "sselhtaed"
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
    client_id = "4ZK4GZQS4N31KZX20GOSQZHV0TH5OHOT5014NQTVLDWDVP3J"
    client_secret = "2LDTDYXDL32UIQITBO20XY4VUA3HQHYX2ACBGA4WJ3AP5NZ0"
    version = "20200228"
    fs_client = apis.Foursquare(dbengine, conn, client_id, client_secret, version)

    # Yelp
    api_key = "xE7CYuOc8w6Nx8gIIaHqn98bO9VJv4oiWDviCpDAg7skiAa_q4fDnfbSM2v5RfKd6xVqvJ0VJ9OiN1KhE4HUMmX3tEYmHc4a7Z2PtNYBQynSQAdwU0bLwVPV7vg5XnYx"
    yp_client = apis.Yelp(dbengine, conn, api_key)

    # Google Places
    api_key = "AIzaSyBKsZ5sZW_1VouFlIxGGeZgCUDjPAG_6sI"
    gp_client = apis.GooglePlaces(dbengine, conn, api_key)

    # Denue
    token = "10b813e7-3b7a-4d66-8588-2404e83c7734sebasti"
    dn_client = apis.DENUE(dbengine, conn)


def search_apis(near=None, query=None, radius=1000):
    global fs_client, yp_client, gp_client, dn_client

    lat, lon = apis.get_latlon(near)
    ll = ','.join([str(lat), str(lon)])
    print('\t Coordinates given: lat[', lat, '] lon[', lon, ']')

    fs_client.explore_venuesEP(near=near, query=query, radius=radius)
    yp_client.business_Tab(term=query, location=near, radius=radius)
    gp_client.search_places(keyword=query, ll=ll, radius=radius)
    dn_client.Buscar(cond=query, lat=lat, lon=lon, radio=radius)


if __name__ == "__main__":
    init_apis('local')
    editor = apis.TableEditor(conn)
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
        print('\nBuscando: ',kw)
        search_apis(near='Ciudad de Mexico', query=kw, radius=2000)

    # Si todo corre bien, la siguiente linea carga los datos a csv por tabla.
    # editor.alltables2csv()