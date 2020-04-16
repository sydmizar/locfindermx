import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import csv
import json
import pyproj
from math import sin,cos,sqrt,asin,pi

data_tumblr = pd.read_csv("../../data/Tumblr/OUTCSV_tumblr.csv")

list_nodes = data_tumblr.blog_name.unique()

G_tumblr = nx.MultiGraph()
for inode in list_nodes:
    G_tumblr.add_node(inode)

for ind in range(len(data_tumblr)):
    nodo = data_tumblr.loc[ind]['blog_name']
    ini_tmp = data_tumblr.loc[ind]['tags']
    list_tags = ini_tmp.replace("'",'').strip('][').split(', ')
    for ind2 in range(ind+1, len(data_tumblr)):
        nodo2 = data_tumblr.loc[ind2]['blog_name']
        ini_tmp = data_tumblr.loc[ind2]['tags']
        list_tags2 = ini_tmp.replace("'",'').strip('][').split(', ')
        for itag in list_tags2:
            if itag in list_tags:
                print(nodo, nodo2,itag)
                G_tumblr.add_edge(nodo, nodo2, tag=itag)

nx.draw(G_tumblr, with_labels=True, font_weight='bold')
plt.show()
nx.write_gexf(G_tumblr, 'G_Tumblr.gexf')
#nx.write_graphml(G_tumblr, 'Tumblr.graphml')



data_googlePl = pd.read_csv("../../data/Google Places/tb_gp_places.csv")

#list_nodes = data_googlePl.id.unique()

G_googlePl = nx.MultiGraph()
for index in data_googlePl.index:
    local= json.loads(data_googlePl['geometry'][index])
    lat = local['location']['lat']
    lng = local['location']['lng']
    if data_googlePl['price_level'][index]>0:
        price_level = data_googlePl['price_level'][index]
    else:
        price_level = 0
    G_googlePl.add_node(data_googlePl['id'][index], name = data_googlePl['name'][index],
        price_level=price_level, rating=data_googlePl['rating'][index],
        user_ratings_total=data_googlePl['user_ratings_total'][index],
        lat=lat, lng=lng)

precios = nx.get_node_attributes(G_googlePl,'price_level')
latitud = nx.get_node_attributes(G_googlePl,'lat')
longitud = nx.get_node_attributes(G_googlePl,'lng')
geod = pyproj.Geod(ellps="GRS80")

distancias=[]
for ind in range(len(data_googlePl.index)):
    nodo = data_googlePl['id'][ind]
    ini_tmp =data_googlePl['types'][ind]
    list_tags = ini_tmp.replace('"','').strip('][').split(', ')
    for ind2 in range(ind+1,len(data_googlePl.index)):
        nodo2 =  data_googlePl['id'][ind2]
        ini_tmp = data_googlePl['types'][ind2]
        list_tags2 = ini_tmp.replace('"','').strip('][').split(', ')

        angle1,angle2,distance = geod.inv(longitud[nodo], latitud[nodo], longitud[nodo2], latitud[nodo2])
        if (precios[nodo] == precios[nodo2]) and (distance > 3000): #km
            for itag in list_tags2:
                if itag in list_tags:
                    G_googlePl.add_edge(nodo, nodo2, tag=itag)
                    break

        # angle1,angle2,distance = geod.inv(longitud[nodo], latitud[nodo], longitud[nodo2], latitud[nodo2])
        # for itag in list_tags2:
        #     if itag in list_tags:
        #         #print(nodo, nodo2,itag)
        #         G_googlePl.add_edge(nodo, nodo2, tag=itag, origen_type ='tags')
        # if precios[nodo] == precios[nodo2]:
        #     #print(nodo, nodo2)
        #     G_googlePl.add_edge(nodo, nodo2, price_level=precios[nodo], origen_type ='precios')
        # if distance > 3000: #km
        #     G_googlePl.add_edge(nodo, nodo2, distance=distance, origen_type ='distance')


nx.draw(G_googlePl, font_weight='bold')
plt.show()
nx.write_gexf(G_googlePl, 'googlePlace.gexf')


data_tripadvisor = pd.read_csv("../../data/tripadvisor/tripadvisor.csv")
list_nodes = data_tripadvisor.nombre.unique()

G_tripadvisor = nx.MultiGraph()
for index in data_tripadvisor.index:
    G_tripadvisor.add_node(data_tripadvisor['nombre'][index],
        calificacion = data_tripadvisor['calificacion'][index],
        n_opiniones = data_tripadvisor['n_opiniones'][index],
        precio_trip = data_tripadvisor['preciosimbolo'][index])

precios = nx.get_node_attributes(G_tripadvisor,'precio_trip')
puntaje = nx.get_node_attributes(G_tripadvisor,'calificacion')

for ind in range(len(data_tripadvisor.index)):
    nodo = data_tripadvisor['nombre'][ind]
    ini_tmp = data_tripadvisor['detalles_tipo_comida'][ind]
    list_tags = ini_tmp.replace('"','').strip('][').split(', ')
    for ind2 in range(ind+1,len(data_tripadvisor.index)):
        nodo2 = data_tripadvisor['nombre'][ind2]
        ini_tmp = data_tripadvisor['detalles_tipo_comida'][ind2]
        list_tags2 = ini_tmp.replace('"','').strip('][').split(', ')

        if (precios[nodo] == precios[nodo2]) and (puntaje[nodo] == puntaje[nodo2]): #km
            for itag in list_tags2:
                if itag in list_tags:
                    G_tripadvisor.add_edge(nodo, nodo2, tag=itag)
                    break

nx.draw(G_tripadvisor, font_weight='bold')
plt.show()
nx.write_gexf(G_tripadvisor, 'tripadvisor.gexf')
