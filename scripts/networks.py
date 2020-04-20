# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 00:20:00 2020

@author: BALAMLAPTOP2
"""

import networkx as nx
import csv
import matplotlib.pyplot as plt
import pandas as pd

tweets = [];
#with open("/home/eris/Dropbox/Red CIE SA Poster/dataset-vdm.csv",'r', encoding='utf8') as f:
with open(r"C:\Users\BALAMLAPTOP2\Documents\GitHub\locfindermx\data\data_tw_alldata.csv",'r', encoding='utf-8-sig') as f:
	for m in csv.reader(f):
		if m not in tweets:
			tweets.append(m);
            
input_data = pd.read_csv('C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/data/data_tw_alldata.csv', encoding='utf-8-sig')
#words = ['Globos de Oro', 'Globos de oro', 'Globo de Oro', 'Oscar', 'Fátima', 'Trump', 'Gaza']
#for word in words:
#    input_data = input_data[~input_data['new_title'].str.contains(word, na=False, regex=False)]
#    input_data = input_data[~input_data['new_content'].str.contains(word, na=False, regex=False)]

# input_data = input_data[input_data['new_content'].isin(['Globos de Oro'])]
# mask = (datalatam['daterep'] > '26/02/2020') & (datalatam['daterep'] <= '30/03/2020')
# datalatam = datalatam.loc[mask]
#“

################################################################################
#  >  HASHTAG - HASHTAG NETWORK 
################################################################################ 

index = 0
for hashtags in input_data['hashtags']:
    print(hashtags)
    
    if pd.notnull(hashtags):
        hashtags = hashtags.split(',')
        if index == 0:
            
            df = pd.DataFrame(hashtags, columns=['hashtags'])
        else: 
            df = df.append(pd.DataFrame(hashtags, columns=['hashtags']))
        index += 1
    
#data_last = df.loc[(df['type'] == 'PER') | (df['type'] == 'ORG')]

nodes = list(dict.fromkeys(df['hashtags']))

index = 0
for hashtags in input_data['hashtags']:
    if pd.notnull(hashtags):
        hashtags = hashtags.split(',')
        if index == 0:
            df2 = pd.DataFrame(hashtags, columns=['hashtags'])
            df2['id'] = index
        else: 
            df3 = pd.DataFrame(hashtags, columns=['hashtags'])
            df3['id'] = index
            df2 = df2.append(df3)
        index += 1
    
idx = list(dict.fromkeys(df2['id']))

G = nx.Graph()
G.add_nodes_from(nodes)

for i in idx:
    data_aux = df2.loc[df2['id'] == i]
    for edgeA in data_aux.iterrows():
        for edgeB in data_aux.iterrows():
            if edgeB[1]['hashtags'] != edgeA[1]['hashtags'] and edgeB[1]['id'] == edgeA[1]['id']:
                G.add_edge(edgeB[1]['hashtags'], edgeA[1]['hashtags'])
            
nx.write_graphml(G,'C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/networks/hashtags_network_tw.graphml')

################################################################################
#  >  ACCOUNT - USER MENTIONS NETWORK 
################################################################################ 


################################################################################
#  >  VENUES - VENUES NETWORK
################################################################################ 

input_data_fs = pd.read_csv('C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/data/Foursquare/tb_frsq_venues.csv', encoding='utf-8-sig')
nodes_fs = list(dict.fromkeys(input_data_fs['name']))
Gfs = nx.Graph()
Gfs.add_nodes_from(nodes_fs)

for edgeA in input_data_fs.iterrows():
    for edgeB in input_data_fs.iterrows():
        if edgeB[1]['name'] != edgeA[1]['name'] and edgeB[1]['rating'] == edgeA[1]['rating']:
            Gfs.add_edge(edgeB[1]['name'], edgeA[1]['name'])

nx.write_graphml(Gfs,'C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/networks/venues_network_fs.graphml')

################################################################################
#  >  BUSINESS - BUSINESS NETWORK
################################################################################ 
input_data_yp = pd.read_csv('C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/data/Yelp/tb_yelp_businesses.csv', encoding='utf-8-sig')

nodes_yp = list(dict.fromkeys(input_data_yp['name']))
Gyp = nx.Graph()
Gyp.add_nodes_from(nodes_yp)

for edgeA in input_data_yp.iterrows():
    for edgeB in input_data_yp.iterrows():
        if edgeB[1]['name'] != edgeA[1]['name'] and edgeB[1]['rating'] == edgeA[1]['rating'] and edgeB[1]['price'] == edgeA[1]['price']:
            Gyp.add_edge(edgeB[1]['name'], edgeA[1]['name'])

nx.write_graphml(Gyp,'C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/networks/business_network_yp.graphml')

################################################################################
#  >  BUSINESS - BUSINESS NETWORK
################################################################################ 
input_data_dn = pd.read_csv('C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/data/Denue/Exctract_DENUE.csv', encoding='utf-8-sig')

nodes_dn = list(dict.fromkeys(input_data_dn['Nombre']))
Gdn = nx.Graph()
Gdn.add_nodes_from(nodes_dn)

for edgeA in input_data_dn.iterrows():
    for edgeB in input_data_dn.iterrows():
        if edgeB[1]['Nombre'] != edgeA[1]['Nombre'] and edgeB[1]['Clase_actividad'] == edgeA[1]['Clase_actividad']:
            Gdn.add_edge(edgeB[1]['Nombre'], edgeA[1]['Nombre'])

nx.write_graphml(Gdn,'C:/Users/BALAMLAPTOP2/Documents/GitHub/locfindermx/networks/empresa_network_dn.graphml')