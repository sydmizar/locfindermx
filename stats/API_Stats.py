"""
#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
# In[]
# _____API General Statistics by Region  ______#

# Autor:
#      ° Palomino Alan Jesús

#_____________ LIBRARIES ______________________#
import sys, string
import pandas as pd
import numpy as np
from pathlib import Path
from nltk import ngrams, FreqDist
import operator
from nltk.tokenize import RegexpTokenizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
np.set_printoptions(threshold=sys.maxsize)
# %%
tablas =[]
pathlist = Path('tablas').glob('**/*.csv')
for path in pathlist:
     path_in_str = str(path)
     data= pd.read_csv(path_in_str)
     tablas.append(data)
     print(path_in_str)


# %%
def YELP():
     # _______________ YELP Bussiness_____________________
     ylp = tablas[0]
     param= []
     val=[]
     # Lugares Encontrados
     number = ylp['id'].count()
     param.append('Lugares Encontrados')
     val.append(number)

     # Total de Reviews encontrados
     Total_reviews = ylp['review_count'].sum()
     param.append('Total de Reviews encontrados')
     val.append(Total_reviews)

     # Zona de Mayor Frecuencia
     region = ylp['city']
     freqDist = FreqDist(region)
     param.append('Zona de Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Rating de Precio más frecuente
     freqDist = FreqDist(ylp['price'].dropna())
     param.append('Price-Rating de Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Categorías más frecuentes
     freqDist = FreqDist(ylp['categories'].dropna())
     param.append('Categorías de Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Tag más frecuente
     lists= ' '.join(ylp['categories'].dropna())
     tokenizer = RegexpTokenizer(r'\w+')
     words = tokenizer.tokenize(lists)
     stop = ['alias', 'title']
     filtered_words = [w for w in words if w not in stop]
     freqDist = FreqDist(filtered_words)
     param.append('Tag de Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # _________________YELP Reviews_________________________
     yelp_rev = tablas[6]

     # Reviews Encontrados
     param.append('Reseñas Encontradas')
     val.append(int(yelp_rev ['id'].count()))
     # Polaridad Compuesta de Review
     revs = yelp_rev['text']
     sid = SentimentIntensityAnalyzer()
     polarities = [sid.polarity_scores(review) for review in revs]
     comp = [polarities[i]['compound'] for i in range(len(polarities))]

     param.append('Mayor polaridad compuesta de Reseña')
     val.append(max(comp))

     param.append('Menor  polaridad compuesta de Reseña')
     val.append(min(comp))

     param.append('Promedio de  polaridad compuesta de Reseña')
     val.append(np.mean(comp))

     #Fecha de Creación
     param.append('Reseña más Reciente')
     val.append(yelp_rev['time_created'].max())

     param.append('Reseña más Antigua')
     val.append(yelp_rev['time_created'].min())


     # Data Frame Exportado
     data = {'Parámetros':  param,'Valor': val}

     df = pd.DataFrame (data, columns = ['Parámetros','Valor'])
     print(df)
     df.to_excel("Stats_Yelp.xlsx")
     

# %%
def FRSQ():
     # ______________ FOURSQUARE_photo_____________________
     fs_ph = tablas[1]
     
     param= []
     val=[]
     # Usuarios  Encontrados
     param.append('Usuarios Encontrados')
     val.append(fs_ph['id'].count())

     param.append('Fotos')
     val.append(fs_ph['venue'].count())

     # ______________ FOURSQUARE_venues_____________________

     fs_venues = tablas[2]

     # Lugares Encontrados
     param.append('Total Lugares Encontrados')
     val.append(fs_venues['id'].count())

     # Lugar con Mayor frecuencia
     plc = fs_venues['name']
     freqDist = FreqDist(plc)
     param.append('Lugar con Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Ratings de lugar
     param.append('Mayor Califiación Asignada')
     val.append(fs_venues['rating'].max())

     param.append('Menor Calificación Asignada')
     val.append(fs_venues['rating'].min())

     param.append('Promedio de Calificaciones')
     val.append(fs_venues['rating'].mean())

     # Descripción de Lugares
     param.append('Lugares que cuentan con Descripción')
     val.append(fs_venues['description'].dropna().count())

     # Zona de Mayor Frecuencia
     region = fs_venues['city']
     freqDist = FreqDist(region)
     param.append('Zona de Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Conteo de tips de lugar
     param.append('Mayor Conteo de tips')
     val.append(fs_venues['tipCount'].max())

     param.append('Menor Conteo de tips')
     val.append(fs_venues['tipCount'].min())

     param.append('Promedio de Conteo de tips')
     val.append(fs_venues['tipCount'].mean())

     # Conteo de likes
     param.append('Mayor Conteo de likes')
     val.append(fs_venues['likeCount'].max())

     param.append('Menor Conteo de likes')
     val.append(fs_venues['likeCount'].min())

     param.append('Promedio de likes')
     val.append(fs_venues['likeCount'].mean())

     # ______________ FOURSQUARE_lists_____________________

     fs_lists = tablas[3]

     # Lugares  Encontrados
     param.append('Lugares Encontrados')
     val.append(fs_lists['id'].count())

     # Lugar con Mayor frecuencia
     freqDist = FreqDist(fs_lists['name'])
     param.append('Palabra incluida con Mayor Frecuencia')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Data Frame Exportado
     data = {'Parámetros':  param,'Valor': val}

     df2 = pd.DataFrame (data, columns = ['Parámetros','Valor'])
     df2.to_excel("Stats_Frsq.xlsx")



# %%

# %%
data = {'Parámetros':  param,'Valor': val}

df3 = pd.DataFrame (data, columns = ['Parámetros','Valor'])
#df2.to_excel("Stats_Frsq.xlsx")

df3

# %%
YELP()


# %%
