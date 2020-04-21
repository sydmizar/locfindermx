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

tablas =[]
pathlist = Path('tablas').glob('**/*.csv')
for path in pathlist:
     path_in_str = str(path)
     data= pd.read_csv(path_in_str)
     tablas.append(data)
     print(path_in_str)

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

     df = pd.DataFrame (data, columns = ['Parámetros','Valor'])
     df.to_excel("Stats_Frsq.xlsx")

def DENUE():

     # _______________ DENUE _____________________
     dn = tablas[4]
     param= []
     val=[]

     # Lugares Encontrados
     param.append('Establecimientos Encontrados')
     val.append(dn['Id'].count())

     # Nombres Encontrados
     freqDist = FreqDist(dn['Nombre'].dropna())
     param.append('Nombre de Establecimiento más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])
     # RS de Mayor Frecuencia
     freqDist = FreqDist(dn['Razon_social'].dropna())
     param.append('Razón Social más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # CA de Mayor Frecuencia
     freqDist = FreqDist(dn['Clase_actividad'].dropna())
     param.append('Clase o Actividad más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Estrato de Mayor Frecuencia
     freqDist = FreqDist(dn['Estrato'].dropna())
     param.append('Estrato de Mayor más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Tipo de Vialidad de Mayor Frecuencia
     freqDist = FreqDist(dn['Tipo_vialidad'].dropna())
     param.append('Tipo de Vialidad más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Colonia de Mayor Frecuencia
     freqDist = FreqDist(dn['Colonia'].dropna())
     param.append('Colonia más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Tipo de Mayor Frecuencia
     freqDist = FreqDist(dn['Tipo'].dropna())
     param.append('Tipo de establecimiento más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Centro Comercial de Mayor Frecuencia
     freqDist = FreqDist(dn['CentroComercial'].dropna())
     param.append('Centro Comercia más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Tipo de Centro Comercial de Mayor Frecuencia
     freqDist = FreqDist(dn['TipoCentroComercial'].dropna())
     param.append('Tipo de Centro Comercia más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Corredor Industrial de Mayor Frecuencia
     freqDist = FreqDist(dn['nom_corredor_industrial'])
     param.append('Corredor Industrial más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Tipo de Centro Comercial de Mayor Frecuencia
     freqDist = FreqDist(dn['tipo_corredor_industrial'])
     param.append('Tipo de Corredor Industrial más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Data Frame Exportado
     data = {'Parámetros':  param,'Valor': val}
     df = pd.DataFrame (data, columns = ['Parámetros','Valor'])
     df.to_excel("Stats_Denue.xlsx")

def GP():
     # _______________ Google Places _____________________
     gp = tablas[5]
     param= []
     val=[]

     # Lugares Encontrados
     param.append('Establecimientos Encontrados')
     val.append(gp['id'].count())

     # Nombres Encontrados
     freqDist = FreqDist(gp['name'].dropna())
     param.append('Nombre de Establecimiento más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Rating de Precio más frecuente
     freqDist = FreqDist(gp['price_level'].dropna())
     param.append(' Nivel de Precio más Frecuente')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])
          
     # Nivel  de Precio
     param.append('Mayor Nivel de Precio')
     val.append(gp['price_level'].dropna().max())

     param.append('Menor Nivel de Precio')
     val.append(gp['price_level'].dropna().min())

     param.append('Nivel de Precio Promedio ')
     val.append(gp['price_level'].dropna().mean())

     # Ratings de lugar
     param.append('Mayor Califiación Asignada')
     val.append(gp['rating'].dropna().max())

     param.append('Menor Calificación Asignada')
     val.append(gp['rating'].dropna().min())

     param.append('Promedio de Calificaciones')
     val.append(gp['rating'].dropna().mean())

     # Ratings de por usuario
     param.append('Mayor Num de calificaciones  por usuario')
     val.append(gp['user_ratings_total'].dropna().max())

     param.append('Menor Num de calificaciones  por usuario')
     val.append(gp['user_ratings_total'].dropna().min())

     param.append('Promedio de Calificaciones por usuario')
     val.append(gp['user_ratings_total'].dropna().mean())

     # Tipo de Vialidad de Mayor Frecuencia
     freqDist = FreqDist(gp['types'].dropna())
     param.append('Tags más Frecuentes')
     val.append(max(freqDist.items(), key=operator.itemgetter(1))[0])

     # Data Frame Exportado
     data = {'Parámetros':  param,'Valor': val}
     df = pd.DataFrame (data, columns = ['Parámetros','Valor'])
     df.to_excel("Stats_GPlaces.xlsx")

def main():
     YELP()
     FRSQ()
     DENUE()
     GP()
     pass
# %%
# __________ Main____________
main()

