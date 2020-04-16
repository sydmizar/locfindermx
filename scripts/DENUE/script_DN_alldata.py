#!/usr/bin/env python
# -*- coding: utf8 -*-

# In[]
# _____DENUE API:   ______#

# Autor:
#      ° Palomino Alan Jesús

#_____________ LIBRARIES ______________________#
"TOKEN: 10b813e7-3b7a-4d66-8588-2404e83c7734"
import requests
import json
import pandas as pd
from geopy.geocoders import Nominatim
from sqlalchemy import create_engine

geolocator = Nominatim(user_agent='App de prueba')

def get_ll(near):
    geocod = geolocator.geocode(near)
    return geocod.latitude, geocod.longitude

#_____________ DEFINITIONS ____________________#
class DENUE():
    def __init__(self,dbengine):
        self.dbengine = dbengine
        self.conn = self.dbengine.connect()
        self.geolocator = Nominatim(user_agent='App de prueba')
        self.claves_entidad ={'entidad':['Aguascalientes',
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
            print('Estatus de la petición: {}'.format(response.status_code))
        else:
            print('Estatus de la petición: {}'.format(response.status_code))
            print('Algo salió mal. Intentalo de nuevo')
            dfItem = pd.DataFrame
            jsonObj = {}
            print('No existen resultados para su búsqueda.')
            
        return dfItem, jsonObj

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
        """1. Para incluir de 0 a 5 personas.
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
        return jsonObj, dfItem

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


# Creating Engine
# dbengine=create_engine("postgres://{user}:{password}@{host}/{database}".format(user="pgcslab",password="Cslab1y.",host="cslab.cokyn0ewjjku.us-east-2.rds.amazonaws.com",database="postgres"),echo=True)
dbengine=create_engine("postgres://{user}:{password}@{host}/{database}".format(user="alanjr.palomino@gmail.com",password="alanjr",host="127.0.0.1",database="postgres"),echo=True)
ObjetoDenue = DENUE(dbengine)

# Test Area
DF = ObjetoDenue.Extract_denue('camiones','Ciudad de México',1000)
DF.to_sql('tb_denue_locals', con=dbengine,if_exists='append',index=False, sort=False)

