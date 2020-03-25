# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TripadvisorItem(scrapy.Item):
	# define the fields for your item here like:
	# name = scrapy.Field()
	Nombre  = scrapy.Field()
	Horario = scrapy.Field()
	link = scrapy.Field()
	#calificaciones
	Calificacion  = scrapy.Field()
	top1  = scrapy.Field()
	top2  = scrapy.Field()
	Comida =  scrapy.Field()
	Servicio  = scrapy.Field()
	Calidad = scrapy.Field()
	Ambiente = scrapy.Field()
	#detalles
	detalles_precios = scrapy.Field()
	detalles_tipo_comida = scrapy.Field()
	detalles_comida = scrapy.Field()
	detalles_dietas  = scrapy.Field()
	detalles_caracteristicas  = scrapy.Field()
	#ubicacion
	direccion  = scrapy.Field()
	mail = scrapy.Field()
	telefono = scrapy.Field()
	calle = scrapy.Field()
	pais = scrapy.Field()
	localidad = scrapy.Field()
	direccionExtra = scrapy.Field()
	longitude = scrapy.Field()
	latitude = scrapy.Field()
	#opiniones
	n_opiniones = scrapy.Field()
	opiniones_excelente = scrapy.Field()
	opiniones_muy_bueno = scrapy.Field()
	opiniones_regular = scrapy.Field()
	opiniones_pesimo = scrapy.Field()
	opiniones_malo = scrapy.Field()

class TripadvisorOpinionesItem(scrapy.Item):

	nombre = scrapy.Field()
	autor  = scrapy.Field()
	aportaciones = scrapy.Field()
	calificacacion = scrapy.Field()
	opinion = scrapy.Field()
	opinion2 = scrapy.Field()
	FechaVisita  = scrapy.Field()
	FechaEscritura = scrapy.Field()
