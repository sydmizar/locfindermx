# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import psycopg2
from configparser import ConfigParser

from .items import TripadvisorItem, TripadvisorOpinionesItem
tableName = 'Tripadvisor'
tableName2 = 'TripadvisorCometarios'


def config(filename='database.ini', section='postgresql'):
# create a parser
	parser = ConfigParser()
	# read config file
	parser.read(filename)

	# get section, default to postgresql
	db = {}
	if parser.has_section(section):
		params = parser.items(section)
		for param in params:
			db[param[0]] = param[1]
	else:
		raise Exception('Section {0} not found in the {1} file'.format(section, filename))

	return db

def create_table_new(tableName):
	""" create tables in the PostgreSQL database"""
	command =		""" CREATE TABLE """ +tableName+"""(
			ID SERIAL PRIMARY KEY,
			Nombre VARCHAR(255) NOT NULL,
			Horario VARCHAR(255),
			Calificacion float,
			top1 VARCHAR(255),
			top2 VARCHAR(255),
			link VARCHAR(255),
			Comida float,
			Servicio float,
			Calidad float,
			Ambiente float,
			detalles_precios VARCHAR(50),
			detalles_tipo_comida VARCHAR(500),
			detalles_comida VARCHAR(500),
			detalles_dietas VARCHAR(500),
			detalles_caracteristicas VARCHAR(500),
			direccion VARCHAR(500),
			mail VARCHAR(100),
			telefono VARCHAR(20),
			calle VARCHAR(100),
			pais VARCHAR(50),
			localidad VARCHAR(50),
			direccionExtra VARCHAR(500),
			longitude float,
			latitude float,
			n_opiniones int,
			opiniones_excelente int,
			opiniones_muy_bueno int,
			opiniones_regular int,
			opiniones_pesimo int,
			opiniones_malo int
		); """
	conn = None
	try:
		# read the connection parameters
		params = config()
		# connect to the PostgreSQL server
		conn = psycopg2.connect(**params)
		cur = conn.cursor()
		# create table one by one
		cur.execute(command)
		# close communication with the PostgreSQL database server
		cur.close()
		# commit the changes
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()

def create_table_new_Comentarios(tableName):
	""" create tables in the PostgreSQL database"""
	command =		""" CREATE TABLE """ +tableName2+"""(
			ID SERIAL PRIMARY KEY,
			nombre VARCHAR(255) NOT NULL,
			autor VARCHAR(255),
			aportaciones int,
			calificacion float,
			opinion VARCHAR(50),
			opinion2 VARCHAR(500),
			fecha_de_opinion  VARCHAR(500),
			fecha_de_visita VARCHAR(500)
		); """
	conn = None
	try:
		# read the connection parameters
		params = config()
		# connect to the PostgreSQL server
		conn = psycopg2.connect(**params)
		cur = conn.cursor()
		# create table one by one
		cur.execute(command)
		# close communication with the PostgreSQL database server
		cur.close()
		# commit the changes
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()


class TripadvisorPipeline(object):
		def open_spider(self, spider):
			self.conn = None
			try:
				create_table_new(tableName)
				create_table_new_Comentarios(tableName2)
				# read database configuration
				params = config()
				# connect to the PostgreSQL database
				self.conn = psycopg2.connect(**params)
				# create a new cursor
				self.cur = self.conn.cursor()
			except (Exception, psycopg2.DatabaseError) as error:
				print(error)
			# hostname = 'localhost'
			# username = 'cesarf' # the username when you create the database
			# password = 'passNews'
			# database = 'db-News'
			# self.connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
			# self.cur = self.connection.cursor()

		def close_spider(self, spider):
			self.cur.close()
			self.conn.close()

		def process_item(self, item, spider):
			if isinstance(item, TripadvisorItem):
				sql = """INSERT INTO """ +tableName+""" (
					Nombre,
					Horario,
					Calificacion,
					top1,
					top2,
					link,
					Comida,
					Servicio,
					Calidad,
					Ambiente,
					detalles_precios,
					detalles_tipo_comida,
					detalles_comida,
					detalles_dietas,
					detalles_caracteristicas,
					direccion,
					mail,
					telefono,
					calle,
					pais,
					localidad,
					direccionExtra,
					longitude,
					latitude,
					n_opiniones,
					opiniones_excelente,
					opiniones_muy_bueno,
					opiniones_regular,
					opiniones_pesimo,
					opiniones_malo)
					VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING Nombre"""
				new_id = None
				new_name = ( item['Nombre'], item['Horario'], item['Calificacion'], item['top1'], item['top2'], item['link'], item['Comida'],
					item['Servicio'], item['Calidad'], item['Ambiente'], item['detalles_precios'], item['detalles_tipo_comida'],
						item['detalles_comida'], item['detalles_dietas'], item['detalles_caracteristicas'], item['direccion'],
							item['mail'], item['telefono'], item['calle'], item['pais'], item['localidad'], item['direccionExtra'],
							item['longitude'], item['latitude'], item['n_opiniones'], item['opiniones_excelente'], item['opiniones_muy_bueno'],
							item['opiniones_regular'], item['opiniones_pesimo'], item['opiniones_malo'])
				#print(sql,new_name)
				self.cur.execute(sql, new_name)
				# get the generated id back
				new_id = self.cur.fetchone()[0]
				print(new_id)
				# commit the changes to the database
				self.conn.commit()
				return item

			if isinstance(item, TripadvisorOpinionesItem):
				sql = """INSERT INTO """ +tableName+""" (
					nombre,
					autor,
					aportaciones,
					calificacion,
					opinion,
					opinion2,
					fecha_de_opinion,
					fecha_de_visita)
					VALUES(%s, %s, %s, %s, %s, %s, %s, %s) RETURNING nombre"""
				new_id = None
				new_opinion = ( item['nombre'], item['autor'], item['aportaciones'],
					item['calificacacion'], item['opinion'], item['opinion2'],
					item['FechaVisita'], item['FechaEscritura'])
				#print(sql,new_name)
				self.cur.execute(sql, new_opinion)
				# get the generated id back
				new_id = self.cur.fetchone()[0]
				print(new_id)
				# commit the changes to the database
				self.conn.commit()
				return item
