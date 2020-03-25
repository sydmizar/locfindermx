import scrapy
from scrapy.selector import Selector
from scrapy.http.request import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import locale
locale.setlocale(locale.LC_TIME, '')
from datetime import datetime
import html2text

import requests
from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scrapy_splash import SplashRequest


options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1200x600')
driver = webdriver.Chrome(chrome_options=options)

from ..items import TripadvisorItem, TripadvisorOpinionesItem

def process_request(request):
	driver.get(request.url)
	print(request.url)
	WebDriverWait(driver, 10).until(
		EC.presence_of_element_located((By.XPATH, '//div[@class="search-results-list"]'))
		)

	try:
		driver.find_element_by_xpath('//div[@class="show-block show-more"]').click()
	except Exception as e:
		print(e)

	butonPages = driver.find_elements_by_xpath('//div[@class="ui_pagination is-centered"]/a')
	classBoton = butonPages[1].get_attribute('class')

	linkNew = []
	while classBoton == 'ui_button nav next primary ':
		articulos  = driver.find_elements_by_xpath('//div[@class="result-title"]')
		for inew in articulos:
			linkPage = inew.get_attribute("onclick")
			if isinstance(linkPage, str):
				parametros = linkPage.split(',')
				linkNew.append(parametros[3][2:-1] )

		butonPages = driver.find_elements_by_xpath('//div[@class="ui_pagination is-centered"]/a')
		butonPages[1].click()

		WebDriverWait(driver, 10).until(
			EC.presence_of_element_located((By.XPATH, '//div[@class="ui_pagination is-centered"]/a'))
			)

		butonPages = driver.find_elements_by_xpath('//div[@class="ui_pagination is-centered"]/a')
		classBoton = butonPages[1].get_attribute('class')


	return linkNew

def move_empty(listWord):
	newlist = []
	for iw in listWord:
		if len(iw) > 0:
			newlist.append(iw)
	return newlist

def get_coordinates(link):
	driver.get(link)
	latitude = 0
	longitude = 0
	WebDriverWait(driver, 10).until(
		EC.presence_of_element_located((By.XPATH, "//img[@class='restaurants-detail-overview-cards-LocationOverviewCard__mapImage--22-Al']"))
		)

	nameMap = driver.find_elements_by_xpath("//img[@class='restaurants-detail-overview-cards-LocationOverviewCard__mapImage--22-Al']")
	dirMap = nameMap[0].get_attribute('src')
	partReque = dirMap.split('&')
	for iparametro in partReque:
		if 'center=' in iparametro:
			iparametro = iparametro.replace('center=', '')
			coord = iparametro.split(',')
			latitude = coord[0]
			longitude = coord[1]

	return [latitude, longitude]

class tripAdvisorSpider(CrawlSpider):
	name = 'tripAdvisor'
	def __init__(self, Keywords=None, *args, **kwargs):
		super(tripAdvisorSpider, self).__init__(*args, **kwargs)
		self.allowed_domain = ['https://www.tripadvisor.com.mx/']
		self.Keywords = Keywords
		print(self.Keywords)
		listKeywords = "%20".join(Keywords.split(','))
		geo = '150768'
		self.page = "https://www.tripadvisor.com.mx/Search?q=" + listKeywords + "&blockRedirect=true&ssrc=e&geo="+geo+"&rf=1"
		self.start_urls = [self.page]

	def parse(self, response):
		xrequests = requests.get(self.page)
		linkNews= process_request(xrequests)
		for head in linkNews:
			print(head)
			head = 'https://www.tripadvisor.com.mx/' + head
			#yield SplashRequest(head, self.parse_item, endpoint='render.json', args={ 'har': 1, 'html': 1,})
			yield Request(url = head, callback=self.parse_item, dont_filter=True)

	def parse_item(self, response):
		print(response.request.url)
		text_maker = html2text.HTML2Text()
		text_maker.ignore_links = True

		tripAdvisor_item =  TripadvisorItem()
		tripAdvisor_item ['Nombre'] = response.xpath('//h1[@class="ui_header h1"]/text()').extract()[0]

		horario = response.xpath('//span[@class="public-location-hours-LocationHours__hoursOpenerText--42y6t"]/span[2]').extract()
		tripAdvisor_item ['Horario'] = ''
		if len(horario) > 0:
			horarioSplit = move_empty(text_maker.handle(horario[0]).split("\n\n"))
			tripAdvisor_item ['Horario'] = horarioSplit[0]

		#calificaciones`
		tripAdvisor_item ['Calificacion'] = response.xpath('//div[@class="restaurants-detail-overview-cards-RatingsOverviewCard__primaryRatingRow--VhEsu restaurants-detail-overview-cards-RatingsOverviewCard__cx_brand_refresh_phase2--1d7i-"]/span/text()').extract()[0]
		top1 = response.xpath('//div[@class="restaurants-detail-overview-cards-RatingsOverviewCard__ranking--17CmN"]').extract()
		tripAdvisor_item ['top1'] = text_maker.handle(top1[0]).replace('\n\n','')
		top2 = response.xpath('//div[@class="restaurants-detail-overview-cards-RatingsOverviewCard__award--31yzt"]').extract()
		if len(top2)>0:
			tripAdvisor_item ['top2'] = text_maker.handle(top2[0]).replace("\n\n",'')
		else:
			tripAdvisor_item ['top2'] = '-'

		calificaciones = response.xpath('//span[@class="restaurants-detail-overview-cards-RatingsOverviewCard__ratingBubbles--1kQYC"]/span').extract()
		tripAdvisor_item ['Comida'] = '0'
		tripAdvisor_item ['Servicio'] = '0'
		tripAdvisor_item ['Calidad'] = '0'
		tripAdvisor_item ['Ambiente'] = '0'
		if  len(calificaciones) > 0:
			tripAdvisor_item ['Comida'] = calificaciones[0][37:39]
		if  len(calificaciones) > 1:
			tripAdvisor_item ['Servicio'] = calificaciones[1][37:39]
		if  len(calificaciones) > 2:
			tripAdvisor_item ['Calidad'] = calificaciones[2][37:39]
		if  len(calificaciones) > 3:
			tripAdvisor_item ['Ambiente'] = calificaciones[3][37:39]


		#detalles
		tripAdvisor_item ['detalles_precios'] = '-'
		tripAdvisor_item ['detalles_tipo_comida'] = '-'
		tripAdvisor_item ['detalles_comida'] = '-'
		tripAdvisor_item ['detalles_dietas'] ='-'
		tripAdvisor_item ['detalles_caracteristicas'] ='-'
		detalles = response.xpath('//div[@class="restaurants-detail-overview-cards-DetailsSectionOverviewCard__detailsSummary--evhlS"]/div').extract()
		if  len(detalles) > 0:
			for idet in detalles:
				idet = text_maker.handle(idet)
				if 'RANGO DE PRECIOS' in idet:
					listaWord = idet.split("\n\n")
					cleanList =  move_empty(listaWord)
					tripAdvisor_item ['detalles_precios'] = cleanList[1]
				elif 'TIPOS DE COMIDA' in idet:
					listaWord = idet.split("\n\n")
					cleanList =  move_empty(listaWord)
					tripAdvisor_item ['detalles_tipo_comida'] = cleanList[1]
				elif 'Comidas' in idet:
					listaWord = idet.split("\n\n")
					cleanList =  move_empty(listaWord)
					tripAdvisor_item ['detalles_comida'] = cleanList[1]
				elif 'Dietas especiales' in idet:
					listaWord = idet.split("\n\n")
					cleanList =  move_empty(listaWord)
					tripAdvisor_item ['detalles_dietas'] = cleanList[1]
				elif 'CARACTERÍSTICAS' in idet:
					listaWord = idet.split("\n\n")
					cleanList =  move_empty(listaWord)
					tripAdvisor_item ['detalles_caracteristicas'] = cleanList[1]

		#ubicacion
		direccion =response.xpath('//div[@class="restaurants-detail-overview-cards-LocationOverviewCard__addressLink--1pLK4 restaurants-detail-overview-cards-LocationOverviewCard__detailLink--iyzJI"]').extract()
		cleanDireccion =  move_empty(text_maker.handle(direccion[0]).split("\n\n"))
		tripAdvisor_item ['direccion'] = cleanDireccion[0]
		print(direccion)

		mail = response.xpath('//div[@class="restaurants-detail-overview-cards-LocationOverviewCard__detailLink--iyzJI restaurants-detail-overview-cards-LocationOverviewCard__contactItem--1flT6"]/span/a/@href').extract()
		if len(mail) > 0:
			mailSplit = mail[0].split('?')
			tripAdvisor_item ['mail'] = mailSplit[0][7:]
		else:
			tripAdvisor_item ['mail'] = ''

		telefono = response.xpath('//div[@class="restaurants-detail-overview-cards-LocationOverviewCard__detailLink--iyzJI"]').extract()
		if len(telefono) > 0:
			telefonoSplit =  move_empty(text_maker.handle(telefono[0]).split("\n\n"))
			tripAdvisor_item ['telefono'] = telefonoSplit[0]
		else:
			tripAdvisor_item ['telefono'] = ''

		tripAdvisor_item ['calle'] = ''
		calle = response.xpath('//span[@class="street-address"]').extract()
		if len(calle) > 0:
			callesplit = move_empty(text_maker.handle(calle[0]).split("\n\n"))
			tripAdvisor_item ['calle'] = callesplit[0]

		pais = response.xpath('//span[@class="country-name"]').extract()
		paissplit = move_empty(text_maker.handle(pais[0]).split("\n\n"))
		tripAdvisor_item ['pais'] = paissplit[0]

		localidad = response.xpath('//span[@class="locality"]').extract()
		localidadsplit = move_empty(text_maker.handle(localidad[0]).split("\n\n"))
		tripAdvisor_item ['localidad'] = localidadsplit[0]

		direccionExtra = response.xpath('//span[@class="extended-address"]').extract()
		if len(direccionExtra) > 0:
			direccionExtrasplit = move_empty(text_maker.handle(direccionExtra[0]).split("\n\n"))
			tripAdvisor_item ['direccionExtra'] = direccionExtrasplit[0]
		else:
			tripAdvisor_item ['direccionExtra'] = ''

		tripAdvisor_item['link'] = response.request.url
		latitude, longitude = get_coordinates(response.request.url)
		tripAdvisor_item ['latitude'] = latitude
		tripAdvisor_item ['longitude'] = longitude

		# opiniones
		n_opiniones = response.xpath('//span[@class="reviews_header_count"]').extract()
		nOpinionsplit = move_empty(text_maker.handle(n_opiniones[0]).split("\n\n"))
		tripAdvisor_item ['n_opiniones'] = nOpinionsplit[0][1:-1].replace(',','')

		#evaluaciones = response.xpath("//span[@class='row_num is-shown-at-tablet']").extract()
		tripAdvisor_item ['opiniones_excelente'] = -1
		tripAdvisor_item ['opiniones_muy_bueno'] = -1
		tripAdvisor_item ['opiniones_regular'] = -1
		tripAdvisor_item ['opiniones_pesimo'] = -1
		tripAdvisor_item ['opiniones_malo'] = -1
		evaluaciones = response.xpath("//span[@class='row_num  is-shown-at-tablet']/text()").extract()
		if len(evaluaciones) > 0:
			tripAdvisor_item ['opiniones_excelente'] = evaluaciones[0].replace(',','')
			tripAdvisor_item ['opiniones_muy_bueno'] = evaluaciones[1].replace(',','')
			tripAdvisor_item ['opiniones_regular'] = evaluaciones[2].replace(',','')
			tripAdvisor_item ['opiniones_pesimo'] = evaluaciones[3].replace(',','')
			tripAdvisor_item ['opiniones_malo'] = evaluaciones[4].replace(',','')

		yield tripAdvisor_item

		for cometariosTabla in response.xpath("//div[@class='review-container']"):
			yield response.follow(cometariosTabla, callback=self.parse_cometarios)

	def parse_cometarios(self,response):
		# Gather cast list data
		print('comentario1------------------------------>')
		cometarios_item =  TripadvisorOpinionesItem()
		cometarios_item['nombre'] = response.xpath('//h1[@class="ui_header h1"]/text()').extract()[0]
		print('comentario2------------------------------>')
		cometarios_item['autor'] = response.xpath('//div[@class="info_text pointer_cursor"]/div/text()').extract()[0]
		aportaciones = response.xpath('//span[@class="badgeText"]/text()').extract()[0]
		cometarios_item['aportaciones'] = aportaciones.replace(' opiniones','')
		print('comentario3------------------------------>')
		eleCalificacion = response.xpath('//div[@class="ui_column is-9"]/span[1]/@class').extract()
		cometarios_item['calificacacion'] = eleCalificacion[-2:]
		escrito = response.xpath('//div[@class="ui_column is-9"]/span[1]/@class').extract()
		if  "Opinión escrita " in escrito:
			cometarios_item['FechaEscritura'] = escrito.replace('Opinión escrita ', '')
		elif "Escribió una opinión el " in escrito:
			cometarios_item['FechaEscritura'] = escrito.replace('Escribió una opinión el ', '')
		else:
			cometarios_item['FechaEscritura'] = ''

		cometarios_item['opinion'] = response.xpath('//span[@class="noQuotes"]/text()').extract()
		cometarios_item['opinion2'] = response.xpath('//p[@class="partial_entry"]/text()').extract()
		cometarios_item['FechaVisita'] = response.xpath('//div[@class="prw_rup prw_reviews_stay_date_hsx"]/text()').extract()
		print(cometarios_item)
		yield cometarios_item
