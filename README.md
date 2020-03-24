# Descripción del repositorio - Location finder
```r
* Instalar GitHub Desktop para un mejor uso del repositorio, clonar el proyecto en una ruta local.
* Cada vez que realicen modificaciones: agregar un resumen del cambio (requerido) con sus iniciales 
y acción. Ej. ILR Update
* Finalmente, dar click en COMMIT TO MASTER y en PUSH ORIGIN.
```
El repositorio cuenta con 6 carpetas, en las cuales se debe almacenar la siguiente información:
* `data`: Almacenar datos extraídos por API (crear carpeta). Nombre sugerido de los archivos: `data_<API>_<tabla>.csv`
* `keywords`: Se guardan las palabras clave que se buscaran por región.
* `maps`: Almacenar los mapas generados por API. Nombre archivo: `map_<region>.html`
* `networks`: Almacenar los archivos que se utilizaron para la construcción de las redes y el proyecto Gephi o Cytoscape.
* `scripts`: Almacenar los scripts en Python por API. Nombre archivo: `script_<API>_alldata.py`
* `stats`: Almacenar los archivos CSV con las estadisticas generadas por API. Nombre archivo: `stats_<API>_full.csv`

## Regiones
Las regiones que se deben considerar para la entrega final son: CDMX, CR, CUN, GDL, MEXICO. MTY, MXLI, SR

## Abreviaturas API
Favor de usar estas abreviaturas en los nombres de sus archivos:
* Twitter: TW
* Yelp: YL
* Tumblr: TB
* Google Places: GP
* Trip Advisor: TA
* DENUE: DN
* Foursquare: FS
