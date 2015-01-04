# -*- coding: utf-8-sig -*-
import logging,rdflib,sys
import threading
reload(sys)
sys.setdefaultencoding("utf-8")

from neo4jrestclient.client import GraphDatabase,Q
gdb = GraphDatabase("http://localhost:7474/db/data/")#Se localiza el servidor de la base de datos de neo4j

def InsertarSPO(sujeto,predicado,objeto):#Esta función sirve para insertar en la base de dato neo4j, se le envía en formato de tripletas (sujeto,predicado,objeto).
	if 'http' in sujeto[:4] and 'http' in objeto[:4]:
		queryCrear = """MERGE (sujeto:Recurso {uri:"%s"})MERGE(objeto:Recurso {uri:"%s"})"""%(sujeto,objeto)
		queryRelacionar = """MATCH (sujeto {uri:"%s"}),(objeto {uri:"%s"}) MERGE (sujeto) - [p:Predicado{uri:"%s"}] ->(objeto) RETURN sujeto"""%(sujeto,objeto,predicado)
	else:
		if 'http' in sujeto[:4]:
			queryCrear = """MERGE (sujeto:Recurso {uri:"%s"})"""%(sujeto)
			queryRelacionar = """MATCH (sujeto {uri:"%s"}) MERGE (sujeto) - [p:Predicado{uri:"%s"}] ->(objeto:Atributo {valor:"%s"}) RETURN sujeto"""%(sujeto,predicado,objeto)
		else:
			if 'http' in objeto[:4]:
				queryCrear = """MERGE (sujeto:Atributo {valor:"%s"})MERGE(objeto:Recurso {uri:"%s"})"""%(sujeto,objeto)
				queryRelacionar = """MATCH (sujeto {valor:"%s"}),(objeto {uri:"%s"}) MERGE (sujeto) - [p:Predicado{uri:"%s"}] ->(objeto) RETURN sujeto"""%(sujeto,objeto,predicado)
			else:				
				queryCrear = """MERGE (sujeto:Atributo {valor:"%s"})MERGE(objeto:Atributo {valor:"%s"})"""%(sujeto,objeto)
				queryRelacionar = """MATCH (sujeto {valor:"%s"}),(objeto {valor:"%s"}) MERGE (sujeto) - [p:Predicado{uri:"%s"}] ->(objeto) RETURN sujeto"""%(sujeto,objeto,predicado)

	result = gdb.query(q=queryCrear)
	result = gdb.query(q=queryRelacionar)

def InsertarLista(lista):#Esta función revide una lista que contiene una lista en formato de tripletas(sujeto,predicado,objeto) para enviar una por una a la función insertarSPO
	for triple in lista:
		InsertarSPO(triple[0],triple[1],triple[2])

#def InsertHilos(g):
#	print 'insetando a Neo4j con Hilos'
#	i=0
#	jobs = []
#	cont = 100
#	for subj, pred, obj in g:
#		if i<0:
#			i=i+1
#			continue
#		print i
#		i=i+1
#		try:
#			t = threading.Thread(target=insertarSPO, args=(subj,pred,obj))
#			jobs.append(t)
#			t.start()
#			if i == cont:
#				for proc in jobs:
#					proc.join()
#				jobs=[]
#				cont=cont+100
#
#		except Exception, e:
#			print e
#			continue
def Insertar(g):#Recive el archivo leído ya sea en formato red o nt, para enviarlo fila por fila a la función insertarSPO para que lo inserte en la Base de datos de neo4j
	print 'insetando a Neo4j'
	i=0
	for subj, pred, obj in g:
		if i<0:
			i=i+1
			continue
		print i
		i=i+1
		InsertarSPO(subj,pred,obj)
def CargarArchivo(DirArchivo):# con la direcion del archivo lo lee, y una ves leido lo encia a insertar
	print 'Cargando rdf'
	g = rdflib.Graph()
	try:
		#Se lee el archivo especificado para determinar que tipo de archivo es, si en un archivo diferente a rdf o nt, presenta el mensaje que el archivo no es soportadpo
		if ".rdf" in DirArchivo:
			result = g.load(DirArchivo, format="application/rdf+xml")	
		elif ".nt" in DirArchivo
			result = g.parse(DirArchivo, format="nt")
		else:
			print 'ARCHIVO NO SOPORTADO'
	except Exception, e:
		print 'Error en la lectura del archivo'
		
	print 'Rdf Cargado '
	print("graph has %s statements." % len(g))
	#Después de leer el archivo se lo envía a insertar con la función Insert
	Insertar(g)


logging.basicConfig()
DirArchivo=sys.argv[1]

CargarArchivo(DirArchivo)
