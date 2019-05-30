#!/usr/bin/env python

import os, sys, getpass, glob
import subprocess
import random, time
import inspect
import json
import pprint
import requests
from xml.etree import ElementTree
try:
	import overpy
	overpy_api = overpy.Overpass()
except:
	print("\nRUN $HOME/anaconda2/bin/pip install overpy\n")
try:
	import overpass
	overpass_api = overpass.API()
except:
	print("\nRUN $HOME/anaconda2/bin/pip install overpass\n")
try:
	from OSMPythonTools.api import Api
	OSMPythonTools_api = Api()
	from OSMPythonTools.overpass import Overpass
	OSMPythonTools_overpass = Overpass()
	from OSMPythonTools.nominatim import Nominatim
	OSMPythonTools_nominatim = Nominatim()
except:
	print("\nCOPY ./OSMPythonTools\n")

mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
if mypydir not in sys.path: sys.path.append(mypydir)
from readconf import get_conf,get_conf_int,get_conf_float
iprint = 2
configfile="conf.txt"


def nominatim_search(place):
	'''search place bbox etc.'''
	res = OSMPythonTools_nominatim.query(place)
	print res.areaId() 
	print res.toJSON()
	'''[{u'display_name': u'Champaign, Champaign County, Illinois, United States of America', u'importance': 0.34456300299294, u'place_id': u'178045234', u'lon': u'-88.2433829', u'lat': u'40.1164205', u'osm_id': u'126114', u'boundingbox': [u'40.0615448', u'40.167074', u'-88.333179', u'-88.22104'], u'type': u'city', u'class': u'place'} , 
	{u'display_name': u'Champaign County, Illinois, United States of America', u'importance': 0.32209850607033, u'place_id': u'178377849', u'lon': u'-88.1975558', u'lat': u'40.1346857', u'osm_type': u'relation', u'boundingbox': [u'39.8790978', u'40.4006758', u'-88.4635711', u'-87.9288159'], u'type': u'administrative', u'class': u'boundary'} , 
	{u'display_name': u'Champaign County, Ohio, United States of America', u'importance': 0.20191361516768, u'place_id': u'178516085', u'lon': u'-83.7701999', u'lat': u'40.1726987', u'osm_type': u'relation', u'boundingbox': [u'40.0111509', u'40.273459', u'-84.036069', u'-83.4947925'], u'type': u'administrative', u'class': u'boundary'}
	]'''

def query_obj_given_id_list(what,idlist):
	idlist=[str(x) for x in idlist]
	if what.startswith("node"):
		response = overpass_api.Get('node(id:%s);'%(",".join(idlist)))
	elif what.startswith("way"):
		response = overpass_api.Get('way(id:%s);'%(",".join(idlist)))
	elif what.startswith("rel"):
		response = overpass_api.Get('rel(id:%s);'%(",".join(idlist))) # not reliable. missing.
	return response
'''{"features": [{"geometry": {"coordinates": [-88.2602251, 40.1369475], "type": "Point"}, "id": 2521239800, "properties": {"amenity": "bicycle_parking", "bicycle_parking": "stands", "capacity": "6"}, "type": "Feature"}, {"geometry": {"coordinates": [-88.2593694, 40.1072651], "type": "Point"}, "id": 3624259758, "properties": {"natural": "tree", "species": "Acer rubrum"}, "type": "Feature"}], "type": "FeatureCollection"}'''

def query_way_given_name(name):
	result = overpy_api.query('[out:json];way["name"="%s"];out;'%name)
	print result.nodes
	print result.ways
	print result.relations
	'''[]
	[<overpy.Way id=5342274 nodes=[38031888, 38129856, 1349435035]>, <overpy.Way id=509961565 nodes=[190491617, 5304944218, 5296393324, 5296391118, 190525191, 5256634744, 190473227, 190474464]>]
	[]'''
	result = OSMPythonTools_overpass.query('way["name"="%s"]; out;'%name)
	print result.elements()[0].nodes()[0].id()
	print result.elements()[0].tags()
	'''38031888
	{u'lanes': u'4', u'name': u'East Springfield Avenue', u'tiger:reviewed': u'no', u'postal_code': u'61820', u'loc_name': u'County Road 1600 N', u'ref': u'US 45;US 150', u'highway': u'primary'}'''

def query_way_given_id(wid):
	way = OSMPythonTools_api.query('way/%d'%wid)
	print( "way.tag('name')", way.tag('name') )# u'East Coddington Circle'
	print( "way.tag('highway')", way.tag('highway') )# u'residential'

def query_node_given_name(name):
	response = overpass_api.Get('node["name"="%s"]'%name)
	pprint.pprint(response)
	result = overpy_api.query('[out:json];node["name"="%s"];out;'%name)
	print result.nodes
	print result.ways
	print result.relations
'''[<overpy.Node id=153541047 lat=40.1164205 lon=-88.2433829>, <overpy.Node id=316952605 lat=40.1346857 lon=-88.1975558>, <overpy.Node id=316982638 lat=40.1726987 lon=-83.7701999>]
[]
[]'''

def query_node_given_bbox_swne(bbox):
	result = overpy_api.query('[out:json];node(%f,%f,%f,%f);out;'%tuple(bbox))
	print result.nodes
	print result.ways
	print result.relations
'''[<overpy.Node id=38154012 lat=40.1116650 lon=-88.2239490>, <overpy.Node id=4717333882 lat=40.1106273 lon=-88.2233440>]
[]
[]'''

def query_nodes_ways_given_bbox_swne(bbox):
	map_query = overpass.MapQuery(bbox[0],bbox[1],bbox[2],bbox[3])
	response = overpass_api.Get(map_query)
	print response
'''{"features": [{"geometry": {"coordinates": [-88.223949, 40.111665], "type": "Point"}, "id": 38154012, "properties": {}, "type": "Feature"}, ] }'''

def get_ways_given_nid(nid):
	OsmApiUrl="http://api.openstreetmap.org"
	url = OsmApiUrl+"/api/0.6/node/%d/ways"%nid
	if iprint>=2: print(url)
	ret = requests.get(url)
	tree = ElementTree.fromstring(ret.content)
	for child in tree.iter('*'):
		print(child.tag, child.attrib)
'''('way', {'changeset': '48318015', 'version': '2', 'uid': '3901661', 'visible': 'true', 'timestamp': '2017-05-01T20:53:21Z', 'id': '358730881', 'user': 'Kev-H'})
('nd', {'ref': '1518918162'})
('nd', {'ref': '1509407233'})
('nd', {'ref': '2804539692'})
('nd', {'ref': '1509407223'})
('nd', {'ref': '37985171'})
('nd', {'ref': '1509407112'})
('nd', {'ref': '1509407333'})
('nd', {'ref': '37985166'})
('tag', {'k': 'highway', 'v': 'residential'})
('tag', {'k': 'name', 'v': 'South Wright Street'})
('tag', {'k': 'surface', 'v': 'asphalt'})'''


if __name__ == "__main__":

	if 0: get_ways_given_nid(37985166)
	if 0: query_node_given_bbox_swne([40.110548,-88.2251,40.111834,-88.220669])
	if 0: query_nodes_ways_given_bbox_swne([40.110548,-88.2251,40.111834,-88.220669])
	if 0: query_way_given_name('East Springfield Avenue')
	if 0: query_node_given_name('Champaign')
	if 0: 
		query_obj_given_id_list('rel',[4464052,4476220]) # missing
		query_obj_given_id_list('node',[3624259758,2521239800])
	if 0: query_way_given_id(5332909)
	if 0: nominatim_search("Champaign,IL,USA")
	if 0: query_obj_given_id_list('node',[3624259758])

