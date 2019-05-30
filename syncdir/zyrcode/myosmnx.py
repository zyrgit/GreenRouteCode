#!/usr/bin/env python

import os, sys, getpass
import subprocess
import random, time
import inspect, glob
import collections
import math
from shutil import copy2, move as movefile
import numpy as np
import pandas as pd
import pprint
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
from namehostip import get_platform

import osmnx as ox, geopandas as gpd
from mygmaps import GoogleMaps
ox.config(log_file=False, log_console=False, use_cache=True)

iprint=2
Has_Display=True
if get_platform()!="mac": Has_Display=False

def get_important_node_ids(G): # get rid of some mid-way shaping nodes
	nds = []
	for nid in G.nodes():
		if ox.is_endpoint(G, nid, strict=True):
			nds.append(nid)
	return nds


def simplify_graph(G):
	''' strict mode more consistent with non-important mid nodes.'''
	# nc = ['b' if ox.is_endpoint(G, node, strict=True) else 'r' for node in G.nodes()]
	# fig, ax = ox.plot_graph(G, node_color=nc, node_zorder=3)
	G = ox.simplify_graph(G, strict=True)
	if iprint>=2: print("simplify_graph showing simplified graph...")
	if Has_Display: fig, ax = ox.plot_graph(G, node_color='b', node_zorder=3)
	return G

def download_graph(loc_list, **kwargs):
	simplify = kwargs.get("simplify",True) # get rid of non-important nodes?
	auto_bbox = kwargs.get("auto_bbox",True) # auto query polygon bbox
	show_plot = kwargs.get("show_plot",True)
	distance = kwargs.get("distance",20000)
	print("osmnx download_graph()...")

	if isinstance(loc_list,list) and isinstance(loc_list[0],float) and len(loc_list)>2: # [lats,latn,lngw,lnge]
		north=loc_list[1]
		south=loc_list[0]
		east=loc_list[3]
		west=loc_list[2]
		G=ox.graph_from_bbox(north,south,east,west,network_type='drive',simplify=simplify)
	
	elif isinstance(loc_list,list) and isinstance(loc_list[0],float) and len(loc_list)==2:# [lat,lng]
		G=ox.graph_from_point(loc_list, distance=distance,network_type='drive',simplify=simplify)

	elif isinstance(loc_list, str) or (isinstance(loc_list,list) and isinstance(loc_list[0], str)):
		if auto_bbox: # addr or [addr1,addr2,] use auto bbox
			G=ox.graph_from_place(loc_list,network_type='drive',simplify=simplify)# No-distance arg
		else: #  addr or [addr1,addr2,] use distance
			G=ox.graph_from_address(loc_list,network_type='drive',distance=distance,simplify=simplify)
	else:
		print(__file__+" download_graph() input error: ",loc_list)
		return None

	if show_plot and iprint and Has_Display: 
		if iprint>=2: print("download_graph showing downloaded graph...")
		fig, ax = ox.plot_graph(G)
	if not simplify: 
		simplify_graph(G)
	if iprint>=2: 
		print ox.basic_stats(G)
	return G


if __name__ == "__main__": # not in use
	arglist=sys.argv[1:]

	if 0: download_graph(["Champaign, IL, USA"，])
	if 0 or "bbox" in arglist: 
		addr="San Francisco Bay Area,US"
		print(addr,find_osm_bbox(addr))
	if 0:
		download_graph("Champaign, IL, USA", simplify=False)
