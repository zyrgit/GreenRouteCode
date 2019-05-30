#!/usr/bin/env python

import os, sys, getpass
import random, time, math
import subprocess
import inspect, glob
import requests, urllib
from shutil import copy2, move as movefile
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
from namehostip import get_my_ip,get_platform
from readconf import get_conf,get_conf_int,get_conf_float
from logger import SimpleAppendLogger
from util import read_lines_as_list,read_lines_as_dic,strip_illegal_char,strip_white_spaces
from myosmnx import download_graph,get_important_node_ids
from myosrm import query_node_elevation_from_osm_file,query_way_speed_from_osm_file
from geo import get_osm_file_quote_given_file,get_dist_meters_latlng2,mm_addr_bbox_snwe,dic_mm_addr_bbox_snwe
from util import download_file,get_file_size_bytes
from mygmaps import GoogleMaps


iprint =2

def find_osm_bbox(addr, geo_lookup_file=True):
	''' You need to specify bbox in geo.py if you do not trust auto bbox below. '''
	res=mm_addr_bbox_snwe.get(addr)
	if res is None and geo_lookup_file: # look up pickled file 
		if addr in dic_mm_addr_bbox_snwe: # gen by geo.py
			res= dic_mm_addr_bbox_snwe[addr]
	if iprint: print("mm_addr_bbox_snwe:",addr,res)
	isFromMM=0
	if res is not None:
		lats,latn,lngw,lnge=res
		isFromMM=1
	else:
		urladdr=urllib.quote_plus(addr)
		url="http://nominatim.openstreetmap.org/search?format=json&limit=1&dedupe=0&polygon_geojson=1&q=%s"%(urladdr)
		if iprint>=2: print(url)
		ret = requests.get(url).json()
		try:
			lats,latn,lngw,lnge = ret[0]["boundingbox"]
			use_google=0
		except:
			use_google=1

		if use_google==1:
			if iprint>=2: print("Using gmaps for bbox")
			gmaps=GoogleMaps()
			viewport=gmaps.address2bbox(addr)
			lats = viewport["southwest"]["lat"]
			latn = viewport["northeast"]["lat"]
			lngw = viewport["southwest"]["lng"]
			lnge = viewport["northeast"]["lng"]
	if iprint>0:  
		print(__file__,lats,latn,lngw,lnge)
		print(get_dist_meters_latlng2([lats,lngw],[latn,lngw])/1000,"km  height")
		print(get_dist_meters_latlng2([latn,lngw],[latn,lnge])/1000,"km  width")
	return float(lats),float(latn),float(lngw),float(lnge),isFromMM


def download_osm(wlng,slat,elng,nlat,fname=""): # see ~note.txt for manual download
	# url="http://overpass-api.de/api/map?bbox=%f,%f,%f,%f"%(wlng,slat,elng,nlat)
	# url="http://api.openstreetmap.org/api/0.6/map?bbox=%f,%f,%f,%f"%(wlng,slat,elng,nlat)
	url="http://overpass.openstreetmap.ru/cgi/xapi_meta?*[bbox=%f,%f,%f,%f]"%(wlng,slat,elng,nlat)
	if iprint>=1: print(url)
	if fname=="": fname="%.3f-%.3f.osm"%(slat,wlng)
	ret =    download_file(url, fname)
	if iprint>=1: print(ret)
	return ret


class osmPipeline:
	def __init__(self,**kwargs): 
		folder_path = kwargs["folder_path"]
		assert folder_path.startswith(os.sep), "Please provide abs path for folder_path!"
		self.data_dir = folder_path
		try:
			self._osm_file = self.get_osm_file_path()
			self._address_no_space=self.get_address_no_space()
			with open(self.data_dir+os.sep+"_bbox_snwe.txt","r") as f:
				self._bbox_snwe=[float(x) for x in f.readline().split(" ")]
			if iprint>=3: print("osmPipeline %s loaded"%folder_path)
		except:
			if iprint>=2: print("New osmPipeline instance")

	def within_bbox(self,latlng):
		''' If it's in the area'''
		return latlng[0]>self._bbox_snwe[0] and latlng[0]<self._bbox_snwe[1] and latlng[1]>self._bbox_snwe[2] and latlng[1]<self._bbox_snwe[3] 


	def write_way_speed(self,):
		''' Run by cluster. Includes MemLock within, no need for lock. 
		Allow re-run before expire. Cause duplication appended after expire.'''
		self._osm_file = self.get_osm_file_path()
		tmpf=self._osm_file.rstrip(".osm")+"-nids-to-speed.txt"
		osmMB=get_file_size_bytes(self._osm_file)/1024.0/1024.0
		if osmMB>100:exec_init_delay=0.2
		elif osmMB>50:exec_init_delay=0.1
		elif osmMB<20:exec_init_delay=0.005
		else: exec_init_delay=(osmMB)/4000.0
		exec_init_delay*=(1+random.random()/50)
		if iprint: 
			print(self.__class__.__name__+" write_way_speed exec_init_delay",exec_init_delay)
			print("Reading "+self._osm_file)
		query_way_speed_from_osm_file(self._osm_file, tmpf, addr=self.get_address_no_space(),exec_init_delay=exec_init_delay)

	def remove_file_way_speed(self,):
		''' Need lock for caller!'''
		self._osm_file = self.get_osm_file_path()
		tmpf=self._osm_file.rstrip(".osm")+"-nids-to-speed.txt"
		if os.path.exists(tmpf): 
			if iprint>=2:print("remove",tmpf,tmpf+".old")
			movefile(tmpf,tmpf+".old")
		tmpf=self.data_dir+os.sep+"cache-%s-nids-to-speed.txt"%self._address_no_space
		if os.path.exists(tmpf): 
			if iprint>=2:print("remove",tmpf)
			os.remove(tmpf)
		tmpf=self.data_dir+os.sep+"COMPLETE-way-speed" # defined in 3genOsmCache.py
		if os.path.exists(tmpf): 
			if iprint>=2:print("remove",tmpf)
			os.remove(tmpf)


	def write_node_elevation(self, ignore_mc=False, load_previous=True,lock_sfx=""):
		''' Run by One server.
		Allow re-run before expire. 
		Cause duplication appended after expire, or ignore_mc is set.'''
		self._osm_file = self.get_osm_file_path()
		tmpf=self._osm_file.rstrip(".osm")+"-nid-to-elevation.txt"
		query_node_elevation_from_osm_file(self._osm_file, tmpf, addr=self.get_address_no_space(),ignore_mc=ignore_mc,load_previous=load_previous,lock_sfx=lock_sfx)

	def remove_file_node_elevation(self,):
		''' Need lock for caller!'''
		self._osm_file = self.get_osm_file_path()
		tmpf=self._osm_file.rstrip(".osm")+"-nid-to-elevation.txt"
		if os.path.exists(tmpf): 
			if iprint>=2:print("remove",tmpf,tmpf+".old")
			movefile(tmpf,tmpf+".old")


	def write_important_nodes(self,overwrite=False):
		''' Run by One server. Need lock for caller! 
		Idempotent all time, either skip or overwrite.'''
		self._osm_file = self.get_osm_file_path()
		tmpf=self._osm_file.rstrip(".osm")+"-important-nids.txt"
		if not os.path.exists(tmpf) or overwrite:
			try:
				if iprint: print("Num of important nodes:",len(self._major_nids))
			except:
				self.download_graph_nx()
			with open(tmpf,"w") as f:
				for nid in self._major_nids:
					f.write(str(nid)+"\n")


	def get_osm_file_path(self,):
		try:
			return self._osm_file
		except:
			tmpf=self.data_dir+os.sep+"_osm_file_path.txt"
			with open(tmpf,"r") as f:
				return f.readline().strip()

	def get_address_no_space(self,):
		self._address_no_space= self.get_osm_file_path().split(os.sep)[-1].rstrip(".osm")
		return self._address_no_space

	# not consume quota, 
	def get_target_bbox(self,addr, extend=True):
		meterPerDeg = 111000
		lats,latn,lngw,lnge, isFromMM =    find_osm_bbox(addr)
		width= get_dist_meters_latlng2([lats,lngw],[lats,lnge])
		height= get_dist_meters_latlng2([latn,lngw],[lats,lngw])
		print("width",width,"height",height)
		if width>60000: # if not from memcache, then do extend.
			extendMeters = -width/6.0
		elif width>40000:
			extendMeters = -5000
		elif width<10000:
			extendMeters = 3000
		else: extendMeters=0
		extendLng=0.0
		if isFromMM==0:
			extendLng = float(extendMeters)/max(1e-10,meterPerDeg*math.cos(math.pi/180*latn))
		print("extendLng",extendLng)
		lngw=max(-179.9,min(179.9,lngw-extendLng))
		lnge=max(-179.9,min(179.9,lnge+extendLng))
		if height>60000:
			extendMeters = -height/6.0
		elif height>40000:
			extendMeters = -5000
		elif height<10000:
			extendMeters = 3000
		else: extendMeters=0
		extendLat=0.0
		if isFromMM==0:
			extendLat = float(extendMeters)/meterPerDeg
		print("extendLat",extendLat)
		lats=max(-89.9,min(89.9,lats-extendLat))
		latn=max(-89.9,min(89.9,latn+extendLat))
		return lats,latn,lngw,lnge

	# not consume quota, 
	def download_osm_given_bbox(self,lats,latn,lngw,lnge, addr=""):
		'''Called by download_osm_given_address()'''
		addr= strip_white_spaces(strip_illegal_char(addr))
		if addr is None or addr=="": addr="%.3f-%.3f"%(lats,lngw)
		if not addr.endswith(".osm"): addr+=".osm"
		fpath=self.data_dir+os.sep+addr
		if not os.path.exists(fpath):
			print(fpath+"  not exists. Downloading")
			download_osm(lngw,lats,lnge,latn, fpath)
		if iprint>=1: print(fpath,os.stat(fpath).st_size/1024.0/1024.0,"MB")
		self._bbox_snwe = [lats,latn,lngw,lnge]
		tmpf=self.data_dir+os.sep+"_bbox_snwe.txt"
		if not os.path.exists(tmpf):
			print(tmpf,self._bbox_snwe)
			with open(tmpf,"w") as f:
				f.write(" ".join([str(x) for x in self._bbox_snwe]))
			if iprint>=1: print("__bbox_snwe written")
		self._osm_file = fpath
		tmpf=self.data_dir+os.sep+"_osm_file_path.txt"
		if not os.path.exists(tmpf):
			print(tmpf,self._osm_file)
			with open(tmpf,"w") as f:
				f.write(self._osm_file)
			if iprint>=1: print("_osm_file_path written")
		return fpath

	# not consume quota, 
	def download_osm_given_address(self,addr):
		'''Main'''
		tmpf=self.data_dir+os.sep+"_bbox_snwe.txt"
		if not os.path.exists(tmpf):
			lats,latn,lngw,lnge=self.get_target_bbox(addr)
			print("get_target_bbox",lats,latn,lngw,lnge)
		else:
			with open(tmpf,"r") as f:
				st=f.readline().split(" ")
				print("get_target_bbox f",st)
				lats,latn,lngw,lnge=[float(x) for x in st]
		fpath = self.download_osm_given_bbox(lats,latn,lngw,lnge,addr)
		addr= strip_white_spaces(strip_illegal_char(addr))
		tmpf=self.data_dir+os.sep+"_place_address.txt"
		if not os.path.exists(tmpf):
			with open(tmpf,"w") as f:
				f.write(addr)
		self.check_assumption()
		if iprint>=1: print("_place_address written")
		return fpath

	def download_graph_nx(self,bbox_snwe=[]):
		if bbox_snwe==[]: bbox_snwe = self._bbox_snwe
		self._graph =     download_graph(bbox_snwe)
		self._major_nids =     get_important_node_ids(self._graph)
		if iprint>=1: print(len(self._major_nids),"major_nids")

	def purge_all_data(self,):
		print("purging all data: "+self.data_dir)
		cmd ="rm -rf "+ self.data_dir
		print(cmd)
		subprocess.call(cmd.split())

	def get_osm_file_quote(self,):
		QUOTE=get_osm_file_quote_given_file(self._osm_file)
		return QUOTE

	def check_assumption(self,):
		''' some version use ='', some use ="" '''
		return '"'==self.get_osm_file_quote()


if __name__ == "__main__":

	if 0:
		gg = osmPipeline(folder_path=mypydir+"/urbana")
		gg.download_osm_given_address("Champaign,IL,USA")
		gg.write_node_elevation()
		gg.write_way_speed()
		gg.write_important_nodes()

