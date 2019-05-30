#!/usr/bin/env python
# this is the fuel prediction server running on PC
# it responds to the request from web/greenmap server.
import os, sys, traceback
import subprocess, pprint
import random, time
import urllib2
import requests
import threading, thread
import httplib
import json
import cgi
import ast
import inspect
from copy import deepcopy
import logging,glob
import socket
import SocketServer
import signal
from datetime import datetime, timedelta
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from os import curdir, sep
import urlparse
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
if mypydir not in sys.path: sys.path.append(mypydir)
if mypydir+'/mytools' not in sys.path: sys.path.append(mypydir+'/mytools')
from costModule import *
HomeDir = os.path.expanduser('~')


configfile="conf.txt"
CUT=get_conf(configfile,"CUT")
SERVER_ADDR="0.0.0.0" 
SERVER_PORT = get_conf_int(configfile,"port")
l = SimpleAppendLogger("./logs/"+__file__.split(os.sep)[-1]+".log",500) # KB 
DateFormat = "%a %b %d %H:%M %Y"
l.lg_list(["server_run", datetime.now().strftime(DateFormat), get_my_ip()])

iprint = 1
pathbeg = get_conf(configfile,"pathbeg") 
pathbeg = pathbeg.rstrip("/")
assert(pathbeg.startswith("/"))
redis_port = get_conf_int(configfile,"redis_port") 
use_gzip = 0
latlng_to_city_state_country(44.,-88.) # pre-load geo module, avoid waiting later.


def gen_latlngs_given_2latlng(latlng1,latlng2,addr, backend=None, mm_nid2latlng=None, print_res=False):
	''' Get GPS latlngs trace given O/D. Calling costModule.py funcs. 
	'''
	nodeids,addr,gpslist,_gas = gen_nodeids_given_2latlng(latlng1,latlng2, addr=addr, backend=backend, print_res=print_res) # from costModule.py 
	if nodeids is not None:
		if mm_nid2latlng is None:
			mm_nid2latlng = CacheManager(overwrite_prefix=True)
		if mm_nid2latlng.get_id()!="osm/cache-%s-nodeid-to-lat-lng.txt"%addr:
			mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr, ignore_invalid_mem=True)
		wstr=""
		has_none=0
		has_valid=0
		for nid in nodeids:
			latlng=mm_nid2latlng.get(nid)
			if latlng is None: 
				has_none+=1
				if iprint>=1 or print_res: 
					print("mm_nid2latlng.get None! Check mem",nid)
					print(mm_nid2latlng.get_id())
			else: 
				has_valid+=1
				wstr+="%.5f,%.5f%s"%(latlng[0],latlng[1],"~|")
		if has_valid<has_none: # mm not loaded, may appear sparse.
			wstr=""
			for latlng in gpslist:
				wstr+="%.6f,%.6f%s"%(latlng[0],latlng[1],"~|")
		if print_res: 
			print("GPS pts, #valid",has_valid,'#none',has_none)
			print('res: '+wstr)
			print(addr,mm_nid2latlng.mm.prefix)
		return wstr.rstrip("~|"),nodeids,addr
	else:
		return "",[],addr


class myHandler(BaseHTTPRequestHandler):
	def do_GET(self):
		if iprint>=2: print("do_GET, path="+self.path)
		try:
			sendReply = True
			if sendReply :
				self.send_response(200)
				if "%s/healthcheck"%pathbeg == self.path:
					if iprint>=2: print("checking health...")
					content="<html><body><h1>ok</h1></body></html>"
					self.send_header("Content-length", str(len(str(content))))
					self.send_header('Content-type','text/html;charset=utf-8')
					self.end_headers()
					self.wfile.write(content)
					self.wfile.flush()
			return
		except IOError:
			self.send_error(404,'File Not Found: %s' % self.path)

	def do_POST(self):
		if iprint>=2 : print("do_POST, path="+self.path)

		if self.path=="/html/route":
			form = cgi.FieldStorage(
				fp=self.rfile, 
				headers=self.headers,
				environ={'REQUEST_METHOD':'POST',
		                 'CONTENT_TYPE':self.headers['Content-Type'],
			})
			if iprint>=2: 
				print self.headers['Content-Type']
				print self.headers['Content-Length']
			startLat=float(form["startLat"].value)
			startLng=float(form["startLng"].value)
			endLat=float(form["endLat"].value)
			endLng=float(form["endLng"].value)
			latlng1,latlng2=[startLat,startLng],[endLat,endLng]
			addr=latlng_to_city_state_country(float(startLat),float(startLng),no_space=True)


			gpstrace,nodeids,addr = gen_latlngs_given_2latlng(latlng1,latlng2, addr=addr, mm_nid2latlng=mm_nid2latlng, print_res=False)


			data={"gpstrace":gpstrace}

			self.send_response(200)
			self.end_headers()
			self.wfile.write(json.dumps(data))
			if iprint>=2 : print self.client_address[0] # it's haproxy addr...
			return	

		if self.path=="/html/fuel":
			form = cgi.FieldStorage(
				fp=self.rfile, 
				headers=self.headers,
				environ={'REQUEST_METHOD':'POST',
		                 'CONTENT_TYPE':self.headers['Content-Type'],
			})
			if iprint>=2: 
				print self.headers['Content-Type']
				print self.headers['Content-Length']
			startLat=float(form["startLat"].value)
			startLng=float(form["startLng"].value)
			endLat=float(form["endLat"].value)
			endLng=float(form["endLng"].value)
			latlng1,latlng2=[startLat,startLng],[endLat,endLng]
			addr=latlng_to_city_state_country(float(startLat),float(startLng),no_space=True)

			ggas = get_fuel_google(latlng1,latlng2,addr)# from costModule.py 
			fgas= get_fuel_given_latlng_list([latlng1,latlng2],addr)


			data={"google-fuel":ggas, "green-fuel":fgas}
			if iprint>=2: 
				print(latlng1,latlng2)
				print(data)

			self.send_response(200)
			self.end_headers()
			self.wfile.write(json.dumps(data))
			return	



	def _set_headers(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/html')
		self.end_headers()

	def do_HEAD(self):
		self._set_headers()
		

if __name__ == "__main__":
	try:
		#Create a web server and define the handler to manage the incoming request
		server = HTTPServer((SERVER_ADDR, SERVER_PORT), myHandler)
		print 'Started httpserver addr %s port %s' %(SERVER_ADDR,SERVER_PORT)
		
		#Wait forever for incoming htto requests
		server.serve_forever()

	except KeyboardInterrupt:
		print '^C received, shutting down the web server'
		server.socket.close()

