#!/usr/bin/env python

import os, sys, getpass
import subprocess
import random, time
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir)
from namehostip import get_my_ip
from hostip import ip2tarekc
import datetime
import glob
from shutil import copy2, move as movefile
HomeDir = os.path.expanduser("~")

_iprint = 1
iprintverb =0
folder  = "log/"
fnamePrefix = "%slog-"%folder

class Logger: 
	''' Used for individual server;  '''
	def __init__(self ,tag=""):
		self.lg_index=0
		self.my_ip = get_my_ip()
		if not os.path.exists(folder):
			os.makedirs(folder)
		try:
			self.my_tname = tag+ ip2tarekc[self.my_ip]
		except:
			self.my_tname = tag+ self.my_ip.split(".",2)[-1]
			if _iprint>=2: print(self.my_tname)
		self.fd_list=[]
		self.fnames =[]
		self.freshness = 0 
		tmp = glob.glob(folder+"log*") # log/log*
		for fn in tmp:
			try:
				st = fn.replace(fnamePrefix+self.my_tname+"-","").split("-",1)
				ind = int(st[0])+1
				newfn = fnamePrefix+self.my_tname+"-%d-"%ind+st[-1]
				movefile(fn,newfn)
				if _iprint>=2: print(fn,newfn)
			except:
				pass
		fmain = fnamePrefix+self.my_tname+"-0-"+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+".txt"
		if _iprint>=1: print("create",fmain)
		fd = open(fmain,"w")
		self.fd_list.append(fd)
		self.fnames.append(fmain)
		self.lg(self.my_tname)
		self.lg(self.my_ip)
		self.lg(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		self.lg(time.time())
		self.lg("\n")

	def lg(self, st, i=-1):
		if i<0:
			i=self.lg_index
		st=str(st)
		if not st.endswith("\n"):
			st=st+"\n"
		self.fd_list[i].write(st)

	def overwrite(self,st,i=-1):
		if i<0:
			i=self.lg_index
		self.fd_list[i].close()
		self.fd_list[i] = open(self.fnames[i],"w")
		self.lg(self.my_tname,i)
		self.lg(self.my_ip,i)
		self.lg(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),i)
		self.lg(time.time(),i)
		self.lg("\n",i)
		self.lg(st,i)

	def lg_new(self, st=""):
		ind = len(self.fd_list) 
		fn =fnamePrefix+self.my_tname+"-0-"+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+"-"+str(ind)+".txt"
		self.fd_list.append(open(fn,"w"))
		self.fnames.append(fn)
		if _iprint>=1: print("create2",fn)
		self.lg(self.my_tname,ind)
		self.lg(self.my_ip,ind)
		self.lg(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),ind)
		self.lg(time.time(),ind)
		self.lg("\n",ind)
		if st!="":
			self.lg(st,ind)
		return ind

	def set_lg_index(self,ind):
		if ind>=0:
			self.lg_index=ind

	def lg_list(self,ls,i=-1):
		st=""
		for x in ls:
			st = st+ str(x) + " "
		self.lg(st,i)
	def lg_dict(self,dic,i=-1):
		for k,v in dic.items():
			self.lg(str(k)+" = "+str(v),i)
	
	def flush(self,):
		for fd in self.fd_list:
			fd.flush()
	def print_file_names(self,):
		for fn in self.fnames:
			print(fn)
	def __del__(self):
		for fd in self.fd_list:
			fd.close()


class SimpleAppendLogger: 
	''' multi servers contribute to 1 file, but don't overwrite. '''
	def __init__(self ,fname, maxsize=1000, overwrite=False): 
		self.fname=fname
		self.maxsize=maxsize
		self.fd_list=[]
		tmp = glob.glob(self.fname) 
		for fn in tmp:
			try:
				sz = float(os.stat(fn).st_size)/1024
				if sz>=maxsize:
					os.remove(fn)
			except:
				pass
		tmpdir = os.path.dirname(self.fname)
		if not os.path.exists(tmpdir):
			os.makedirs(tmpdir)
		if not overwrite:
			fd = open(self.fname,"a")
		else:
			fd = open(self.fname,"w")
		self.fd_list.append(fd)
		self.my_ip = get_my_ip()
		self.lg("\n")
		self.lg(self.my_ip)
		self.lg(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		self.lg(time.time())
		i=0
		self.fd_list[i].close()
		self.history={}
	def lg(self, st, i=-1):
		i=0
		if float(os.stat(self.fname).st_size)/1024>=self.maxsize:
			self.fd_list[i] = open(self.fname,"w")
			self.fd_list[i].close()
		self.fd_list[i] = open(self.fname,"a")
		st=str(st)
		if not st.endswith("\n"):
			st=st+"\n"
		self.fd_list[i].write(st)
		self.fd_list[i].close()
	def lg_list(self,ls,i=-1):
		i=0
		st=""
		for x in ls:
			st = st+ str(x) + " "
		self.lg(st,i)
	def lg_dict(self,dic,i=-1):
		i=0
		for k,v in dic.items():
			self.lg(str(k)+" = "+str(v),i)
	def print_file_names(self,):
		print(self.fname)
	def lg_str_once(self,wstr):
		''' remember what logged so only first occur written.'''
		wstr=str(wstr)
		if wstr in self.history: return
		self.lg(wstr)
		self.history[wstr]=1


class ErrorLogger: # under ~/errorlogger/*.log , fetched by file-server to view.
	def __init__(self,fname, tag="", maxsize=10000): # max log size KB 
		self.fname= HomeDir+os.sep+"errorlogger"+os.sep+ fname
		self.tag= fname if tag=="" else tag
		if _iprint>=2: print("ErrorLogger created at: "+self.fname+", TAG "+self.tag)
		self.maxsize=maxsize
		self.fd_list=[]
		tmp = glob.glob(self.fname) 
		for fn in tmp:
			try:
				sz = float(os.stat(fn).st_size)/1024
				if sz>=maxsize:
					os.remove(fn)
			except:
				pass
		tmpdir = os.path.dirname(self.fname)
		if not os.path.exists(tmpdir):
			os.makedirs(tmpdir)
		fd = open(self.fname,"a")
		self.fd_list.append(fd)
		self.my_ip = get_my_ip()
		self.create_time= (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		i=0
		self.fd_list[i].close()
	def lg(self, st, i=-1):
		i=0
		if float(os.stat(self.fname).st_size)/1024>=self.maxsize:
			self.fd_list[i] = open(self.fname,"w")
			self.fd_list[i].close()
		self.fd_list[i] = open(self.fname,"a")
		st=str(st)
		if not st.endswith("\n"):
			st=st+"\n"
		self.fd_list[i].write(st)
		self.fd_list[i].close()
	def lg_err(self):
		i=0
		self.fd_list[i] = open(self.fname,"a")
		self.fd_list[i].write("ERROR "+self.tag+" "+self.my_ip+" "+datetime.datetime.now().strftime("%Y-%m-%d,%H:%M:%S")+" ~| ")
		self.fd_list[i].close()
	def lg_list(self,ls,i=-1):
		i=0
		st=""
		self.lg_err()
		for x in ls:
			st = st+ str(x) + " "
		self.lg(st,i)
	def lg_dict(self,dic,i=-1):
		i=0
		self.lg_err()
		for k,v in dic.items():
			self.lg(str(k)+" = "+str(v),i)
	def print_file_names(self,):
		print(self.fname)


