#!/usr/bin/env python

import os, sys
import inspect

CUT='='

def get_conf(fpath,typ, firstbreak = True, delimiter=CUT):
	res=''
	try:
		if not ( fpath.startswith('/') ):
			abspath=os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
			fpath = abspath+"/../"+ fpath
		conf=open(fpath,'r')
	except:
		print(fpath,"get_conf   NOT   Found !")
		return res
	try:
		for line in conf:
			if line.split(delimiter,1)[0].strip()==typ: 
				res=line.split(delimiter,1)[1].split('#')[0].strip()
				if firstbreak:
					break # last/first one wins
		conf.close()
	except:
		print(fpath,"get_conf   delimiter   Error !")
	
	return res

def get_conf_int(fpath,typ):
	return int(get_conf(fpath,typ))

def get_conf_float(fpath,typ):
	return float(get_conf(fpath,typ))

def get_conf_str(fpath,typ):
	return (get_conf(fpath,typ))

def get_list_startswith(fpath,typ, delimiter=" "): # tarekc =1 2 3 4
	res=[]
	tmp = get_conf(fpath,typ)
	if tmp=="":
		return res
	res=tmp.split(delimiter)
	return [r.strip() for r in res]

def get_dic_startswith(fpath,entryName, delimiter=" "): # tarekc = fft:256 epc:10
	res={}
	tmp = get_conf(fpath,entryName)
	if tmp=="":
		return res
	st=tmp.split(delimiter)
	for n in st:
		kv = n.split(":")
		res[kv[0].strip()]=kv[1].strip()
	return res

