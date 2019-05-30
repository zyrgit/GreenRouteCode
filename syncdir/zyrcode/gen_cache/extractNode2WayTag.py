#!/usr/bin/env python

import os, sys, getpass
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/../mytools"
if addpath not in sys.path: sys.path.append(addpath)
from geo import yield_obj_from_osm_file,get_osm_file_quote_given_file

iprint = 2


def gen_cache_file(fin,fout,overwrite):
	if os.path.exists(fout) and not overwrite:
		return
	header=["nodeid_tuple","tag"]
	dtype = [(int,int),str]
	keyPos = 0
	QUOTE=get_osm_file_quote_given_file(fin)
	
	import cPickle as pickle
	res={}
	res["header"]=header
	res["dtype"]=dtype
	res["keyPos"]=keyPos
	cnt=0
	thresh=10

	for da in yield_obj_from_osm_file("way", fin):
		cnt+=1
		if cnt>thresh: 
			thresh*=2 
			print(__file__, cnt)
		nlist=[]
		tg=""
		for e in da:
			if e.startswith("<nd "):
				nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
				nlist.append(nid)
			elif e.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)): #<tag k='highway' v='residential' />
				tg=e.split(' v=')[-1].split(QUOTE)[1]
		for i in range(len(nlist)-1):
			res[(nlist[i],nlist[i+1])] = tg
			res[(nlist[i+1],nlist[i])] = tg
	if iprint: print("pickle.dump "+fout)
	pickle.dump(res, open(fout,"wb")) 
	return

if __name__ == "__main__": # not in use
	if iprint: print(sys.argv)
	if len(sys.argv)>=4:
		if "gen_cache_file" == sys.argv[1]:
			overwrite = False
			if len(sys.argv)>=5:
				for i in range(4,len(sys.argv)):
					if sys.argv[i].lower().startswith('overwrite'):
						st = sys.argv[i].split("=",1)[-1].strip().lower()
						overwrite= st =='true'
			
			gen_cache_file(sys.argv[2],sys.argv[3], overwrite=overwrite)

