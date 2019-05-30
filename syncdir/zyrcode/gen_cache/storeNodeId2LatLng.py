#!/usr/bin/env python

import os, sys, getpass
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/../mytools"
if addpath not in sys.path: sys.path.append(addpath)
from geo import get_osm_file_quote_given_file

iprint = 2

'''
	<node id="2521239904" lat="40.1470361" lon="-88.2576489" version="1"/>
	<node id="2521239905" lat="40.1471468" lon="-88.254888" version="1">
			<tag k="shop" v="photo"/>
			<tag k="phone" v="+1 217 3521322"/>
	</node>
'''

def gen_cache_file(fin,fout,overwrite):
	QUOTE=get_osm_file_quote_given_file(fin)
	print("QUOTE=",QUOTE)
	header=["nodeid","lat","lng"]
	dtype = [int,float,float]
	keyPos = 0
	if os.path.exists(fout) and not overwrite:
		print(fout+" already exists! skip")
		return
	import cPickle as pickle
	res={}
	res["header"]=header
	res["dtype"]=dtype
	res["keyPos"]=keyPos
	with open(fin,"r") as f:
		lcnt = 0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__, cnt)
			if l.strip().startswith("<node "):
				nidstr= l.split(" id=",1)[-1].split(" ",1)[0].strip(QUOTE)
				latstr= l.split(" lat=",1)[-1].split(" ",1)[0].strip(QUOTE)
				lonstr= l.split(" lon=",1)[-1].split(" ",1)[0].strip(QUOTE)
				res[int(nidstr)]= [float(latstr) , float(lonstr)]
				lcnt+=1
		if iprint: print("\n%d nodes !\n"%lcnt)
	if iprint: print("pickle.dump "+fout)
	pickle.dump(res, open(fout,"wb")) 
	return


if __name__ == "__main__":
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

