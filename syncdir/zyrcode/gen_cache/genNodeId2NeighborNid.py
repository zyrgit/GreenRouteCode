#!/usr/bin/env python

import os, sys, getpass
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/../mytools"
if addpath not in sys.path: sys.path.append(addpath)
from geo import get_osm_file_quote_given_file,Highway_Routable_types

iprint = 2

'''
	<way id="538590722" version="1">
		<nd ref="3702623502"/>
		<nd ref="5214549069"/>
		<nd ref="5213079544"/>
		<tag k="highway" v="service"/>
		<tag k="service" v="driveway"/>
	</way>
'''

def gen_cache_file(fin,fout,overwrite):
	QUOTE=get_osm_file_quote_given_file(fin)
	header=["nodeid","nodeid_list"]
	dtype = [int,int]
	keyPos = 0
	if os.path.exists(fout) and not overwrite:
		return
	import cPickle as pickle
	res={}
	res["header"]=header
	res["dtype"]=dtype
	res["keyPos"]=keyPos
	kInit=1
	kWithin=2
	state=kInit
	with open(fin,"r") as f:
		lcnt=0
		cnt=0
		thresh=10
		for l in f:
			cnt+=1
			if cnt>thresh: 
				thresh*=2 
				print(__file__, cnt)
			lcnt+=1
			l=l.strip()
			if state==kInit:
				if l.startswith("<way "):
					state=kWithin
					nlist=[]
					valid=0
			elif state==kWithin:
				if l.startswith("<nd "):
					nidstr= l.split(' ref=%s'%QUOTE,1)[-1].split(QUOTE,1)[0]
					nlist.append(int(nidstr))
				elif l.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)):
					v=l.split(" v=")[-1].split(QUOTE)[1]
					if v in Highway_Routable_types:
						valid=1
				elif l.startswith("</way>"):
					state=kInit
					if valid>0:
						for i in range(len(nlist)):
							if nlist[i] not in res:
								res[ nlist[i] ]=[]
							if i>0:
								if nlist[i-1] not in res[ nlist[i] ]:
									res[ nlist[i] ].append(nlist[i-1])
							if i<len(nlist)-1:
								if nlist[i+1] not in res[ nlist[i] ]:
									res[ nlist[i] ].append(nlist[i+1])

				elif l.startswith("<way"):
					raise Exception("Bug: <way> no closure at line %d!"%lcnt)

	if iprint: print("\nsize %d !\n"%(len(res)-3))
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
