#!/usr/bin/env python

import os, sys, getpass
iprint = 2

'''
5270448216 229.90
5270448217 229.81
5270448218 229.99
'''

def gen_cache_file(fin,fout,overwrite):
	if os.path.exists(fout) and not overwrite:
		return
	header=["nodeid","elevation"]
	dtype = [int,float]
	keyPos = 0
	
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
			st=l.split(" ")
			if len(st)==2:
				nidstr= st[0]
				ele= st[1]
				res[int(nidstr)]= float(ele)
				lcnt+=1
		if iprint: print("\ngen_cache_file  num: %d\n"%lcnt)
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

