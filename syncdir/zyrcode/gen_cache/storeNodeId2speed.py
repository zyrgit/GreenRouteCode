#!/usr/bin/env python

import os, sys, getpass
iprint = 2

'''
38005439,38005440,22.13
37961359,38047476,11.63
37951223,37961359,13.37
'''

def gen_cache_file(fin,fout,overwrite):
	if os.path.exists(fout) and not overwrite:
		return
	header=["nodeid_tuple","speed"]
	dtype = [(int,int),float]
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
			st=l.split(",")
			if len(st)==3:
				nid1= int(st[0])
				nid2= int(st[1])
				spd= st[-1]
				res[(nid1,nid2)]= float(spd)
				lcnt+=1
		if iprint: print("\ngen_cache_file  num: %d\n"%lcnt)
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

