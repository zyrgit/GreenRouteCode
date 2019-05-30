#!/usr/bin/env python

import os, sys, getpass
iprint = 2


def gen_cache_file(fin,fout,overwrite):
	raise Exception("major nid not in use!")
	if os.path.exists(fout) and not overwrite:
		return
	header=["nodeid","int"]
	dtype = [int,int]
	keyPos = 0

	import cPickle as pickle
	res={}
	res["header"]=header
	res["dtype"]=dtype
	res["keyPos"]=keyPos

	with open(fin,"r") as f:
		lcnt=0
		for l in f:
			lcnt+=1
			l=l.strip()
			res[int(l)]=1

	if iprint: print("\nsize %d !\n"%(len(res)-3))
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
