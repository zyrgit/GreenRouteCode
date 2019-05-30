#!/usr/bin/env python

import inspect, glob, os, sys
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
HomeDir = os.path.expanduser("~")

iprint =2
OSMBaseDir = HomeDir+"/greendrive/osmdata/"

def parse(fn):
	with open(fn,"r") as f:
		st=f.readline()
	st=st.split(" ")
	lats,latn,lngw,lnge =[float(x) for x in st]
	lat=(lats+latn)/2.0
	lng=(lngw+lnge)/2.0
	return lat,lng

if __name__ == "__main__":
	arglist=sys.argv[1:]

	if 1 and "addr" in arglist:
		for i in range(len(arglist)):
			if arglist[i]=="addr":
				addr=arglist[i+1]
				try:
					lat,lng= parse(OSMBaseDir+addr+os.sep+"_bbox_snwe.txt")
					print("%f %f"%(lat,lng))
				except:
					lat,lng= parse(mypydir+os.sep+"_bbox_snwe.txt")
					print("%f %f"%(lat,lng))
				

