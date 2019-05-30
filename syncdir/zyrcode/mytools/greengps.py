#!/usr/bin/env python

import os, sys, getpass
import subprocess
import random, time
import inspect
import collections
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir)

from readconf import get_conf,get_conf_int,get_conf_float,get_list_startswith,get_dic_startswith

iprint=2
configfile = "conf.txt"
DirGreengps = get_conf(configfile,"DirGreengps")
assert(DirGreengps!="")
CUT = get_conf(configfile,"CUT")
pakFile=DirGreengps+os.sep+"champaign.extracted.geofabrik.sep2011.augmented.pak"
nodesAndStopsFile=DirGreengps+os.sep+"champaign.extracted.geofabrik.sep2011.augmented.nodesAndstops"
GramPerGallon = get_conf_float(configfile,"GramPerGallon")
Bound_N_lat=40.157605
Bound_S_lat=40.020233
Bound_W_lng=-88.314759
Bound_E_lng=-88.128484
def inBound(lat,lng):
	if (lat<Bound_N_lat and lat>Bound_S_lat and lng<Bound_E_lng and lng>Bound_W_lng):
		return True
	return False

def query_coef_str(make ,model ,year ,vclass):
	cmd=DirGreengps+os.sep+"query "+DirGreengps+os.sep+"2014-1-30_u44_[CoeffsOrdered]_model_db_LS_wok7.txt "+make+" "+model+" "+year+" "+vclass
	pp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
	pout = pp.communicate()[0].rstrip() 
	k15=["0",'1.531e-06', '4.286e-04', '8.625e-06', '-6.668e-07', "0","0",'0', '0', '0', '0', '4.385e-09', '-1.351e-09', '4.815e-11', '1.107e-09']
	try:
		st = pout.split("\t") # just 12 of them, not 15.
		k15[1]=st[0]
		k15[2]=st[1]
		k15[3]=st[2]
		k15[4]=st[3]
		k15[7]=st[4]
		k15[8]=st[5]
		k15[9]=st[6]
		k15[10]=st[7]
		k15[11]=st[8]
		k15[12]=st[9]
		k15[13]=st[10]
		k15[14]=st[11]
	except:
		pass

	cmd=DirGreengps+os.sep+"query "+DirGreengps+os.sep+"2014-1-30_u44_[CoeffsOrdered]_model_db_NNLS.txt "+make+" "+model+" "+year+" "+vclass
	pp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
	pout = pp.communicate()[0].rstrip()
	k15nn=["0",'0.000000e+00', '0.000000e+00', '1.238523e-05', '0.000000e+00', "0","0",'0.000000e+00', '0.000000e+00', '0.000000e+00', '2.916069e-03', '1.930056e-09', '0.000000e+00', '0.000000e+00', '7.400955e-10']
	try:
		st = pout.split("\t")
		k15nn[1]=st[0]
		k15nn[2]=st[1]
		k15nn[3]=st[2]
		k15nn[4]=st[3]
		k15nn[7]=st[4]
		k15nn[8]=st[5]
		k15nn[9]=st[6]
		k15nn[10]=st[7]
		k15nn[11]=st[8]
		k15nn[12]=st[9]
		k15nn[13]=st[10]
		k15nn[14]=st[11]
	except:
		pass

	return k15,k15nn



def route_green_coef_donot_import(startLat,startLng,endLat,endLng,k15,k15nn):# float,,, [str],
	mass=3086
	area=24.45
	cdrag=0.28
	params=[startLat,startLng,endLat,endLng]
	for v in k15: params.append(v)
	for v in k15: params.append(v)
	params.append(mass)
	params.append(area)
	params.append(cdrag)
	params.append(nodesAndStopsFile)
	QUERY_STRING="flat=%.7f&flon=%.7f&tlat=%.7f&tlon=%.7f&fast=2&v=motorcar&k0=%s&k1=%s&k2=%s&k3=%s&k4=%s&k5=%s&k6=%s&k7a=%s&k7b=%s&k7c=%s&k7d=%s&k8a=%s&k8b=%s&k8c=%s&k8d=%s&k0_nnls=%s&k1_nnls=%s&k2_nnls=%s&k3_nnls=%s&k4_nnls=%s&k5_nnls=%s&k6_nnls=%s&k7a_nnls=%s&k7b_nnls=%s&k7c_nnls=%s&k7d_nnls=%s&k8a_nnls=%s&k8b_nnls=%s&k8c_nnls=%s&k8d_nnls=%s&m=%d&a=%.2f&cd=%.2f&nodesAndStopsFile=%s"%tuple(params)
	cmd= DirGreengps+os.sep+"gosmore %s"%pakFile
	my_env = os.environ.copy()
	my_env["QUERY_STRING"] = QUERY_STRING
	pp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, env=my_env)
	pout = pp.communicate()[0].rstrip()
	return pout


def route_green(startLat,startLng,endLat,endLng, make="",model="",year="",vclass=""):
	if not (inBound(startLat,startLng) and inBound(endLat,endLng)): 
		return []
	if make=="undefined": make=""
	if model=="undefined": model=""
	if year=="undefined": year=""
	if vclass=="undefined": vclass=""

	if (make=="") and (model==""): # can't be both absent 
		make="Ford"
		model="Taurus"
		if iprint>=2: print(make,model)

	k15,k15nn= query_coef_str(make ,model ,year ,vclass)
	res= route_green_coef_donot_import(startLat,startLng,endLat,endLng,k15,k15nn).split("\n")
	gpstrace=""
	ttgas = 0.0
	dist = 0.0
	ttime = 0.0
	mpg = 0.0
	for l in res:
		l=l.strip()
		if l =="": continue
		if not l[0].isalpha(): # 40.104116,-88.234497,J,residential,46,Euclid St
			st=l.split(",",2)
			if len(st)>1:
				gpstrace+=st[0]+","+st[1]+CUT
		elif l.startswith("Total Expected Gas"):
			ttgas = float(l.split(",")[1]) * GramPerGallon
		elif l.startswith("Total Distance"):
			dist = float(l.split(",")[1])
		elif l.startswith("Total Time"):
			ttime = float(l.split(",")[1])
		elif l.startswith("MPG"):
			mpg = float(l.split(",")[1])
	return [gpstrace.rstrip(CUT),ttgas,ttime,mpg,dist]

# 40.104116,-88.234497,J,residential,46,Euclid St
# Total Expected Gas Usage, 0.048112
# Total Distance in Miles, 1.4355
# Total Time in Minutes, 4.5937
# MPG, 29.84

if __name__ == "__main__":
	startLat=40.1138
	startLng=-88.2246
	endLat=40.101079
	endLng=-88.235935
	print route_green(startLat,startLng,endLat,endLng)


