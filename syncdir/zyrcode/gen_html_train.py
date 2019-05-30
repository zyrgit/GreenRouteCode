#!/usr/bin/env python

import os, sys
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
addpath=mypydir+"/code"
if addpath not in sys.path: sys.path.append(addpath)
from myosrm import match_trace_listOfDict
try:
	from common import *
except: print('\nERR from common import *\n')

iprint = 2


kCutTraceTimeGap = 2 # timestamp between two lines > this then cut.
kCutTraceDistGap = kCutTraceTimeGap* 50 # dist between two lines > this then cut.
kNumMajorSegForTrain = 5
kMinNumMajorLastSegs = 3 # >= this last batch is valid.

''' myosrm.py also has gen html code using template.html '''

''' Used by 5.py gen trace for debug '''
def gen_html_given_path_nid_ind_list(path,nid_ind_list,htmlf,addr,truetimestr="",comment="",letterMajorNodes=False,add_tail=False,loc_pt=[]):
	osmname=addr
	meta_nid2latlng= "osm/cache-%s-nodeid-to-lat-lng.txt"%osmname
	mm_nid2latlng.use_cache(meta_file_name=meta_nid2latlng)
	meta_nid2neighbor= "osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname
	mm_nid2neighbor.use_cache(meta_file_name=meta_nid2neighbor)
	html_paths=[] # list of latlng lists
	html_heats=[] # gas marker
	def add_new():
		html_paths.append([])
		html_heats.append([])
	def add_sp(latlng,gas):
		html_heats[-1].append([latlng,gas])
	def add_node(latlng,gas=0):
		html_paths[-1].append(latlng)

	startpos = nid_ind_list[0][1]
	print("startpos gen_html_",startpos)
	print("gen_html_",nid_ind_list)

	add_new()
	for i in range(len(nid_ind_list)-1):
		for j in range(nid_ind_list[i][1],max(1+nid_ind_list[i][1],nid_ind_list[i+1][1])):
			dic=path[j]
			add_sp([dic[KeyGPSLat],dic[KeyGPSLng]], dic[KeyGas])
		nid=nid_ind_list[i][0]
		latlng0 = mm_nid2latlng.get(nid)
		add_node(latlng0)
		nid2=nid_ind_list[i+1][0]
		if letterMajorNodes:
			if i==len(nid_ind_list)-2 or len(mm_nid2neighbor.get(nid2))>2:
				add_node(mm_nid2latlng.get(nid2))
				if i!=len(nid_ind_list)-2: 
					add_new()
				elif add_tail:
					for k in range(nid_ind_list[-1][1],min(len(path),50+nid_ind_list[-1][1])):
						dic=path[k]
						add_sp([dic[KeyGPSLat],dic[KeyGPSLng]], dic[KeyGas])
		else:
			if i!=len(nid_ind_list)-2: 
				add_node(mm_nid2latlng.get(nid2))
				add_new()
			if i==len(nid_ind_list)-2:
				add_node(mm_nid2latlng.get(nid2))
				if add_tail:
					for k in range(nid_ind_list[-1][1],min(len(path),50+nid_ind_list[-1][1])):
						dic=path[k]
						add_sp([dic[KeyGPSLat],dic[KeyGPSLng]], dic[KeyGas])
						
	# gen html:
	of = open(htmlf,"w")
	if iprint>=2: print("generating html: "+htmlf)
	timestr=""
	if truetimestr!="":
		timestr=unix2datetime(truetimestr)
	if len(html_paths)==0:
		print("\n! empty "+htmlf)
		print("  Time: "+timestr)
		return
	''' html_paths=[[latlng,],] 
		html_heats=[[[latlng,gas], ], ]  '''
		
	kInit=0
	kInsert1=1
	kInsert2=2
	kInsert3=3
	kInsert4=4
	kInsert5=5
	kInsert6=6
	state = kInit
	lstr = "new google.maps.LatLng("
	rstr = "), "

	with open(mypydir+"/template.html","r") as f:
		for l in f:
			if state==kInit:
				if l.startswith("////startinsertlatlng1"):
					state=kInsert1
					of.write("//timestr="+timestr+"\n")
					for p in html_paths:
						of.write("[")
						for latlng in p:
							of.write(lstr+str(latlng[0])+","+str(latlng[1])+rstr)
						of.write("],\n")
				elif l.startswith("////startinsertlatlng2"):
					state=kInsert2
					for i in range(len(html_paths)):
						of.write("function check%d() {\n"%i)
						of.write("if (myform.chk%d.checked == true) {\n"%i)
						of.write("set_checked_index(%d,true); }else{\n"%i)
						of.write("set_checked_index(%d,false);}}\n"%i)
				elif l.startswith("<!-- ////startinsertlatlng3 -->"):
					state=kInsert3
					for i in range(len(html_paths)):
						of.write('<input name="chk%d" type=checkbox checked onClick="check%d()"> <b><span style="color:green">%s</span></b> <br/>\n'%(i,i,chr(i%26+65)))
				elif l.startswith("////insertheatstart1"):
					state=kInsert4
					for p in html_heats:
						'''[
						[{lat: 40.1138, lng: -88.2246},0],
						[{lat: 40.1136, lng: -88.2246},1],
						],'''
						of.write("[")
						for latlngval in p:
							latlng=latlngval[0]
							val=latlngval[1]
							of.write("[{lat:"+str(latlng[0])+",lng:"+str(latlng[1])+"},"+str(val)+"],\n")
						of.write("],\n")
				elif l.startswith("<!-- <title>start</title> -->"):
					state=kInsert5
					of.write("<title>%s</title>\n"%timestr)
					of.write("<!-- %s -->\n"%comment)
				elif l.startswith("///startinsertpt"):
					state=kInsert6
					if loc_pt is not None and len(loc_pt)>0:
						of.write("  var endLatLng=new google.maps.LatLng(%.6f,%.6f);\n"%(loc_pt[0],loc_pt[-1]))

				else:
					of.write(l)

			elif state == kInsert1:
				if l.startswith("////endinsertlatlng1"):
					state=kInit
			elif state == kInsert2:
				if l.startswith("////endinsertlatlng2"):
					state=kInit
			elif state == kInsert3:
				if l.startswith("<!-- ////endinsertlatlng3 -->"):
					state=kInit
			elif state == kInsert4:
				if l.startswith("////insertheatend1"):
					state=kInit
			elif state == kInsert5:
				if l.startswith("<!-- <title>end</title> -->"):
					state=kInit
			elif state == kInsert6:
				if l.startswith("///endinsertpt"):
					state=kInit
	of.close()



''' Used after genSpeedSegOSRM.py gen seg color '''
def gen_html_given_segspeed_turnpanalty(htmlf,addr):# not in use
	import math
	osmname=addr
	meta_nid2latlng= "osm/cache-%s-nodeid-to-lat-lng.txt"%osmname
	mm_nid2latlng.use_cache(meta_file_name=meta_nid2latlng)
	scoreCalc = ScoreCalculator({"sigmoida":40.0, "sigmoidx":20, "sigmoidy":0.1})
	OSMdir="/home/zhao97/greendrive/osmdata/"+addr+"/data/"
	seg_val_list=[]
	shift_latlng=0.00004
	with open(OSMdir+"segspeed.txt","r") as f:
		for l in f:
			st=l.split(",")
			nid0=int(st[0])
			nid1=int(st[1])
			val=float(st[2])
			lat0,lng0=mm_nid2latlng.get(nid0)
			lat1,lng1=mm_nid2latlng.get(nid1)
			hd01= get_bearing_latlng2([lat0,lng0],[lat1,lng1])
			''' shift to right-hand traveling side '''
			rad=(hd01+90.0)/180.0*math.pi
			shiftlng=math.sin(rad)*shift_latlng
			shiftlat=math.cos(rad)*shift_latlng
			sc=scoreCalc.getscore(val)
			seg_val_list.append([[lat0+shiftlat,lng0+shiftlng],[lat1+shiftlat,lng1+shiftlng],sc])
	gen_html_using_template_seg(htmlf, seg_val_list)



''' Used after gen cache way tag. '''
def gen_html_given_waytags(addr):# not in use
	center=[40.105295, -88.227844]
	osmfile=DirOSM+os.sep+addr+os.sep+addr+".osm"
	from geo import get_osm_file_quote_given_file
	QUOTE=get_osm_file_quote_given_file(osmfile) 

	mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr)
	seg_val_list=[]
	cnt=0
	for da in yield_obj_from_osm_file("way", osmfile):
		nlist=[]
		tg=""
		for e in da:
			if e.startswith("<nd "):
				nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
				nlist.append(nid)
			elif e.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)): #<tag k='highway' v='residential' />
				tg=e.split(' v=%s'%QUOTE)[-1].split(QUOTE)[0]
		for i in range(len(nlist)-1):
			latlng1 = mm_nid2latlng.get(nlist[i])
			if get_dist_meters_latlng2(center,latlng1)>10000:
				continue
			latlng2 = mm_nid2latlng.get(nlist[i+1])
			if latlng1 is not None and latlng2 is not None and tg!="":
				if tg in Highway_Fast_taglist:
					val=1
				elif tg in Highway_Slow_taglist:
					val=0.2
				elif tg=="unclassified":
					val=0
				else: val=0.5
				tmp1=latlng1[:]
				tmp2=latlng2[:]
				latlng1[0]=tmp1[0]*0.9+tmp2[0]*0.1
				latlng1[1]=tmp1[1]*0.9+tmp2[1]*0.1
				latlng2[0]=tmp2[0]*0.9+tmp1[0]*0.1
				latlng2[1]=tmp2[1]*0.9+tmp1[1]*0.1
				seg_val_list.append([latlng1,latlng2,val])
		if cnt%100==0: print(cnt,tg,val)
		cnt+=1
	gen_html_using_template_seg("waytag-%s.html"%addr,seg_val_list,"Way Tag")


def gen_diff_waytags_speeds(addr):# not in use
	center=[40.105295, -88.227844]
	osmfile=DirOSM+os.sep+addr+os.sep+addr+".osm"
	mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr)
	mm_nid2waytag.use_cache(meta_file_name="osm/cache-%s-nids-to-waytag.txt"%addr)
	mm_nids2speed.use_cache(meta_file_name="osm/cache-%s-nids-to-speed.txt"%addr)
	from geo import get_osm_file_quote_given_file
	QUOTE=get_osm_file_quote_given_file(osmfile)  

	seg_val_dic={0:[],1:[],2:[]} # service/residential/primary 
	cnt=0
	for da in yield_obj_from_osm_file("way", osmfile):
		nlist=[]
		tg=""
		for e in da:
			if e.startswith("<nd "):
				nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
				nlist.append(nid)
			elif e.startswith('<tag k=%shighway%s'%(QUOTE,QUOTE)): #<tag k='highway' v='residential' />
				tg=e.split(' v=%s'%QUOTE)[-1].split(QUOTE)[0]
		for i in range(len(nlist)-1):
			latlng1 = mm_nid2latlng.get(nlist[i])
			if get_dist_meters_latlng2(center,latlng1)>8000:
				continue
			latlng2 = mm_nid2latlng.get(nlist[i+1])
			if latlng1 is not None and latlng2 is not None and tg!="":
				spd = mm_nids2speed.get((nlist[i],nlist[i+1]))
				if tg in Highway_Fast_taglist:
					typ=2
					if spd is None:
						val=0.07
					elif spd<SpeedTagHigh:
						val=0.4
					else:
						val=1
				elif tg in Highway_Slow_taglist:
					typ=0
					if spd is None:
						val=0.07
					elif spd<SpeedTagLow:
						val=0.4
					else:
						val=1
				else: 
					typ=1
					if spd is None:
						val=0.07
					elif spd<SpeedTagMedium:
						val=0.4
					else:
						val=1
				tmp1=latlng1[:]
				tmp2=latlng2[:]
				latlng1[0]=tmp1[0]*0.9+tmp2[0]*0.1
				latlng1[1]=tmp1[1]*0.9+tmp2[1]*0.1
				latlng2[0]=tmp2[0]*0.9+tmp1[0]*0.1
				latlng2[1]=tmp2[1]*0.9+tmp1[1]*0.1
				seg_val_dic[typ].append([latlng1,latlng2,val])
		if cnt%100==0: print(cnt,tg,val)
		cnt+=1
	gen_html_using_template_seg("way-low-%s.html"%addr,seg_val_dic[0],"Way Low")
	gen_html_using_template_seg("way-med-%s.html"%addr,seg_val_dic[1],"Way Medium")
	gen_html_using_template_seg("way-high-%s.html"%addr,seg_val_dic[2],"Way High")



''' input : [  [  [lat0,lng0],[lat1,lng1],val  ], ... ] '''
def gen_html_using_template_seg(htmlf, seg_val_list, comment="", loc_pt=[]):# not in use
	print("len seg_val_list",len(seg_val_list))
	of = open(htmlf,"w")
	if iprint>=2: print("generating html: "+htmlf)
	kInit=0
	kInsert1=1
	kInsert2=2
	kInsert3=3
	kInsert4=4
	kInsert5=5
	kInsert6=6
	state = kInit
	lstr = "new google.maps.LatLng("
	rstr = "), "

	with open(mypydir+"/template-seg.html","r") as f:
		for l in f:
			if state==kInit:
				if l.startswith("////startseg_val_list"):
					state=kInsert1
					for p in seg_val_list: # [[lat0,lng0],[lat1,lng1],sc], 
						latlng0=p[0]
						latlng1=p[1]
						sc=max(0.04,p[2])
						#[ {lat: 40.1138, lng: -88.2246}, {lat: 40.1137, lng: -88.2246} , 0.1 ],
						of.write("[{lat:%.6f,lng:%.6f},"%tuple(latlng0))
						of.write("{lat:%.6f,lng:%.6f},"%tuple(latlng1))
						of.write("%.2f],"%(sc))
				
				elif l.startswith("<!-- <title>start</title> -->"):
					state=kInsert5
					of.write("<title>%s</title>\n"%comment)
				elif l.startswith("///startinsertpt"):
					state=kInsert6
					if loc_pt is not None and len(loc_pt)>0:
						of.write("  var endLatLng=new google.maps.LatLng(%.6f,%.6f);\n"%(loc_pt[0],loc_pt[-1]))
				else:
					of.write(l)

			elif state == kInsert1:
				if l.startswith("////endseg_val_list"):
					state=kInit
			elif state == kInsert2:
				if l.startswith("////endinsertlatlng2"):
					state=kInit
			elif state == kInsert3:
				if l.startswith("<!-- ////endinsertlatlng3 -->"):
					state=kInit
			elif state == kInsert4:
				if l.startswith("////insertheatend1"):
					state=kInit
			elif state == kInsert5:
				if l.startswith("<!-- <title>end</title> -->"):
					state=kInit
			elif state == kInsert6:
				if l.startswith("///endinsertpt"):
					state=kInit
	of.close()




''' Used after genSpeedSegOSRM.py gen turn heat '''
def gen_turn_heats(htmlf,addr,comment="",loc_pt=[]):
	import math
	osmname=addr
	maxlat=40.14
	minlng=-88.25
	minlat=40.09
	maxlng=-88.19
	mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%osmname)
	meta_nid2neighbor= "osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname
	mm_nid2neighbor.use_cache(meta_file_name=meta_nid2neighbor)
	scoreCalc = ScoreCalculator({"sigmoida":5.0, "sigmoidx":1, "sigmoidy":0.04})
	OSMdir="/home/zhao97/greendrive/osmdata/"+addr+"/data/"
	nid1nid2_cost_cnt={} # { (n1,n2):[cost,cnt], } sum all n1,n2,n3 
	with open(OSMdir+"turnpenalty.txt","r") as f:
		for l in f:
			st=l.split(",")
			nid0=int(st[0])
			nid1=int(st[1])
			if  len(mm_nid2neighbor.get(nid2))>2 : 
				nid3=int(st[2])
				val=float(st[3])
				tup=(nid0,nid1)
				if tup not in nid1nid2_cost_cnt:
					nid1nid2_cost_cnt[tup]=[0.0,0]
				nid1nid2_cost_cnt[tup][0]+=val
				nid1nid2_cost_cnt[tup][1]+=1

	shift_latlng=0.00025
	latlng_val=[]
	for k,v in nid1nid2_cost_cnt.items():
		nid0,nid1 = k
		lat1,lng1=mm_nid2latlng.get(nid1)
		if lat1<minlat or lat1>maxlat or lng1<minlng or lng1>maxlng: continue
		avgcost= v[0]/v[1]
		lat0,lng0=mm_nid2latlng.get(nid0)
		hd01= get_bearing_latlng2([lat0,lng0],[lat1,lng1])
		''' shift to right-hand traveling side '''
		rad=(hd01+90.0)/180.0*math.pi
		shiftlng=math.sin(rad)*shift_latlng*0.6
		shiftlat=math.cos(rad)*shift_latlng*0.6
		rad2=(hd01+2*90.0)/180.0*math.pi
		shiftlng2=math.sin(rad2)*shift_latlng
		shiftlat2=math.cos(rad2)*shift_latlng
		sc=scoreCalc.getscore(avgcost)
		latlng_val.append([[lat1+shiftlat+shiftlat2,lng1+shiftlng+shiftlng2],sc])
	print("len latlng_val",len(latlng_val))
	# gen html:	
	of = open(htmlf,"w")
	if iprint>=2: print("generating html: "+htmlf)
		
	kInit=0
	kInsert1=1
	kInsert2=2
	kInsert3=3
	kInsert4=4
	kInsert5=5
	kInsert6=6
	state = kInit
	lstr = "new google.maps.LatLng("
	rstr = "), "

	with open(mypydir+"/template-seg.html","r") as f:
		for l in f:
			if state==kInit:
				if l.startswith("////insertheatstart1"):
					state=kInsert1
					for p in latlng_val: # p= [[lat1,lng1],sc]
						latlng=p[0]
						sc=p[1]
						#[ {lat: 40.1138, lng: -88.2246} , 0.1 ],
						of.write("[{lat:%.6f,lng:%.6f},"%tuple(latlng))
						of.write("%.2f],"%(sc))
				
				elif l.startswith("<!-- <title>start</title> -->"):
					state=kInsert5
					of.write("<title>%s</title>\n"%comment)
				elif l.startswith("///startinsertpt"):
					state=kInsert6
					if loc_pt is not None and len(loc_pt)>0:
						of.write("  var endLatLng=new google.maps.LatLng(%.6f,%.6f);\n"%(loc_pt[0],loc_pt[-1]))
				else:
					of.write(l)

			elif state == kInsert1:
				if l.startswith("////insertheatend1"):
					state=kInit
			elif state == kInsert2:
				if l.startswith("////endinsertlatlng2"):
					state=kInit
			elif state == kInsert3:
				if l.startswith("<!-- ////endinsertlatlng3 -->"):
					state=kInit
			elif state == kInsert4:
				if l.startswith("////insertheatend1"):
					state=kInit
			elif state == kInsert5:
				if l.startswith("<!-- <title>end</title> -->"):
					state=kInit
			elif state == kInsert6:
				if l.startswith("///endinsertpt"):
					state=kInit
	of.close()

