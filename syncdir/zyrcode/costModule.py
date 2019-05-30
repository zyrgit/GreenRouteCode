#!/usr/bin/env python
# get_turn_cost extract_feature get_gas_given_nlist
import os, sys
import inspect
import requests
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
addpath=mypydir+"/"
if addpath not in sys.path: sys.path.append(addpath)
import code
from code.common import *
from mygmaps import GoogleMaps
from geo import get_dist_meters_latlng2
from code.constants import * # addr2ip
gmaps=GoogleMaps()

iprint = 2   

def extract_feature_plus_turn(nlist, print_str=False, is_end_to_end=True, country="US"):
	if iprint>=3 and print_str: 
		print("[ extract_feature_plus_turn ]   Enter  ---------------------- ")
	train_nid_pos=[[x,0] for x in nlist] # compatible with old code in 5.py.
	sample= gen_sample(train_nid_pos=train_nid_pos,mm_nid2elevation=mm_nid2elevation,mm_nid2latlng=mm_nid2latlng,mm_nid2neighbor=mm_nid2neighbor,print_str=False,is_end_to_end=is_end_to_end)
	''' ---- calc Inc speed^2 according to turn min speed: --- '''
	dspd2inc=0.0
	turnIncV2=0.0
	stopleft=0 # cumu prob 
	stopright=0
	stopstraight=0
	for i in range(len(train_nid_pos)-1):
		nid0 = train_nid_pos[i][0]
		nid1 =train_nid_pos[i+1][0]
		spd= get_speed_nid2(nid0,nid1)
		if len(mm_nid2neighbor.get(nid1))>2: # at cross mm_major_nids.get(nid1)==1 or 
			if i<len(train_nid_pos)-2: # last node no cost.
				''' turn penalty includes both stop and spd^2 dec! ___ '''
				nextNid=train_nid_pos[i+2][0]
				tcost = get_turn_cost(nid0,nid1,nextNid,print_str=False,country=country)
				stopleft+=tcost[1]
				stopstraight+=tcost[3]
				stopright+=tcost[2]
				turnIncV2+= tcost[0]
				if iprint>=3 and print_str: print("[ extract_feature ] turn_penalty",nid0,nid1,nextNid,tcost)
		else: # not crossing
			if i<len(train_nid_pos)-2:
				nextspd= get_speed_nid2(train_nid_pos[i+1][0],train_nid_pos[i+2][0])
				dspd2inc+=max(0.0, nextspd**2- spd**2)
	sample["turnIncV2"]=turnIncV2
	sample["turnle"]=stopleft
	sample["turnri"]=stopright
	sample["turnst"]=stopstraight
	sample[KincSpeed2]=dspd2inc
	sample[TPspd2inc]=dspd2inc
	sample[TstopLeft]=0 # filled later if not exclude turn.
	sample[TstopRight]=0
	sample[TstopStraight]=0
	sample[TPleft]=0.0 # filled later
	sample[TPright]=0.0
	sample[TPstraight]=0.0
	''' fix net spd dec problem, if entire trip two ends.'''
	if is_end_to_end:
		spd0= get_speed_nid2(train_nid_pos[0][0],train_nid_pos[1][0])
		spd1= get_speed_nid2(train_nid_pos[-2][0],train_nid_pos[-1][0])
		sample[KincSpeed2] -= max(0, spd0**2 - spd1**2)
	return sample


def extract_feature(nlist, exclude_turn, print_str=False, is_end_to_end=True):
	sample=extract_feature_plus_turn(nlist, print_str=print_str, is_end_to_end=is_end_to_end)
	if not exclude_turn: 
		sample[KincSpeed2]+= sample["turnIncV2"]
		sample[TPspd2inc] = sample[KincSpeed2]
		sample[TstopLeft]=sample["turnle"]
		sample[TstopRight]=sample["turnri"]
		sample[TstopStraight]=sample["turnst"]
		sample[TPleft]=sample[TstopLeft]
		sample[TPright]=sample[TstopRight]
		sample[TPstraight]=sample[TstopStraight]
	return sample



def get_gas_given_nlist(nlist, exclude_turn=False, cut_by_waytag=False, selective_training=False, return_model_list=None, use_this_model=None, use_these_features=None, retrieve_feature_list=None, is_end_to_end=True, choose_best_model=False, print_feat=False, print_str=False, country="US"):
	if iprint>=2 and print_str: 
		print("\n[ get_gas_given_nlist ] Enter --------")
	
	if not cut_by_waytag:

		sample=extract_feature(nlist, exclude_turn=exclude_turn, print_str=print_str, is_end_to_end=is_end_to_end)
		
		if choose_best_model: # default, according to road context
			if print_str: print("choose_best_model")
			asp=      get_lv01_v01_given_nlist(nlist)
			sample["asp"]=asp
			sample["nodes"]=nlist 
			gas_sum= predict_using_best_model(sample,use_these_features,return_model_list=return_model_list,use_this_model=None,print_feat=True,retrieve_feature_list=retrieve_feature_list, print_str=print_str)

		elif selective_training: # select similar sp to train
			if print_str: print("selective_training")
			asp=      get_lv01_v01_given_nlist(nlist)
			sample["asp"]=asp
			sample["nodes"]=nlist 
			gas_sum = predict_using_best_N_train(sample,use_these_features,return_model_list=return_model_list, use_this_model=use_this_model,retrieve_feature_list=retrieve_feature_list)
		else:
			if print_str: print("use given model")
			gas_sum =     model_predict( gen_x(sample, use_these_features, print_feat=print_feat),use_this_model )
			if return_model_list is not None:
				return_model_list.append(use_this_model)

		dist= get_dist_meters_given_nlist(nlist)
		mpg = get_MPG_given_meters_gram(dist,gas_sum)
	else:
		''' ---- cut by waytag . Not in use'''
		gas_sum=0.0
		missed_turn_penalty_list=[]
		lasttagtype=None
		cutnlist=[]
		for i in range(len(nlist)-1):
			tagtype=get_spd_type_given_nid2(nlist[i],nlist[i+1])
			if lasttagtype is not None and tagtype!=lasttagtype:
				cutnlist.append(nlist[i])
				if not exclude_turn:
					missed_turn_penalty_list.append([nlist[i-1],nlist[i],nlist[i+1]])
				
				sample=extract_feature(cutnlist, exclude_turn=exclude_turn, print_str=print_str, is_end_to_end=False)
				
				if selective_training:
					asp=      get_lv01_v01_given_nlist(cutnlist)
					sample["asp"]=asp
					sample["nodes"]=cutnlist  
					sample[KtagType]=lasttagtype
					y_gas = predict_using_best_N_train(sample,use_these_features,return_model_list=return_model_list, use_this_model=use_this_model, retrieve_feature_list=retrieve_feature_list)
				else:
					y_gas=   model_predict( gen_x(sample,use_these_features, print_feat=print_feat), use_this_model )
				
				gas_sum+=y_gas
				if print_str and iprint: 
					print("+cut gas",y_gas,"gas_sum",gas_sum)
					dist= get_dist_meters_given_nlist(cutnlist)
					mpg = get_MPG_given_meters_gram(dist,y_gas)
					print("cut_by_waytag dist",dist,"mpg",mpg)
				cutnlist=[nlist[i]]
			else:
				cutnlist.append(nlist[i])

			lasttagtype=tagtype
		cutnlist.append(nlist[-1])

		sample=extract_feature(cutnlist, exclude_turn=exclude_turn, print_str=print_str,is_end_to_end=False)
		if selective_training:
			asp=     get_lv01_v01_given_nlist(cutnlist)
			sample["asp"]=asp
			sample["nodes"]=cutnlist
			sample[KtagType]=lasttagtype
			y_gas = predict_using_best_N_train(sample,use_these_features,return_model_list=return_model_list, use_this_model=use_this_model, retrieve_feature_list=retrieve_feature_list)
		else:
			y_gas=    model_predict( gen_x(sample,use_these_features, print_feat=print_feat), use_this_model )

		gas_sum+=y_gas 
		
		''' -------- add missing turn penalty '''
		for n1n2n3 in missed_turn_penalty_list:
			tcost = get_turn_cost(n1n2n3[0],n1n2n3[1],n1n2n3[2],print_str=False,country=country)
			tmp_sum=0
			tmp_sum+= tcost[0]* use_this_model.coef_[Global_model_features.index(KincSpeed2)]
			tmp_sum+= tcost[1]* use_this_model.coef_[Global_model_features.index(TstopLeft)]
			tmp_sum+= tcost[2]* use_this_model.coef_[Global_model_features.index(TstopRight)]
			tmp_sum+= tcost[3]* use_this_model.coef_[Global_model_features.index(TstopStraight)]
			gas_sum+=tmp_sum
	return gas_sum



def predict_using_best_model(sample,model_features,return_model_list=None,use_this_model=None,print_feat=True,retrieve_feature_list=None, print_str=False):
	if use_this_model is None:
		spasp = sample["asp"]
		for feat in [Ktime,Kv2d,Kvd]:
			spasp[feat]=sample[feat]/sample[Kdist]
		vec=[]
		for feat in cov_dims:
			vec.append(spasp[feat]/Global_datamean[cov_dims.index(feat)])
		mindist=1e10
		bestind=None
		for lb,center in Global_lb2center.items():
			di = 1.0- cosine_similarity(np.asarray(vec).reshape(1, -1), np.asarray(center).reshape(1, -1))[0][0]
			if di<mindist: 
				mindist=di
				bestind=lb
		thisregr = Global_lb2model[bestind]
	else:
		thisregr= use_this_model
	if return_model_list is not None: return_model_list.append(thisregr)
	spx=      gen_x(sample, model_features, print_feat=print_feat)
	thisgas=     model_predict(spx, thisregr, print_feat=print_feat)
	if retrieve_feature_list is not None:
		retrieve_feature_list.append(spx)
	return thisgas


def predict_using_best_N_train(sample_asp,model_features,N=50,dist_thresh=0.5,return_model_list=None,use_this_model=None,print_feat=True,retrieve_feature_list=None):
	if use_this_model is None:
		tmpx,tmpy= get_best_N_train(sample_asp,N,dist_thresh,model_features)
		thisregr=train_given_x_y(tmpx,tmpy)
	else:
		thisregr=use_this_model
	if return_model_list is not None: return_model_list.append(thisregr)
	spx=      gen_x(sample_asp, model_features, print_feat=print_feat)
	thisgas=     model_predict(spx, thisregr, print_feat=print_feat)
	if retrieve_feature_list is not None:
		retrieve_feature_list.append(spx)
	return thisgas


def train_given_x_y(x,y,model_index=2): # positive coef may better.
	x=np.asarray(x)
	y=np.asarray(y)
	if model_index==0:
		ChosenModel=linear_model.LinearRegression
		modelKwargs={"fit_intercept":False, "normalize":True}
	elif model_index==1:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":False, "fit_intercept":False, "normalize":True} 
	else:
		ChosenModel=linear_model.Lasso
		modelKwargs={"alpha":0.1, "positive":True, "fit_intercept":False, "normalize":True} 
	thisregr = ChosenModel(**modelKwargs)
	thisregr.fit(x, y)
	return thisregr


def get_best_N_train(sample,N,dist_thresh,model_features):
	max_key = mm_train_valid.get("max_key")
	spasp = sample["asp"]
	gen_html=False
	use_tag= False #  only in its own waytag?
	for feat in plot_corr_features:
		if feat == Kdist: continue
		spasp[feat]=sample[feat]/sample[Kdist]
	ind2dist={}
	if use_tag: 
		thisWayTag=sample[KtagType]
	for i in range(max_key):
		sp=mm_train_valid.get(i)
		if use_tag and sp[KtagType]!=thisWayTag:
			continue
		asp=sp["asp"]
		dist=get_sample_dist_cov(asp,spasp)
		ind2dist[i]=dist
	lst_ind_val=sort_dic_by_value_return_list_val_key(ind2dist)
	if lst_ind_val[0][1]>0.98: # no similarity, use more.
		N*=2
	val_ind=[]
	for i in range(len(lst_ind_val)):
		if i<N: 
			val_ind.append([lst_ind_val[i][1],lst_ind_val[i][0]])
		elif lst_ind_val[i][1]<dist_thresh:
			val_ind.append([lst_ind_val[i][1],lst_ind_val[i][0]])
		else:
			break
	x_all=[]
	y_all = []
	printcnt=0
	if gen_html: paths=[sample["nodes"]]
	for tup in val_ind:
		printcnt+=1
		sp=mm_train_valid.get(tup[1])
		tmpx=[]
		for feat in model_features:
			tmpx.append(sp[feat])
		x_all.append(tmpx)
		y_all.append(sp["gas"])
		if gen_html and printcnt<=10: 
			paths.append(sp["nodes"])
	if gen_html: # need checking.
		gen_map_html_from_path_list(paths,"html/sp/%d.html"%paths[0][0], addr='ChampaignCounty,Illinois,US', disturb=True,disturb_shrink=0.1,print_str=True)
		sys.exit(0)
	return x_all,y_all



def extend_straight_nodes_given_nlist(nlist,max_angle_diff=45,min_extend_dist=800,print_str=False):
	latlng0 = mm_nid2latlng.get(nlist[0])
	latlng1 = mm_nid2latlng.get(nlist[1])
	latlng2 = mm_nid2latlng.get(nlist[-2])
	latlng3 = mm_nid2latlng.get(nlist[-1])
	hd1 = get_bearing_latlng2(latlng1,latlng0)
	hd2 = get_bearing_latlng2(latlng2,latlng3)
	leftnids=extend_nodes_given_nid_angle(nlist[0],hd1,max_angle_diff,min_extend_dist)
	rightnids=extend_nodes_given_nid_angle(nlist[-1],hd2,max_angle_diff,min_extend_dist)
	if len(leftnids)>0: allnids=leftnids[0:-1]
	else: allnids=[]
	allnids.extend(nlist)
	if len(rightnids)>0: allnids.extend(rightnids[1:])
	return leftnids,rightnids,allnids


def extend_nodes_given_nid_angle(nid,heading,max_angle_diff,min_extend_dist):
	''' walk dist heading within angle diff: '''
	cumudist= -1e-10
	extendnids=[nid]
	while cumudist<min_extend_dist:
		nblist=mm_nid2neighbor.get(extendnids[-1])
		lastlatlng=mm_nid2latlng.get(extendnids[-1])
		mindiff=1e10
		straightNid=None
		for nbn in nblist: 
			latlngnb = mm_nid2latlng.get(nbn)
			hdn = get_bearing_latlng2(lastlatlng,latlngnb)
			diff= min_angle_diff(hdn,heading)
			if diff<mindiff and diff<=max_angle_diff: 
				mindiff=diff
				straightNid=nbn
				dist= get_dist_meters_latlng2(lastlatlng,latlngnb)
		if straightNid is not None:
			extendnids.append(straightNid)
			cumudist+=dist
		else: break
	if cumudist<min_extend_dist: 
		extendnids=[]
	return extendnids




''' -------------- for the routing server.py : ---------'''
LngFirst = 1 # osrm url/ret is lng,lat ?  

def get_route_url_given_latlng_list(latlngs,addr, insert_every_num=None, backend=None, print_res=False):
	''' Return URL with via waypoints in it, given a list of latlngs. 
	GET http://172.22.68.71:5000/route/v1/driving/-88.22219,40.114936;-88..,40..;-88.235836,40.101568?annotations=true&steps=true
	- insert_every_num: insert mid way point every this gps pts.
	'''
	dist = get_dist_meters_latlng2(latlngs[0],latlngs[-1])
	if insert_every_num is None:
		if len(latlngs)<4:
			insert_every_num=1
		elif len(latlngs)>=4 and len(latlngs)<10:
			insert_every_num = 2
		elif len(latlngs)>=10 and len(latlngs)<500:
			insert_every_num = max(2,int(len(latlngs)/10.0))
		elif len(latlngs)>=500:
			insert_every_num = 50 # skip mid pts to avoid long url.
	pcnt=0
	i=0
	locs=""
	# rad=""
	while i< len(latlngs):
		if i==len(latlngs)-1:
			locs+=str(latlngs[i][LngFirst])+","+str(latlngs[i][1-LngFirst])
			# rad+="20"
		elif i%insert_every_num==0:
			locs+=str(latlngs[i][LngFirst])+","+str(latlngs[i][1-LngFirst])+";"
			# rad+="20;"
			pcnt+=1
		i+=1
	if backend is None:
		backend = addr2ip[addr]
	distPerPt= dist/pcnt #  way point every this meters. 
	if len(latlngs)==2 or dist>40000 or distPerPt>2000:
		routeUrl = URL_Route.format(Backend=backend,Loc=locs) # route API
		routeUrl+= '&steps=true'
	else:
		routeUrl = URL_Match.format(Backend=backend,Loc=locs) # match API
		routeUrl+= '&steps=true' #'&steps=true&radiuses='+rad
	return routeUrl


def collect_duration_nodelist_given_json(ret, nodeslist=[]):
	''' Gather duration/fuel of either route/match returned '''
	gas=0.
	if 'routes' in ret:
		for tmp in ret['routes']:
			gas += tmp['duration']
			legs= tmp['legs']
			for rt in legs:
				nodes = rt['annotation']['nodes']
				nodeslist.extend(nodes)
	elif 'matchings' in ret:  
		for tmp in ret['matchings']:
			gas += tmp['duration']
			numMatch=len(ret["matchings"])
			for m in range(numMatch):
				matchpoints=ret["matchings"][m]["legs"]
				NumAnnotations=len(matchpoints)
				for i in range(NumAnnotations):
					nlist=matchpoints[i]["annotation"]["nodes"]
					nodeslist.extend(nlist)
	return gas


def get_fuel_given_latlng_list(latlngs,addr, backend=None, loose_end_dist_allow=80, nodeslist=[], print_res=False):
	''' Given list of latlng, return fuel i.e. duration by backend '''
	gas=0.
	routeUrl = get_route_url_given_latlng_list(latlngs,addr, backend=backend, print_res=print_res)
	ret = requests.get(routeUrl).json()
	gas+= collect_duration_nodelist_given_json(ret, nodeslist)  # nodeslist: to check result
	if mm_nid2latlng.get_id()!="osm/cache-%s-nodeid-to-lat-lng.txt"%addr:
		mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%addr, ignore_invalid_mem=True)
	''' Check ----------- cases where first latlng is not matched '''
	try:
		firstLatlng = mm_nid2latlng.get(nodeslist[0]) or crawl_nid_to_latlng(nodeslist[0],silent=True) # in case of None
	except:
		print("Node",nodeslist[0],"does not exist in either mm or overpy! Try 2nd node...")
		firstLatlng = mm_nid2latlng.get(nodeslist[1]) or crawl_nid_to_latlng(nodeslist[1],silent=True)
	dist = get_dist_meters_latlng2(latlngs[0],firstLatlng)
	if dist > loose_end_dist_allow:
		routeUrl= get_route_url_given_latlng_list([latlngs[0],firstLatlng],addr, backend=backend, print_res=print_res)
		ret = requests.get(routeUrl).json()
		tmpl=[]
		gas+= collect_duration_nodelist_given_json(ret, tmpl)
		nodeslist=tmpl+nodeslist
	''' Check ----------- cases where last latlng is not matched '''
	try:
		lastLatlng = mm_nid2latlng.get(nodeslist[-1]) or crawl_nid_to_latlng(nodeslist[-1],silent=True)# in case of None
	except:
		lastLatlng = mm_nid2latlng.get(nodeslist[-2]) or crawl_nid_to_latlng(nodeslist[-2],silent=True) 
	dist = get_dist_meters_latlng2(latlngs[-1],lastLatlng)
	if dist> loose_end_dist_allow:
		routeUrl= get_route_url_given_latlng_list([lastLatlng,latlngs[-1]],addr, backend=backend, print_res=print_res)
		ret = requests.get(routeUrl).json()
		tmpl=[]
		gas+= collect_duration_nodelist_given_json(ret, tmpl)
		nodeslist=nodeslist+tmpl
	return gas


def get_fuel_google(latlng1,latlng2,addr,print_res=False):
	''' Given O/D pair, get fuel of Google route'''
	ret=gmaps.get_route_given_2latlng(latlng1,latlng2) # [[latlng, dist, duration ],] sparse steps
	latlngs=[]
	for stt in ret:
		latlngs.append([ stt[0][0],stt[0][1] ])
	ggas = get_fuel_given_latlng_list(latlngs,addr,print_res=print_res)
	return ggas


def gen_nodeids_given_2latlng(latlng1,latlng2,addr=None, backend=None, print_res=False):
	''' Osrm route api, get nodeids, latlngs. 
	For model_pred_fuel given node list, OR for the greenmap server plot gps trace.
	'''
	if not addr:
		addr=latlng_to_city_state_country(float(latlng1[0]),float(latlng1[1]),no_space=True)
	routeUrl=get_route_url_given_latlng_list([latlng1,latlng2],addr, insert_every_num=20, backend=backend, print_res=print_res)
	ret = requests.get(routeUrl).json()
	if 'routes' in ret:
		legs= ret['routes'][0]['legs']
		gas = ret['routes'][0]['duration']
		nodeslists=[]
		gpslist=[latlng1]
		for rt in legs:
			nodes = rt['annotation']['nodes']
			nodeslists.append(nodes) # node can be dense
			steps = rt['steps']
			for step in steps: # steps can be sparse.
				# lnglat = step['maneuver']['location'] # has too few points than 'intersections'
				cross = step['intersections']
				for inter in cross:
					lnglat = inter['location']
					gpslist.append([lnglat[LngFirst],lnglat[1-LngFirst]])# this is sparse.
		nodeids = []
		for nlst in nodeslists:
			connect_dots(nodeids, nlst, allow_duplicate=0, mm_nid2neighbor=None,addr=addr)
		if iprint>=2 and print_res: print("[ connect_dots ] ",nodeids)
		gpslist.append(latlng2)
		return nodeids,addr,gpslist,gas
	else:
		return None,addr,None,0.

def model_pred_fuel(nlist): # too slow,  Not in use.
	model_features = Infocom_features 
	testnodes=[]
	fuel=0.
	for ni in range(len(nlist)-1):
		nid0=nlist[ni]
		nid1=nlist[ni+1]
		testnodes.append(nid0)
		if get_dist_meters_given_nlist(testnodes)>MinSegDistForMPG and len(mm_nid2neighbor.get(nid1))>2:
			testnodes.append(nid1)
			gas=   get_gas_given_nlist(testnodes, exclude_turn=False, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=True, return_model_list=None,use_this_model=None,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)
			fuel+=gas
			testnodes=[]
		elif ni==len(nlist)-2:
			testnodes.append(nid1)
			gas=   get_gas_given_nlist(testnodes, exclude_turn=False, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=True, return_model_list=None,use_this_model=None,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)
			fuel+=gas
	return fuel

def test_fuel_pred(latlng1,latlng2,addr=None):
	from get_info import calc_cost_given_nlist
	if not addr:
		addr=latlng_to_city_state_country(float(latlng1[0]),float(latlng1[1]),no_space=True)
	gnids,gmod,gfuel,fnids,fgas,fmod=[],0,0,[],0,0
	gfuel= get_fuel_google(latlng1,latlng2,addr,print_res=True)
	print("Google",gmod,gfuel,len(gnids))
	fgas= get_fuel_given_latlng_list([latlng1,latlng2],addr)
	print("Our",fmod,fgas,len(fnids))



	

