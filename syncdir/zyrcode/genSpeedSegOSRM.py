#!/usr/bin/env python
# gen_osm_seg_mpg_turn_penalty 
import os, sys
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
from namehostip import get_my_ip,get_platform
from util import py_fname,replace_user_home
from geo import get_osm_file_quote_given_file
addpath=mypydir+"/code"
if addpath not in sys.path: sys.path.append(addpath)
from common import *
from costModule import *
from osmutil import osmPipeline

iprint = 2   
My_Platform = get_platform() # "centos" means cluster 
On_Cluster = False
if My_Platform=='centos': On_Cluster = True

import costModule
costModule.iprint=1

costmode=-1
if "fuel" in sys.argv:
	costmode =Mode_Fuel 
elif "short" in sys.argv:
	costmode =Mode_Shortest

if costmode== Mode_Shortest:
	TurnPenaltyFile="turnpenalty-short.txt" 
	SegSpeedFile="segspeed-short.txt"
elif costmode== Mode_Fuel:
	TurnPenaltyFile="turnpenalty-fuel.txt" # fuel-related cost
	SegSpeedFile="segspeed-fuel.txt"

model_features = Infocom_features 
UseLeft= TPleft
UseRight= TPright
UseStraight= TPstraight
UseV2inc = TPspd2inc # all prob. features.

if iprint:
	print("cost mode",costmode,"SegSpeedFile",SegSpeedFile,"TurnPenaltyFile",TurnPenaltyFile)
	print("Use",UseLeft,UseRight,UseStraight)

err = ErrorLogger("allerror.txt", tag=py_fname(__file__,False))
lg = SimpleAppendLogger("logs/"+py_fname(__file__,False), maxsize=10000, overwrite=True)

semaphore_gen =Semaphore(prefix=py_fname(__file__,True)+"~segsem~", count=1,no_restriction= not On_Cluster,)
semaphore2 = Semaphore(prefix=py_fname(__file__,False)+"?~", count=1,no_restriction= not On_Cluster,)# Bug


def gen_osm_seg_mpg_turn_penalty(addr=None):
	global iprint
	segmpg=20
	lock = AccessRestrictionContext(
		prefix=py_fname(__file__,False)+"~gosmtp~", 
		persistent_restriction=True,
		persist_seconds=15000, 
		print_str=False,
		no_restriction= not On_Cluster,
	)
	addrlist=[]
	if addr is not None:
		addrlist.append(addr)
	if iprint: print("Proc "+str(addrlist))

	testcnt = -1 
	if testcnt>0: 
		lock.no_restriction=True 
		iprint=3
		costModule.iprint=3
	waycnt=0

	''' --------- main_loop --------------'''
	for addr in addrlist:
		with lock:
			''' make destination directory by 1 server '''
			lock.Access_Or_Wait_And_Skip("makedirs")
			if not os.path.exists(DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep):
				os.makedirs(DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep)

		osm_folder_path= DirOSM+os.sep+addr
		osm= osmPipeline(folder_path=osm_folder_path)
		osmname= osm.get_osm_file_path().split(os.sep)[-1].rstrip(".osm")
		mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%osmname)
		mm_nid2elevation.use_cache(meta_file_name="osm/cache-%s-nid-to-elevation.txt"%osmname)
		mm_nids2speed.use_cache(meta_file_name="osm/cache-%s-nids-to-speed.txt"%osmname)
		mm_nid2neighbor.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname)
		mm_nid2waytag.use_cache(meta_file_name="osm/cache-%s-nids-to-waytag.txt"%osmname)
		QUOTE=get_osm_file_quote_given_file(osm_folder_path+os.sep+addr+".osm")
		country=addr.split(",")[-1]
		print("QUOTE",QUOTE,"country",country)
		if country=="US":
			choose_best_model=True
			use_this_model=None

		with lock:
			lock.Access_Or_Wait_And_Skip("Deleting out files")
			fn=DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep+SegSpeedFile
			try:
				os.remove(fn)
			except: pass
			print("Deleting "+fn)
			fn=DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep+TurnPenaltyFile
			try:
				os.remove(fn)
			except: pass
			print("Deleting "+fn)

		print('\nBegin! --------')
		for da in yield_obj_from_osm_file("way",osm_folder_path+os.sep+addr+".osm"): # for each way 
			'''<way id="5324735" version="1">
			<nd ref="37948105"/>
			<nd ref="37948104"/>'''
			assert da[0].startswith("<way id=")
			with lock:
				lock.Access_Or_Skip(da[0])
				if iprint>=3: print("\n\n\n\n")
				if iprint>=3: print(da[0])
				nlist=[] # nids [int]
				for e in da:
					if e.startswith("<nd "):
						nid = int(e.split(' ref=%s'%QUOTE)[-1].split(QUOTE)[0])
						nlist.append(nid)
				if iprint>=3: print("On This Way:",nlist)

				if costmode==Mode_Fuel:
					
					''' write seg mpg here: '''
					model2nlist={}
					retrieveModel=[]
					testnodes=[]
					for ni in range(len(nlist)-1):
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						testnodes.append(nid0)
						if get_dist_meters_given_nlist(testnodes)>MinSegDistForMPG and len(mm_nid2neighbor.get(nid1))>2:
							testnodes.append(nid1)
							gas=   get_gas_given_nlist(testnodes, exclude_turn=True, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=choose_best_model, return_model_list=retrieveModel,use_this_model=use_this_model,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)

							if retrieveModel[0] not in model2nlist:
								model2nlist[retrieveModel[0]]=[]
							model2nlist[retrieveModel[0]].extend(testnodes[1:])
							retrieveModel=[]

							dist01=get_dist_meters_given_nlist(testnodes)
							segmpg = dist01/MetersPerMile/(gas/GramPerGallon)
							segmpg=max(MinSegMpg,min(MaxSegMpg,segmpg))
							write_nlist_speed_file(testnodes,segmpg,addr=osmname)
							testnodes=[]
						elif ni==len(nlist)-2:
							testnodes.append(nid1)
							gas=   get_gas_given_nlist(testnodes, exclude_turn=True, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=choose_best_model, return_model_list=retrieveModel,use_this_model=use_this_model,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)
							
							if retrieveModel[0] not in model2nlist:
								model2nlist[retrieveModel[0]]=[]
							model2nlist[retrieveModel[0]].extend(testnodes[1:])
							retrieveModel=[]

							dist01=get_dist_meters_given_nlist(testnodes)
							endsegmpg = dist01/MetersPerMile/(gas/GramPerGallon)
							if endsegmpg<MinSegMpg or endsegmpg>MaxSegMpg:
								if dist01>MinSegDistForMPG: 
									endsegmpg=max(MinSegMpg,min(MaxSegMpg,endsegmpg))
								elif segmpg is not None: 
									endsegmpg=segmpg # use last one
							segmpg=max(MinSegMpg,min(MaxSegMpg,endsegmpg))
							write_nlist_speed_file(testnodes,segmpg,addr=osmname)
					''' write turn penalty here: '''
					for ni in range(len(nlist)-1): 
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						nblist=mm_nid2neighbor.get(nid1)
						thisModel=None
						if len(nblist)>2:
							for model,tmpnlst in model2nlist.items():
								if nid1 in tmpnlst:
									thisModel=model
									break
							write_turn_penalty_file(nid0,nid1,nid0,UTurnTimePenalty,addr=osmname)# Uturn
							nblist.remove(nid0) # remove u-turn leg
							for nbn in nblist: #  remain only turn-legs.
								tcost =   get_turn_cost(nid0,nid1,nbn, print_str=False,country=country)
								tmp_sum=0
								tmp_sum+= tcost[0]* thisModel.coef_[model_features.index(UseV2inc)]
								if tcost[1]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[1]* thisModel.coef_[model_features.index(UseLeft)] )
								if tcost[2]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[2]* thisModel.coef_[model_features.index(UseRight)] )
								if tcost[3]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[3]* thisModel.coef_[model_features.index(UseStraight)] )
								if iprint>=3: print("Turn_penalty",nid0,nid1,nbn,tmp_sum)
								write_turn_penalty_file(nid0,nid1,nbn,tmp_sum,addr=osmname)

					if iprint>=3: print('''\n----- reverse direction: ''')
					nlist.reverse()

					''' write seg mpg here: '''
					model2nlist={}
					retrieveModel=[]
					testnodes=[]
					for ni in range(len(nlist)-1):
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						testnodes.append(nid0)
						if  get_dist_meters_given_nlist(testnodes)>MinSegDistForMPG and len(mm_nid2neighbor.get(nid1))>2:
							testnodes.append(nid1)
							gas=   get_gas_given_nlist(testnodes, exclude_turn=True, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=choose_best_model, return_model_list=retrieveModel,use_this_model=use_this_model,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)

							if retrieveModel[0] not in model2nlist:
								model2nlist[retrieveModel[0]]=[]
							model2nlist[retrieveModel[0]].extend(testnodes[1:])
							retrieveModel=[]

							dist01=get_dist_meters_given_nlist(testnodes)
							segmpg = dist01/MetersPerMile/(gas/GramPerGallon)
							segmpg=max(MinSegMpg,min(MaxSegMpg,segmpg))
							write_nlist_speed_file(testnodes,segmpg,addr=osmname)
							testnodes=[]
						elif ni==len(nlist)-2:
							testnodes.append(nid1)
							gas=   get_gas_given_nlist(testnodes, exclude_turn=True, print_str=False, cut_by_waytag=False,selective_training=False, choose_best_model=choose_best_model, return_model_list=retrieveModel,use_this_model=use_this_model,use_these_features=model_features,retrieve_feature_list=None,is_end_to_end=False)
							
							if retrieveModel[0] not in model2nlist:
								model2nlist[retrieveModel[0]]=[]
							model2nlist[retrieveModel[0]].extend(testnodes[1:])
							retrieveModel=[]

							dist01=get_dist_meters_given_nlist(testnodes)
							endsegmpg = dist01/MetersPerMile/(gas/GramPerGallon)
							if endsegmpg<MinSegMpg or endsegmpg>MaxSegMpg:
								if dist01>MinSegDistForMPG: 
									endsegmpg=max(MinSegMpg,min(MaxSegMpg,endsegmpg))
								elif segmpg is not None: 
									endsegmpg=segmpg # use last one
							segmpg=max(MinSegMpg,min(MaxSegMpg,endsegmpg))
							write_nlist_speed_file(testnodes,segmpg,addr=osmname)
					''' write turn penalty here: '''
					for ni in range(len(nlist)-1): 
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						nblist=mm_nid2neighbor.get(nid1)
						thisModel=None
						if len(nblist)>2:
							for model,tmpnlst in model2nlist.items():
								if nid1 in tmpnlst:
									thisModel=model
									break
							write_turn_penalty_file(nid0,nid1,nid0,UTurnTimePenalty,addr=osmname)# Uturn
							nblist.remove(nid0) # remove u-turn leg
							for nbn in nblist: #   remain only turn-legs.
								tcost =   get_turn_cost(nid0,nid1,nbn, print_str=False,country=country)
								tmp_sum=0
								tmp_sum+= tcost[0]* thisModel.coef_[model_features.index(UseV2inc)]
								if tcost[1]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[1]* thisModel.coef_[model_features.index(UseLeft)] )
								if tcost[2]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[2]* thisModel.coef_[model_features.index(UseRight)] )
								if tcost[3]>0:
									tmp_sum+= max( kMinTurnGasPenalty, tcost[3]* thisModel.coef_[model_features.index(UseStraight)] )
								if iprint>=3: print("Turn_penalty",nid0,nid1,nbn,tmp_sum)
								write_turn_penalty_file(nid0,nid1,nbn,tmp_sum,addr=osmname)


				
				elif costmode==Mode_Shortest:
					''' Write to get shortest path '''
					write_nlist_speed_file(nlist,20,addr=osmname)
					for ni in range(len(nlist)-1): 
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						nblist=mm_nid2neighbor.get(nid1)
						if len(nblist)>2:
							for nbn in nblist: 
								write_turn_penalty_file(nid0,nid1,nbn,0,addr=osmname)
					nlist.reverse()
					write_nlist_speed_file(nlist,20,addr=osmname)
					for ni in range(len(nlist)-1): 
						nid0=nlist[ni]
						nid1=nlist[ni+1]
						nblist=mm_nid2neighbor.get(nid1)
						if len(nblist)>2:
							for nbn in nblist: 
								write_turn_penalty_file(nid0,nid1,nbn,0,addr=osmname)

				if testcnt>0: 
					testcnt-=1
					if testcnt<=0: sys.exit(0)
				waycnt+=1
				if waycnt%100==1:
					if iprint:print(da[0],waycnt)


def write_nlist_speed_file(nlist,val_int,addr,multiply=MultiplySegMpg): 
	if val_int<0:
		print(nlist,val_int,addr,multiply)
		raise Exception("seg speed < 0 !!!??? %f"%val_int)
	fn=DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep+SegSpeedFile
	with semaphore_gen :
		with semaphore2 :
			with open(fn,"a") as f:
				for i in range(len(nlist)-1):
					f.write( "%d,%d,%d\n"%(nlist[i],nlist[i+1],int(val_int*multiply*RatioSegCostMpgComparedToKmh) ) )

def write_turn_penalty_file(nid1,via,nid2,val,addr,addition=AddTurnPenalty):
	fn=DirOSM+os.sep+addr+os.sep+SegFileOutdir+os.sep+TurnPenaltyFile
	if val<0:
		print(nid1,via,nid2,val,addr,addition)
		raise Exception("turn penalty < 0 !!!??? %f"%val)
	with semaphore_gen :
		with semaphore2 :
			with open(fn,"a") as f:
				f.write( "%d,%d,%d,%.2f\n"%(nid1,via,nid2,val+addition ) )



if __name__ == "__main__":
	
	arglist=sys.argv[1:]

	if "il" in arglist: 
		gen_osm_seg_mpg_turn_penalty("Illinois,US")

