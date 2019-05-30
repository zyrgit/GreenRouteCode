#!/usr/bin/env python

import os, sys, getpass, glob
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
addpath=mypydir+"/mytools"
if addpath not in sys.path: sys.path.append(addpath)
from util import py_fname,replace_user_home
from namehostip import get_my_ip,get_platform
addpath=mypydir+"/code"
if addpath not in sys.path: sys.path.append(addpath)
from gen_html_train import gen_html_given_path_nid_ind_list
from costModule import get_turn_cost
from common import * # gen_sample
from osmutil import osmPipeline
from code.configure import Train_Samples_mm_IP # redis IP store train samples.

iprint = 1  
My_Platform = get_platform() # "centos" means cluster 
On_Cluster = False
if My_Platform=='centos': On_Cluster = True

err = ErrorLogger("allerror.txt", tag=py_fname(__file__,False))
lg = SimpleAppendLogger("logs/"+py_fname(__file__,False), maxsize=10000, overwrite=True)

lock = AccessRestrictionContext(
	prefix=py_fname(__file__,False)+"~gt~", 
	persistent_restriction=True,
	persist_seconds=120, 
	print_str=False,
	no_restriction= not On_Cluster,
)
semaphore=Semaphore(prefix=py_fname(__file__,False)+"~train_sem", count=1, no_restriction= not On_Cluster,)

car2meta = get_car_metadata()
carkeyseen=dict()
emailseen=dict()
if iprint: pprint.pprint(car2meta)


invalidCombineFn2cnt={}
testcnt= -1  
bugEmails=[]
bugTimes = [] 
PopbugTimes=0 # if you wanna go on after first bug timestr.


if len(bugTimes)>0 or testcnt>0:# ___
	iprint=4
	lock.no_restriction=True
	import genSpeedSegOSRM
	genSpeedSegOSRM.iprint=2

DirData = replace_user_home(DirData)
account_dirs = glob.glob(DirData+"/*")

overwrite_servers_osm = True # not using previous ips
use_ips = [Train_Samples_mm_IP] # overwrites redis settings, here use 1 PC.
Overwrite_mm_params = {'overwrite_servers':overwrite_servers_osm, 'use_ips':use_ips}


'---------------- gen_train() Run by multi servers, get train samples ------------'

def gen_train(mm_train_use, mm_train_turn_use, use_emails=None, black_addrs=[], Low_Dist_Thresh=None): 
	global testcnt
	blacklist=[]
	black_addr_list=[]
	black_addr_list.extend(black_addrs)

	if len(bugTimes)==0:
		with lock:
			lock.Access_Or_Wait_And_Skip("max_key_turn->0")
			mm_train_turn_use.set("max_key_turn",0)
			mm_train_use.set("max_key",0)

	for iddir in account_dirs:
		email = iddir.split(os.sep)[-1]
		if use_emails is None:
			if email in blacklist and (len(bugEmails)==0 or len(bugEmails)>0 and bugEmails[0] not in blacklist): continue
		else:
			if email not in use_emails: continue

		if len(bugEmails)>0 and email not in bugEmails: continue
		# check account empty?
		tmpdir = iddir+"/%s"%matchfolder
		if not ( os.path.exists(tmpdir) and os.path.isdir(tmpdir) ):
			if iprint>=1: print(__file__,"Empty account",iddir)
			continue
		# gather unix time 
		tmpdir = iddir+"/%s"%combinefolder
		time_list = [x.strip(os.sep).split(os.sep)[-1].rstrip(".txt") for x in glob.glob(tmpdir+"/*.txt")]

		for truetimestr in time_list:
			# try:
			if 1:
				with lock:
					''' each truetimestr by 1 server '''
					lock.Access_Or_Skip(email+truetimestr)
					if len(bugTimes)>0 and truetimestr not in bugTimes: 
						continue
					if PopbugTimes>0 and len(bugTimes)>0: bugTimes.remove(truetimestr) 

					combinefn=iddir+os.sep+combinefolder+os.sep+truetimestr+".txt"
					mflist = glob.glob(iddir+os.sep+matchfolder+os.sep+truetimestr+"-*")

					if not os.path.exists(combinefn) or len(mflist)==0:
						if iprint>=2: print("\nskip empty: %s "%combinefn)
						continue


					if iprint>=2: print("\n[ Reading ] %s "%combinefn)
					path=[]
					with open(combinefn,"r") as f:
						for l in f:
							dic=    convert_line_to_dic(l)
							path.append(dic)
					if iprint>=2:  print("combinefn path len %d"%len(path))

					ufn = iddir+os.sep+userFolder+os.sep+truetimestr+".gz"
					if not os.path.exists(ufn):
						if iprint>=2: print("Unknown car!\nNot exists: %s "%ufn)
						carkey="~|~|" # unknown
					else: 
						userinfo= get_user_car_info(ufn)
						carkey = userinfo[KeyCarMake]+CUT+userinfo[KeyCarModel]+CUT+userinfo[KeyCarYear]
					gasScale = carkey2scale[carkey]
					carkeyseen[carkey]=carkeyseen.get(carkey,0)+1
					if email in emailseen:
						emailseen[email].add(carkey)
					else: emailseen[email]=set([carkey])


					for mfn in mflist:
						nid_path_pos=[]

						with open(mfn, "r") as f:
							for l in f:
								st=l.split(" ")
								if len(st)>1:
									nid_path_pos.append([int(st[0]),int(st[1])])
						if iprint>=2: 
							print("%s nid num %d"%(mfn,len(nid_path_pos)))
							if len(bugTimes)>0: print(nid_path_pos)
						if len(nid_path_pos)<=5:
							if iprint>=2: print("Too Short! skip: "+mfn)
							continue

						dic=path[nid_path_pos[0][1]]
						splat = dic[KeyGPSLat]
						splng = dic[KeyGPSLng]
						dic=path[nid_path_pos[-1][1]]
						splat2 = dic[KeyGPSLat]
						splng2 = dic[KeyGPSLng]
						dist=get_dist_meters_latlng(splat,splng,splat2,splng2)
						dic=path[nid_path_pos[len(nid_path_pos)/2][1]]
						splat2 = dic[KeyGPSLat]
						splng2 = dic[KeyGPSLng]
						dist2=get_dist_meters_latlng(splat,splng,splat2,splng2)
						if Low_Dist_Thresh is None:
							if dist<500 and dist2<300:
								if iprint: print("Dist %.1f too small, skip: %s"%(dist,mfn))
								continue
						else:
							if dist<Low_Dist_Thresh and dist2<Low_Dist_Thresh/2.0:
								if iprint: print("Dist %.1f too small, skip: %s"%(dist,mfn))
								continue

						addr = latlng_to_city_state_country(splat,splng) # after geo mapping.
						requestFile = DirOSM+os.sep+"cityrequest.txt"
						approved=0
						if os.path.exists(requestFile): 
							with open(requestFile,"r") as f: 
								for l in f:
									l=l.strip()
									if len(l)>0:
										st=l.split("~|")
										if st[0].strip()==addr:
											if st[-1].strip()=="1":
												approved=1
						if approved==0:
							if iprint: print("Didn't approve "+addr)
							continue
						if addr in black_addr_list and len(bugEmails)==0: # don't use other cities ___
							continue

						osm_folder_path= DirOSM+os.sep+addr
						osm= osmPipeline(folder_path=osm_folder_path)
						osmname= osm.get_osm_file_path().split(os.sep)[-1].rstrip(".osm")

						mm_nid2latlng.use_cache(meta_file_name="osm/cache-%s-nodeid-to-lat-lng.txt"%osmname, params=Overwrite_mm_params )
						mm_nid2elevation.use_cache(meta_file_name="osm/cache-%s-nid-to-elevation.txt"%osmname, params=Overwrite_mm_params )
						mm_nids2speed.use_cache(meta_file_name="osm/cache-%s-nids-to-speed.txt"%osmname, params=Overwrite_mm_params )
						mm_nid2neighbor.use_cache(meta_file_name="osm/cache-%s-nodeid-to-neighbor-nid.txt"%osmname, params=Overwrite_mm_params )
						mm_nid2waytag.use_cache(meta_file_name="osm/cache-%s-nids-to-waytag.txt"%osmname, params=Overwrite_mm_params )

						ind2dist=[]
						baderr=0
						for ind in range(len(nid_path_pos)):
							if ind==0:
								ind2dist.append(0.0)
							else:
								latlng0 = mm_nid2latlng.get(nid_path_pos[ind-1][0])
								latlng1 = mm_nid2latlng.get(nid_path_pos[ind][0])
								if latlng0 is None or latlng1 is None:
									baderr=1
									print(nid_path_pos[ind-1][0],nid_path_pos[ind][0])
									print("\n\n\n bad ...3 !\n\n")
									break
								ind2dist.append( ind2dist[-1]+get_dist_meters_latlng2(latlng0,latlng1) )
						if baderr: # invalid cache
							break

						if iprint>=2: print(ind2dist,"ind2dist, len",len(ind2dist))

						roundsCuts2D=[[0,len(nid_path_pos)-1]] # ==1 for now.

						''' Find path pos according to cuts index '''

						for rn in range(len(roundsCuts2D)): # if you cut this trip. not for now.
							cutsInd=roundsCuts2D[rn]
							for cn in range(len(cutsInd)-1): # additional cuts. not for now.
								html_str=""
								html_nid_path_pos=[] # for html gen display
								html_loc=None
								
								train_nid_pos=[] # more precise path pos than nid_path_pos 
								ind=cutsInd[cn] # start 
								ind2=cutsInd[cn+1] # end 
								if iprint>=2: 
									pstr="|||--- Round %d, cut %d, [%d,%d]/%d:"%(rn,cn,ind,ind2,len(nid_path_pos))
									html_str+=pstr+". "
									print(pstr)

								''' Find start path pos :'''
								if iprint>=2: print("-------------- Find start --------------")
								nid0 = nid_path_pos[ind][0]
								nid1 =nid_path_pos[ind+1][0]
								latlng0 = mm_nid2latlng.get(nid0)
								latlng1 = mm_nid2latlng.get(nid1)
								if iprint>=2:print("nid0,latlng0",nid0,latlng0,"nid1,latlng1",nid1,latlng1)
								# find pos closest to latlng0:
								startpos = find_start_path_pos_on_seg(path,nid_path_pos[ind][1],nid_path_pos[ind+1][1],latlng0,latlng1)
								if startpos is None:
									pstr="invalid start "+mfn+str(latlng0)
									html_str+=pstr+". "
									if iprint: print(pstr)
									invalidStart=True
								else:
									invalidStart=False
									html_nid_path_pos.append([nid0,startpos])
									train_nid_pos.append([nid0,startpos])
									if iprint>=2: print(html_nid_path_pos)
								''' try next, train-dist may be less '''
								while startpos is None:
									ind+=1
									nid0 = nid_path_pos[ind][0]
									nid1 =nid_path_pos[ind+1][0]
									latlng0 = mm_nid2latlng.get(nid0)
									latlng1 = mm_nid2latlng.get(nid1)
									if iprint>=2: 
										print("try next start, nid0,latlng0",nid0,latlng0,"nid1,latlng1",nid1,latlng1)
									# find pos closest to latlng0:
									startpos = find_start_path_pos_on_seg(path,nid_path_pos[ind][1],nid_path_pos[ind+1][1],latlng0,latlng1)
									if startpos is not None: 
										html_nid_path_pos.append([nid0,startpos])
										train_nid_pos.append([nid0,startpos])


								''' Find end path pos '''
								if iprint>=2: print("-------------- Find end --------------")
								nid0 = nid_path_pos[ind2-1][0]
								nid1 =nid_path_pos[ind2][0]
								latlng0 = mm_nid2latlng.get(nid0)
								latlng1 = mm_nid2latlng.get(nid1)
								if iprint>=2:print("nid0,latlng0",nid0,latlng0,"nid1,latlng1",nid1,latlng1)
								# find pos closest to latlng1:
								endpos= find_end_path_pos_on_seg(path,nid_path_pos[ind2][1],len(path),latlng0,latlng1)
								if endpos is None:
									pstr="invalid end "+mfn+" ind2=%d "%ind2+str(latlng1)
									html_str+=pstr+". "
									if iprint>=2: print(pstr)
									invalidEnd=True
								else:
									invalidEnd=False
								''' try next, train-dist may be less '''
								lastlatlng = latlng1
								while endpos is None:
									ind2-=1
									nid0 = nid_path_pos[ind2-1][0]
									nid1 =nid_path_pos[ind2][0]
									latlng0 = mm_nid2latlng.get(nid0)
									latlng1 = mm_nid2latlng.get(nid1)
									if iprint>=2: 
										print("try next end, nid0,latlng0",nid0,latlng0,"nid1,latlng1",nid1,latlng1)
									# find pos closest to latlng1:
									endpos= find_end_path_pos_on_seg(path,nid_path_pos[ind2][1],len(path),latlng0,latlng1)
								if invalidEnd:
									tmpd=get_dist_meters_latlng2(lastlatlng,latlng0)
									html_str+=" cut end dist %.1f. "%tmpd
									if iprint>=2: print(" cut end dist %.1f. "%tmpd)
									if tmpd<100 or ind2>=len(nid_path_pos)-2: # last seg allows fail.
										invalidEnd=False # forgive it.


								''' Check path in between every seg:'''
								if iprint>=2: print("-------------- Check middle --------------")
								problem=0
								lastsegend=startpos
								for i in range(ind+1,ind2):
									nid0 = nid_path_pos[i][0] # end node of this seg.
									nid1 = nid_path_pos[i+1][0] # next end node of next seg. 
									latlng0 = mm_nid2latlng.get(nid0)
									nextLatlng = mm_nid2latlng.get(nid1)
									if iprint>=4:print("nid0,latlng0",nid0,latlng0,"nid1,nextLatlng",nid1,nextLatlng)
									if iprint>=4: print("finding start of %d to %d"%(nid0,nid1))
									segend=find_start_path_pos_on_seg(path,nid_path_pos[i][1],len(path),latlng0,nextLatlng, print_str=len(bugTimes)>0)
									if segend is None:
										problem=1
										pstr=["middle, segEnd None!","ind",i,latlng0, "%d,%d,%d"%(nid_path_pos[i-1][0],nid_path_pos[i][0],nid_path_pos[i+1][0])]
										pstr=",".join([str(x) for x in pstr])
										if iprint>=2: print(pstr)
										html_str+=pstr+". "
										html_loc=latlng0[:]
										break
									else:
										html_nid_path_pos.append([nid0,segend])
										train_nid_pos.append([nid0,segend]) # this upto 2nd to last nid
										if i==ind2-1: # append last nid as endpos.
											html_nid_path_pos.append([nid_path_pos[ind2][0],endpos])
											train_nid_pos.append([nid_path_pos[ind2][0],endpos])

									badcnt=0
									prevNid=nid_path_pos[i-1][0] # begin node of this seg.
									prevLatlng=mm_nid2latlng.get(prevNid)
									hd1=get_bearing_latlng2(prevLatlng,latlng0)
									d1 = get_dist_meters_latlng2(prevLatlng,latlng0)
									for pp in range(lastsegend, segend+1):
										dic=path[pp]
										splat = dic[KeyGPSLat]
										splng = dic[KeyGPSLng]
										sphead= find_bearing(path, pp,search_forward=1)
										anglediff= min_angle_diff(sphead,hd1)
										dist = dist_point_to_line_of_2pts([splat,splng],latlng0,prevLatlng)
										if d1<kTrivialSegLength:
											if (anglediff>=kAngle90Thresh+20 or dist>50+10):
												badcnt+=1
												if iprint>=2 and len(bugTimes)>0:
													print("mid sm bad?","anglediff",anglediff,"d",dist,"pp",pp,"[%d,%d]"%(lastsegend, segend+1),"hd1",hd1)
										else:
											if (anglediff>=kAngle90Thresh or dist>50):
												badcnt+=1
												if iprint>=2 and len(bugTimes)>0:
													print("mid bad?","anglediff",anglediff,"d",dist,"pp",pp,"[%d,%d]"%(lastsegend, segend+1),"hd1",hd1)
										if badcnt>=10:
											problem=1
											pstr=["middle bad!","pp",pp,"ind",i,i+1,dic,"anglediff",anglediff,"d",dist]
											pstr=",".join([str(x) for x in pstr])
											if iprint>=2: print(pstr)
											html_str+=pstr+". "
											html_str+="middle, hd1=%.1f (%d,%d,%d)"%(hd1,prevNid,nid0,nid1)+" sphead="+str(sphead)+' segstart %d, segend %d.'%(lastsegend, segend+1)
											html_loc=[splat,splng]
											break
									lastsegend=segend

									if problem>0:
										if iprint>=2: 
											pstr="seg problem at nid %d,%d"%(nid0,nid1)
											if iprint>=2: print(pstr)
											html_str+=pstr+". "
										break

								if problem>0 or invalidEnd or invalidStart:
									if iprint>=2: 
										print("seg invalid after path %",nid_path_pos[i][1],len(path))
										print("invalidEnd or invalidStart?",invalidEnd , invalidStart)
									if combinefn not in invalidCombineFn2cnt:
										invalidCombineFn2cnt[combinefn]=1
										htmlf="./%s/"%DirHTML+email+"-"+mfn.split(os.sep)[-1]+"-%d-%d.html"%(rn,cn)
										html_str+=mfn+". "
										if len(html_nid_path_pos)>0:
											gen_html_given_path_nid_ind_list(path,html_nid_path_pos,htmlf,addr,comment=html_str,add_tail=True,loc_pt=html_loc)
										else:
											html_str+="empty html_nid_path_pos."
											gen_html_given_path_nid_ind_list(path,nid_path_pos,htmlf,addr,comment=html_str,add_tail=True,loc_pt=html_loc)
									else:
										invalidCombineFn2cnt[combinefn]+=1

									if problem>0: continue

								if len(bugTimes)>0:
									htmlf="./%s/"%DirHTML+email+"-"+mfn.split(os.sep)[-1]+"-%d-%d.html"%(rn,cn)
									html_str+=mfn+". "
									if len(html_nid_path_pos)>0:
										gen_html_given_path_nid_ind_list(path,html_nid_path_pos,htmlf,addr,comment=html_str,add_tail=True,loc_pt=html_loc)
									else:
										html_str+="empty html_nid_path_pos."
										gen_html_given_path_nid_ind_list(path,nid_path_pos,htmlf,addr,comment=html_str,add_tail=True,loc_pt=html_loc)
								if iprint>=2: 
									print("\n|----- Valid round %d, cut %d, gen train sample"%(rn,cn))

								# try:
								if 1:
									''' cut according to way tag type '''
									cut_train_nid_pos=[]
									last_tag_type=None
									cumuTagDist= 0.0
									for i in range(len(train_nid_pos)-1):
										nid0 = train_nid_pos[i][0]
										nid1 = train_nid_pos[i+1][0]
										tag_type= get_spd_type_given_nid2(nid0,nid1)
										if last_tag_type is not None and abs(tag_type-last_tag_type)>1 and cumuTagDist>Train_trace_cut_min_dist:
											cut_train_nid_pos.append(train_nid_pos[i][:])
											
											spstr= gen_sample(path=path,train_nid_pos=cut_train_nid_pos,gasScale=gasScale,mfn=mfn,carkey=carkey,mm_nid2elevation=mm_nid2elevation,mm_nid2latlng=mm_nid2latlng,mm_nid2neighbor=mm_nid2neighbor,semaphore=semaphore,mm_train_turn=mm_train_turn_use,mm_train=mm_train_use,tag_type=last_tag_type,addr=addr,print_str=False, isTest=0, returnSampleStr=False, email=email,testcnt=testcnt,car2meta=car2meta,bugTimes=bugTimes)
											if iprint>=2: 
												print([x[0] for x in cut_train_nid_pos],last_tag_type)
											if spstr is not None and len(spstr)>1:
												html_str+="\n"+spstr
												htmlf="./html2/"+email+"-"+mfn.split(os.sep)[-1]+"-%d-%d.html"%(rn,i)
												gen_html_given_path_nid_ind_list(path,cut_train_nid_pos,htmlf,addr,comment=html_str)

											cut_train_nid_pos=[]
											cut_train_nid_pos.append(train_nid_pos[i][:])
											cumuTagDist=get_dist_meters_osm_nid2(nid0,nid1)

										else:
											cut_train_nid_pos.append(train_nid_pos[i][:])
											cumuTagDist+= get_dist_meters_osm_nid2(nid0,nid1)
										last_tag_type=tag_type

									if len(cut_train_nid_pos)>1 and cumuTagDist>FilterMinDist*MetersPerMile:
										cut_train_nid_pos.append(train_nid_pos[i+1][:])
										
										gen_sample(path=path,train_nid_pos=cut_train_nid_pos,gasScale=gasScale,mfn=mfn,carkey=carkey,mm_nid2elevation=mm_nid2elevation,mm_nid2latlng=mm_nid2latlng,mm_nid2neighbor=mm_nid2neighbor,semaphore=semaphore,mm_train_turn=mm_train_turn_use,mm_train=mm_train_use,tag_type=last_tag_type,addr=addr,print_str=False, isTest=0, email=email,testcnt=testcnt,car2meta=car2meta,bugTimes=bugTimes)
										if iprint>=2: 
											print([x[0] for x in cut_train_nid_pos],last_tag_type,"tail")
									
								# except Exception as e:
								# 	print("Exception [gen_sample] "+mfn)
								# 	print(e)
									# sys.exit(1)

						if testcnt>0: 
							testcnt-=1
							if testcnt<=0: sys.exit(0)
									
			# except Exception as e:
			# 	print("[ exception ] "+combinefn)
			# 	print(e)
				# raise e
	
	if iprint: 
		print("invalidCombineFn2cnt",invalidCombineFn2cnt)
		print("max_key",mm_train_use.get("max_key"))
		print("max_key_turn",mm_train_turn_use.get("max_key_turn"))



if __name__ == "__main__":
	
	arglist=sys.argv[1:]

	if "gen_train" in arglist: 
		gen_train(mm_train, mm_train_turn)



