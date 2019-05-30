#!/usr/bin/env python

# As docker container:
OSRM_IL_IP='localhost:5000'
OSRM_NY_IP='localhost:5001'
OSRM_IN_IP='localhost:5002'

addr2ip={
"Illinois,US":OSRM_IL_IP,
"NewYork,US": OSRM_NY_IP,
"Indiana,US":OSRM_IN_IP,
}

OSRM_Backend="router.project-osrm.org"
URL_Route = "http://{Backend}/route/v1/driving/{Loc}?annotations=true"  
URL_Match = "http://{Backend}/match/v1/driving/{Loc}?annotations=true"

NumFold=5 # for train
FilterMinDist=1.0 # miles dist is not filter. 200m+
FilterMaxDist=10.0 # >100 miles is not filter.10
FilterSpeedUpperThresh =70.0 # >100 mph is not filter.60+
FilterSpeedLowerThresh =13.0 # 0 mph is not filter.10-
FilterMpgUpperThresh=35.0 # >100 is not filter.38+
FilterMpgLowerThresh=14.0 # 0 is not filter.14-

Train_trace_cut_min_dist=500 # if cut by waytag
Trip_sample_min_dist=200 # 5.py gen sample

Mode_Fuel=0
Mode_Shortest=1

Use_get_gas_by_diff=0 # if you have intercept_ Not in use
Use_directly_cumu_gas=1 # don't calc gas by add a seg and take subtraction. 

kTrivialSegLength=30 # used in func.py
kAngle90Thresh=90 # used in find-start/end

indMass=0 # for /stats/car_meta
indArea=1
indDrag=2

GramPerGallon = 2834
MetersPerMile = 1609.344
Mph2MetersPerSec = 0.44704

Kdist="dis"
Kelevation="ele"
Kgas="gas" # rescaled/normalized
Krealgas="realgas"
KincSpeed2="incV2" # all segs in a trip.
Kv2d="v2d"
Kvd="vd"
Ktime="time" # OSRM estimated time, dist/spd
KSmjcrs="mjn"
# turn features:
TSelfV="t0v" # U turn leg speed
TLeftV="t1v" # left turn leg speed
TRightV="t3v" # right turn leg speed
TStraightV="t2v" # Straight leg speed
TminV="tminv" # min speed seen around cross.
TspdDec="tvdec" # speed dec percent at cross.
TNleft="tn1" # number of left turns in trip.
TNright="tn3"
TNstraight="tn2"
Ttype="ttyp"
TstopLeft="ts1" # number of truely stopped turns in trip.
TstopRight="ts3"
TstopStraight="ts2"
TnidFrom="tfr" # nid of turn from 
TnidAt="tat" # nid of turn at cross
TnidLeft="tle"
TnidRight="tri"
TnidStraight="tst"
TPleft="tp1" #cumu prob of stopping at cross. 
TPright="tp3"
TPstraight="tp2"
TPspd2inc="pv2inc" # from speed dec distrib 
Ttime="tti" # true
KaddWaitTime="wtime"
TPwtime="pwti" # trip time plus prob * wait time.
KtripTime="tripti" # true whole trip time.
KtagType="tag" # slow/medium/fast
KelevDecNeg="eleDec" # downhill, negative of elevation decrease
KelevInc="eleInc" # just increment, after adjust end-to-end (may decrease)
KisTest="istest" # has test code?
RealSegSpeedinc='rvinc' # like greengps, real block v^2 inc. 

KMmass="mass"
KMair="drag"
KMarea="area"
KMmdv2="mdv2"
KMmelev="mele"
KMav2d ="av2d"
KMdragv2d ="drav2d"
KMmd = "md"
KMvd = "v*d"
KMmleft="ms1"
KMmright="ms3"
KMmstraight="ms2"
KMmtime="mtime"

CMEMv0='cmv0' # f = sum ci* v^i 
CMEMv1='cmv1'
CMEMv2='cmv2'
CMEMv3='cmv3'
CMEMv4='cmv4'

#VT-CPFEM:
VTCPFEMv0='vtv0'
VTCPFEMv1='vtv1'
VTCPFEMv2='vtv2'
VTCPFEMv3='vtv3'
VTCPFEMv4='vtv4'
VTCPFEMv5='vtv5'
VTCPFEMv6='vtv6'
VTCPFEMa2v2='vta2v2'
VTCPFEMav1='vtav1'
VTCPFEMav2='vtav2'
VTCPFEMav3='vtav3'
VTCPFEMav4='vtav4'


Turn_Left=1
Turn_Right=3
Turn_Straight=2
T_turn01=4
T_turn02=5
T_turn12=6
T_turn10=7
T_turn20=8
T_turn21=9
kMinTurnAngleDiff=10.0
kMinTurnGasPenalty=0.0

SHVL=0 # high speed on self, low on verticle legs.
SVSame=1 # same even speed.
SLVH=2 # high speed on verticle legs.
typDesc=["","Turn Left","Turn Straight","Turn Right","T 01","T 02","T 12","T 10","T 20","T 21"]
proDesc=["high speed on self","same speed","high speed on verticle"]
VTdesc=["On slow road,","On med road,","On fast road,"]
Spd_Type_Slow=0
Spd_Type_Medium=1
Spd_Type_Fast=2
Vtype_lower=0
Vtype_higher=1

Highway_Fast_taglist=["motorway","trunk","primary","secondary","tertiary"] #>12m/s
Highway_Medium_taglist=["residential","motorway_link","trunk_link","primary_link","secondary_link","tertiary_link"]
Highway_Slow_taglist=["service","track","unclassified"] #<4m/s

WayTags=["motorway","trunk","primary","secondary","tertiary","unclassified","residential","motorway_link","trunk_link","primary_link","secondary_link","tertiary_link","service","footway"] # not routable, as in geo.py 


G_normCars=["Subaru~|Impreza~|2010"]

G_blackListCars=[
'~|~|',
]

G_new_test_cars=[
'Volkswagen~|CC~|2015',
'Nissan~|Altima~|2013', 
'Ford~|Fusion~|2010',
'Toyota~|Camry~|2016', 
'Honda~|Accord~|2008',
'Volkswagen~|CC~|2013',
'Mazda~|Mazda6~|2008',
]

G_adjust_gas_scale_ratio={# not in use
}


G_all_cars = set(['Buick~|LeSabre~|2002','Chevrolet~|Impala~|2002','Chevrolet~|Impala~|2017','Dodge~|Ram-1500~|2002','Ford~|E-250~|2011','Ford~|Explorer~|2012','Ford~|Focus~|2000','Ford~|Fusion~|2010','Ford~|Ranger~|2008','Ford~|Taurus~|2002','Honda~|Accord~|2005','Honda~|Accord~|2008','Hyundai~|Tucson~|2011','Mazda~|Mazda6~|2003','Mazda~|Mazda6~|2008','Mitsubishi~|Galant~|2002','Nissan~|Altima~|2013','Subaru~|Impreza~|2010','Toyota~|Camry~|2004','Toyota~|Camry~|2012','Toyota~|Camry~|2015','Toyota~|Camry~|2016','Toyota~|Celica~|2000','Toyota~|Corolla~|2000','Toyota~|Corolla~|2010','Volkswagen~|CC~|','Volkswagen~|CC~|2013','Volkswagen~|CC~|2015','Volkswagen~|CC~|2016','Volkswagen~|~|','~|~|'])

for c in G_blackListCars:
	G_all_cars.remove(c)


G_old_cars= set(G_all_cars)
for c in G_new_test_cars:
	if c in G_old_cars: G_old_cars.remove(c)
