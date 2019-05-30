#!/bin/bash
# no dockerfile, use git

MyIp=`sh $HOME/get_my_ip.sh`
echo $MyIp
if [ -z "$MyIp" ]; then
	echo "Bad IP:"
	echo $MyIp
	exit
fi

BASEDIR=$(dirname "$0")
echo "my BASEDIR:"
echo $BASEDIR

BackendIp=$MyIp
echo "BackendIp"
echo $BackendIp
ADDR="$1"
echo "ADDR"
echo $ADDR
if [ -z $ADDR ]; then
	echo "Give ADDR as 1st argv!"
	exit
fi


OSMBaseDir=$HOME/greendrive/osmdata/$ADDR
SubDir=front



case $MyIp in
172.22.68* ) 
echo "---------- on cluster NFS !!!! ------ "

ret=`python $BASEDIR/getLatLngGivenAddr.py addr $ADDR`
vars=($ret) # use 'bash *.sh' instead of 'sh *' if you see error here.
lat=${vars[0]}
lng=${vars[1]}
echo "$lat $lng"

DockerDir=$HOME/greendrive/osmdata/$ADDR/docker
MyDir=$DockerDir/$SubDir/$MyIp
if [ ! -d "$MyDir" ]; then
	echo "mkdir $MyDir"
	mkdir -p $MyDir
fi

cd $MyDir/
echo $(pwd)
if [ ! -d "osrm-frontend" ]; then
	echo "git clone"
	git clone https://github.com/Project-OSRM/osrm-frontend.git
fi
cd osrm-frontend/

# vim docker/Dockerfile
ModFile=docker/Dockerfile
if ! grep -q "http://$BackendIp" $ModFile; then 
	sed -i "s/localhost/$BackendIp/" $ModFile
fi
if ! grep -q "$lat,$lng" $ModFile; then 
	sed -i "s/ENV OSRM_CENTER=.*/ENV OSRM_CENTER='$lat,$lng'/" $ModFile
fi

# vim src/leaflet_options.js # NO HTTPS !!! 
ModFile=src/leaflet_options.js
if ! grep -q "path: 'https://$BackendIp" $ModFile; then 
	sed -i "s/    path: 'https:.*/    path: 'http:\/\/$BackendIp:5000\/route\/v1'/" $ModFile
fi
if ! grep -q "$lat,$lng" $ModFile; then 
	sed -i "s/    center: L.latLng(.*/    center: L.latLng($lat,$lng),/" $ModFile
fi
docker build . -f docker/Dockerfile -t osrm-frontend
echo "Frontend   http://$MyIp:9966"
echo "Backend   http://$BackendIp:5000"
docker run -p 9966:9966 -d --restart=unless-stopped osrm-frontend:latest



;;
* ) 
echo "---------- on PC ------------ "
# MyIp=0.0.0.0 # just use office IP
echo "MyIp"
echo $MyIp
# BackendIp=0.0.0.0 # just use office IP
echo "BackendIp"
echo $BackendIp

BackendPort="$2" # 5000
echo "BackendPort"
echo $BackendPort
if [ -z "$BackendPort" ]; then
	echo "Bad backend Port:"
	echo $BackendPort
	exit
fi

FrontendPort="$3" # 9966
echo "FrontendPort"
echo $FrontendPort
if [ -z "$FrontendPort" ]; then
	echo "Bad frontend Port:"
	echo $FrontendPort
	exit
fi

ret=`python $BASEDIR/getLatLngGivenAddr.py addr $ADDR`
vars=($ret) # use 'bash *.sh' instead of 'sh *' if you see error here.
lat=${vars[0]}
lng=${vars[1]}
echo "$lat $lng"

DockerDir=$HOME/greendrive/osmdata/$ADDR/docker
MyDir=$DockerDir/$SubDir/$MyIp
if [ ! -d "$MyDir" ]; then
	echo "mkdir $MyDir"
	mkdir -p $MyDir
fi

cd $MyDir/
echo $(pwd)
if [ ! -d "osrm-frontend" ]; then
	echo "git clone"
	git clone https://github.com/Project-OSRM/osrm-frontend.git
fi
cd osrm-frontend/

# vim docker/Dockerfile
ModFile=docker/Dockerfile

if ! grep -q "http://$BackendIp" $ModFile; then 
	sed -i "s/localhost/$BackendIp/" $ModFile
fi
if ! grep -q ":$BackendPort" $ModFile; then 
	sed -i "s/:5000/:$BackendPort/" $ModFile
fi
if ! grep -q "$lat,$lng" $ModFile; then 
	sed -i "s/ENV OSRM_CENTER=.*/ENV OSRM_CENTER='$lat,$lng'/" $ModFile
fi
if ! grep -q "EXPOSE $FrontendPort" $ModFile; then 
	sed -i "s/EXPOSE.*/EXPOSE $FrontendPort/" $ModFile
fi
echo "---------------------"
cat $ModFile
echo "---------------------"

# vim src/leaflet_options.js # NO HTTPS !!! 
ModFile=src/leaflet_options.js
if ! grep -q "path: 'https://$BackendIp" $ModFile; then 
	sed -i "s/    path: 'https:.*/    path: 'http:\/\/$BackendIp:$BackendPort\/route\/v1'/" $ModFile
fi
if ! grep -q "$lat,$lng" $ModFile; then 
	sed -i "s/    center: L.latLng(.*/    center: L.latLng($lat,$lng),/" $ModFile
fi

echo -e "\ndocker build . -f docker/Dockerfile -t osrm-frontend"
docker build . -f docker/Dockerfile -t osrm-frontend
echo "Frontend   http://$MyIp:$FrontendPort"
echo "Backend   http://$BackendIp:$BackendPort"

echo -e "\ndocker run -p $FrontendPort:$FrontendPort -d --restart=unless-stopped osrm-frontend:latest"
docker run -p $FrontendPort:$FrontendPort -d --restart=unless-stopped osrm-frontend:latest




;;
esac








echo "done!"

