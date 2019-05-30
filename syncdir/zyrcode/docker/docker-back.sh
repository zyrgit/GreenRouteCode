#!/bin/bash


MyIp=`sh $HOME/get_my_ip.sh`
echo "MyIp"
echo $MyIp
BASEDIR=$(dirname "$0")
echo "BASEDIR"
echo $BASEDIR

# ADDR=Illinois,US
ADDR="$1"
echo "ADDR"
echo $ADDR
BackendPort="$2" # 5000
echo "BackendPort"
echo $BackendPort

if [ -z $ADDR ]; then
	echo "Give ADDR as argv!"
	exit
fi
if [ -z "$MyIp" ]; then
	echo "Bad IP:"
	echo $MyIp
	exit
fi


OSMBaseDir=$HOME/greendrive/osmdata/$ADDR
OSMfile=$OSMBaseDir/$ADDR.osm
OSRMInputDataDir=$OSMBaseDir/data
SubDir=back


case $MyIp in
172.22.68* ) 
echo -e "\n---------- on cluster (NFS) !!!! ------ "

DockerDir=$HOME/greendrive/osmdata/$ADDR/docker
MyDir=$DockerDir/$SubDir/$MyIp
DockerFile=Dockerfile$MyIp # cluster, IP suffix is enough.

if [ ! -d "$MyDir" ]; then
	echo "mkdir $MyDir"
	mkdir -p $MyDir
fi
echo -e "\ncp $BASEDIR/$DockerFile $MyDir/"
cp $BASEDIR/$DockerFile $MyDir/
echo -e "\ncp $BASEDIR/dockerignore $DockerDir/../.dockerignore"
cp $BASEDIR/dockerignore $DockerDir/../.dockerignore

cd $DockerDir/../
echo -e "\ncd $(pwd)"

echo -e "\ndocker build --build-arg addr=$ADDR -t my-osrm-backend -f docker/$SubDir/$MyIp/$DockerFile ."
docker build --build-arg addr=$ADDR -t my-osrm-backend -f docker/$SubDir/$MyIp/$DockerFile .

echo -e "\ndocker run -p 5000:5000 -d --restart=unless-stopped my-osrm-backend:latest osrm-routed --algorithm ch /data/$ADDR.osrm"
docker run -p 5000:5000 -d --restart=unless-stopped my-osrm-backend:latest osrm-routed --port 5000 --algorithm ch /data/$ADDR.osrm


;;
* ) 
echo "---------- on PC ------------- "
# MyIp=0.0.0.0 # just use office IP
echo "MyIp"
echo $MyIp
if [ -z "$BackendPort" ]; then
	echo "Bad Port:"
	echo $BackendPort
	exit
fi

DockerDir=$HOME/greendrive/osmdata/$ADDR/docker
MyDir=$DockerDir/$SubDir/$MyIp
DockerFile=Dockerfile_$ADDR # PC, addr suffix is enough.

if [ ! -d "$MyDir" ]; then
	echo "mkdir $MyDir"
	mkdir -p $MyDir
fi
echo -e "\ncp $BASEDIR/$DockerFile $MyDir/"
cp $BASEDIR/$DockerFile $MyDir/
echo -e "\ncp $BASEDIR/dockerignore $DockerDir/../.dockerignore"
cp $BASEDIR/dockerignore $DockerDir/../.dockerignore

cd $DockerDir/../
echo -e "\ncd $(pwd)"

echo -e "\ndocker build --build-arg addr=$ADDR -t my-osrm-backend -f docker/$SubDir/$MyIp/$DockerFile ."
docker build --build-arg addr=$ADDR --build-arg port=$BackendPort -t my-osrm-backend -f docker/$SubDir/$MyIp/$DockerFile .

echo -e "\ndocker run -p $BackendPort:$BackendPort -d --restart=unless-stopped my-osrm-backend:latest osrm-routed --port $BackendPort --algorithm ch /data/$ADDR.osrm"
docker run -p $BackendPort:$BackendPort -d --restart=unless-stopped my-osrm-backend:latest osrm-routed --port $BackendPort --algorithm ch /data/$ADDR.osrm



;;
esac







echo "done!"
