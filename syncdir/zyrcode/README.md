# GreenRoute Main Code

This code was developed to run on the campus cluster, you can tailor it to suit your needs. The project needs other components, such as `~/greendrive/*` and `~/cachemeta/*` folders. The main steps to process a new region are the following.

Note, I have an alias in Linux command for Python, as specified in `~/.bashrc`:
```
alias py='$HOME/anaconda2/bin/python'
```

---
## Download OSM from:
http://download.bbbike.org
Or
http://download.geofabrik.de/north-america.html

For example:
```
ADDR=NewYork,US
URL=http://download.bbbike.org/osm/bbbike/NewYork/NewYork.osm.gz
mkdir -p ~/greendrive/osmdata/$ADDR/
cd ~/greendrive/osmdata/$ADDR/
wget -O $ADDR.osm.gz $URL
gunzip $ADDR.osm.gz
# Change osm file name, should be NewYork,US.osm
```

---
## Modify `./mytools/geo.py`
### Feed your [lat,lng] to get address:
For example, see bottom section of `geo.py`: `latlng_to_city_state_country(39.838334, -86.144976)`.

Run: `py geo.py latlng`.
You can see the `County,State,US` printed.
If you process state-level osm, then you do not need `County` level info in address.

### Overwrite address by using state names:
Modify func: `latlng_to_city_state_country()`.

Run: `py geo.py latlng`.
Add a few lines of code at the end of this function to overwrite address. 
If you process state-level osm, this will map latlng points to their state address. 

Now output should be: `Illinois,US`, `NewYork,US`, `Indiana,US`, etc.

### Add bounding box [s,n,w,e]:
Modify func: `load_geo_addr_mapping_and_bbox()`.
Run: `py geo.py map`.

This stores `State,US` address to bounding-box (bbox) mapping into Redis. 
You need to get bbox from Google Maps yourself. 


---
## Add address status to `cityrequest.txt` file:
Create/Find: `~/greendrive/osmdata/cityrequest.txt`.
Add a line like: `Indiana,US~|1`

`1` means this address is approved. When new GPS is found, a new address request will be added to this file, with `0` as its status, if you are not ready to process this address, do not write `1`.


---
## Configure cache's key-prefix:
Redis stores key-value mappings, a key is added with a prefix, but long prefix wastes memory, so we shorten it.
Refer to `./cache/prefix.txt`, prefixes look like those. Just the address part is different for different states.
### Add cache prefix, append only:
`vim ~/cachemeta/prefix.txt`
### Generate mappings from long prefixes to short ones:
Go to `./mytools/`, 
Run: `py CacheManager.py genprefix`,
This will generate `prefixTranslate.pkl` as a pickled dict. 


---
## Process a new state, after the above is prepared:
`3genOsmCache.py` is the main code. Make sure you have Redis running on port 6380.
For example, if you have 14 servers, you can run:
```
py 3genOsmCache.py gen_osm addr=Indiana,US max=14 s=0 t=10
```
For example, if you have 1 PC, and want to overwrite previously generated cache, you can run:
```
py 3genOsmCache.py gen_osm addr=Illinois,US max=1 s=0 t=1 o
```
Check out the code yourself. It takes a few days to crawl/query/process, use `screen` with `cssh` if necessary. 

Tasks completion will generate mark files like `~/greendrive/osmdata/Illinois,US/COMPLETE-download_osm`. All cached mappings will be loaded into Redis on your PC or cluster. 


---
## Generate edge/turn costs:
After all cache ready, generate fuel-related cost files for OSRM, run:
`py genSpeedSegOSRM.py il fuel`.

See the bottom of the code, `il` is short for address `Illinois,US`, `fuel` means to generate fuel-related cost files.

---
## Add docker for OSRM.
Docker for OSRM has both frontend and backend, although you only need backend if you do not need UI. Backend server listens on port 5000 by default, but different dockers (for different states) can use specific port like 5001, etc. Frontend server listens on 9966 port by default, and requests are sent to backend server.

The backend needs OSM file, cost-files (`~/greendrive/osmdata/Illinois,US/data/*`) as input. 
To run shell script for backend docker, you also need to write a `./docker/Dockerfile_Illinois,US` configuration file. 

Go to `./docker/`. Run backend at port 5001:
```
bash docker-back.sh Illinois,US 5001 
```
Run frontend at port 9966 (currently I did not figure out how to use other ports, let me know if you do):
```
bash docker-front.sh Illinois,US 5001 9966
```

---
## API for fuel-efficient routing:
After the backend docker starts running, you can query the route between two GPS points, say from `{'lat':40.098083, 'lng':-88.219134}` to `{'lat':40.0806, 'lng':-88.218833}`, using:
```
curl "http://BackendIP:5001/route/v1/driving/-88.219134,40.098083;-88.218833,40.0806"
```
Read the OSRM documentation for more options and details at http://project-osrm.org/docs/v5.15.2/api/#route-service.





