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
For example, see bottom section of `geo.py`: `latlng_to_city_state_country(39.838334, -86.144976)`

Run: `py geo.py latlng`
You can see the `County,State,US` printed.
If you process state-level osm, then you do not need `County` level info in address.

### Overwrite address by using state names:
Modify func: `latlng_to_city_state_country()`

Run: `py geo.py latlng`
Add a few lines of code at the end of this function to overwrite address. 
If you process state-level osm, this will map latlng points to their state address. 

Now output should be: `Illinois,US`, `NewYork,US`, `Indiana,US`, etc.

### Add bounding box [s,n,w,e]:
Modify func: `load_geo_addr_mapping_and_bbox()`
Run: `py geo.py map`

This stores `State,US` address to bounding-box (bbox) mapping into Redis. 
You need to get bbox from Google Maps yourself. 

---
## Add address status to `cityrequest.txt` file:
Create/Find: `~/greendrive/osmdata/cityrequest.txt`
Add a line like: `Indiana,US~|1`

`1` means this address is approved. When new GPS is found, a new address request will be added to this file, with `0` as its status, if you are not ready to process this address, do not write `1`.











