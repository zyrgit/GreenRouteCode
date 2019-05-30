# GreenRoute Main Code

This code was developed to run on campus cluster, you can tailor it to suit your needs. The projects needs other components, such as `~/greendrive/*` and `~/cachemeta/*` folders. The main steps to process a region is the following.

Note, I have an alias in Linux command for Python, as specified in `~/.bashrc`:
```
alias py='$HOME/anaconda2/bin/python'
```

---
## Find link of *.osm from:
http://download.geofabrik.de/north-america.html

For example:
```
ADDR=NewYork,US
URL=http://download.bbbike.org/osm/bbbike/NewYork/NewYork.osm.gz
mkdir -p ~/greendrive/osmdata/$ADDR/
cd ~/greendrive/osmdata/$ADDR/
wget -O $ADDR.osm.gz $URL
gunzip $ADDR.osm.gz
# check osm file name, should be *,US.osm
```

---
## Modify mytools/geo.py
### feed your latlng to get address:
For example, see bottom section of geo.py: `latlng_to_city_state_country(39.838334, -86.144976)`
Run: `py geo.py latlng`
You can see the '<County>,<State>,US' printed, if you process state-level osm, then you do not need County level info in address.

