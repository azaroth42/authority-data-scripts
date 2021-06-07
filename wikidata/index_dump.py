import gzip
import json
import sys
import time
from sqlitedict import SqliteDict

fh = gzip.open('data/wikidata-json.gz')
fh.readline() # trash the first line, just a '['
out_db = SqliteDict('wikidata_items.sqlite', autocommit=False)
p_meta = {}

def process_json(js):
	new = {}
	new['id'] = js['id']

	lbls = {}
	for (k,v) in js['labels'].items():
		lbls[k] = v['value']
	descs = {}
	for (k,v) in js['descriptions'].items():
		descs[k] = v['value']	
	altLbls = {}
	for (k,v) in js['aliases'].items():
		altLbls[k] = [z['value'] for z in v]
	new['prefLabel'] = lbls
	new['altLabel'] = altLbls
	new['description'] = descs

	for (prop, vals) in js['claims'].items():
		if not prop in p_meta:
			# record the value datatype
			p_meta[prop] = vals[0]['mainsnak']['datatype']

		newvals = []
		for val in vals:
			if val['mainsnak']['snaktype'] in ["somevalue", 'novalue']:
				continue

			dv = val['mainsnak']['datavalue']
			dvt = dv['type']
			if dvt == "wikibase-entityid":
				dvv = dv['value']['id']
			elif dvt in ["external-id", 'string']:
				dvv = dv['value']
			elif dvt == "time":
				dvv = {'time': dv['value']['time'], 'precision': dv['value']['precision']}
			elif dvt in ["commonsMedia", 'url']:
				dvv = dv['value']['value']
			elif dvt == "monolingualtext":
				t = dv['value']['text']
				lang = dv['value']['language']
				dvv = {lang:t}
			elif dvt == "quantity":
				t = dv['value']['amount']
				unit = dv['value']['unit']
				dvv = [t, unit]
			elif dvt == "globecoordinate":				
				dvv = {'lat':dv['value']['latitude'], 'long':dv['value']['longitude'], 'alt':dv['value']['altitude']}
			else:
				print(f"{js['id']} has {prop} with unknown datatype {dvt}: {dv}")
				continue
			newvals.append(dvv)
		new[prop] = newvals
	return new

start = time.time()
todo = 93000000
x = 0 
done_x = 0
l = 1
while l:
	l = fh.readline()
	if not l:
		# Somehow got to the end without exiting loop
		break
	# skip properties:
	if l[:100].find(b'"type":"property",') > 0:
		print(f"skipping property: {l[:100]}")
		continue

	# Find id and check if already exists before processing JSON
	ididx = l.find(b'"id":"Q')
	endidx =l[ididx+7:ididx+100].find(b'"')
	what = l[ididx+6:ididx+7+endidx].decode('ascii')
	if what in out_db:
		done_x += 1
		if not done_x % 10000:
			print("Skipping past {done_x} {time.time() - start}")
		continue
	try:
		js = json.loads(l[:-2])
	except:
		# very last record won't have a ','
		if len(l) > 3:
			js = json.loads(l[:-1])		
		except:
			# last line is ']\n'
			break

	x += 1
	try:
		new = process_json(js)
	except:
		print(f"Failed to process {l}")
		continue

	out = json.dumps(new,separators=(',', ':'))
	what = new['id']
	out_db[what] = out
	if not x % 10000:
		t = time.time() - start
		xps = x/t
		ttls = todo / xps
		out_db.commit()
		print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
fh.close()

out_db['_p_meta'] = json.dumps(p_meta)
out_db.commit()
