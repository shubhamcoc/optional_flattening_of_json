import json
from influxdb import InfluxDBClient

def flattening(nested, prefix, ignore_list):
	field = {}

	flatten(True, nested, field, prefix, ignore_list)

	return field

def flatten(top, nested, flatdict, prefix, ignore_list):
	def assign(newKey, data, toignore):
		if toignore:
			if isinstance(data, (dict, list, tuple,)):
				json_data = json.dumps(data)
				flatdict[newKey] = json_data
			else:
				flatdict[newKey] = data
		else:
			if isinstance(data, (dict, list, tuple,)):
				flatten(False, data, flatdict, newKey, ignore_list)
			else:
				flatdict[newKey] = data

	if isinstance(nested, dict):
		for key, value in nested.items():
			ok = match_key(ignore_list, key)
			if ok and prefix == "":
				assign(key, value, True)
			elif ok and prefix != "":
				newKey = create_key(top, prefix, key)
				assign(newKey, value, True)
			else:
				newKey = create_key(top, prefix, key)
				assign(newKey, value, False)

	elif isinstance(nested, (list, tuple,)):
		for index, value in enumerate(nested):
			if isinstance(value, dict):
				for key1, value1 in value.items():
					ok = match_key(ignore_list, key1)
					if ok:
						subkey = str(index) + "." + key1
						newkey = create_key(top, prefix, subkey)
						assign(newkey, value1, True)
					else:
						newkey = create_key(top, prefix, str(index))
						assign(newkey, value, False)
						
			else:
				newkey = create_key(top, prefix, str(index))
				assign(newkey, value, False)
				
				
	else:
		return ("Not a Valid input")

def create_key(top, prefix, subkey):
	key = prefix
	if top:
		key += subkey
	else:
		key += "." + subkey

	return key				


def match_key(ignorelist, value):
	for element in ignorelist:
		if element == value:
			return True 
	
	return False

def insert_data(msg, ignorelist):
	msg = json.loads(msg)

	field = flattening(msg, "", ignorelist)
	print(field)

	client = InfluxDBClient(database="python_influxdemo")
	client.create_database("python_influxdb_demo")

	points = [{
		"measurement": 'demo',
		"tags": {},
		"fields": field
	}]
	client.write_points(points)
	

data = { "intdata": [10,24,43,56,45,78],
         "floatdata": [56.67, 45.68, 78.12],
         "nested_data": {
               "key1": "string_data",
               "key2": [45, 56],
               "key3": [60.8, 45.78]
         }}
data = json.dumps(data)
ignore = ["floatdata", "key2"]
insert_data(data, ignore)
