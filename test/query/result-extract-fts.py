import json
import sys


json_input = sys.argv[1]
with open(json_input) as json_file:
    data = json.load(json_file)

for query in data:
    if query['Query']['EndPosition'] < query['Query']['StartPosition']:
        print("FT   CDS             complement(%d..%d)"%(query['Query']['EndPosition'], query['Query']['StartPosition']))
    else:
        print("FT   CDS             %d..%d"%(query['Query']['StartPosition'], query['Query']['EndPosition']))
