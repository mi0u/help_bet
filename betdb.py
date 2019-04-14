from pymongo import MongoClient
from pymongo import errors
from bson import json_util
from prettytable import PrettyTable
from prettytable import DEFAULT, PLAIN_COLUMNS, MSWORD_FRIENDLY
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import getpass

uri = None
client  = None
db = None
books = ['stoiximan', 'championsbet', 'bet365', 'betrebels', 'pamestoixima']

#Connect to db
def connect(host):
	global uri
	global client
	global db
	if db: return
	user = getpass.getpass('User:')
	passw = getpass.getpass('Pass:')
	uri = "mongodb://" + user + ":" + passw + "@" + host + ":17017/?authSource=bet_db&authMechanism=SCRAM-SHA-1"
	try:
		client  = MongoClient(uri, serverSelectionTimeoutMS=10000)
		client.server_info()
		db = client.get_database('bet_db')
		return True
	except errors.ServerSelectionTimeoutError as err:
		print(err)
		client = None
		db = None
		return False

# Drop all db collections
def emptydb():
	collections = db.list_collection_names()
	for c in collections:
		if len(c.split('.')) ==1:
			print(c)
			db.drop_collection(c)

# Run spiders
def runSpiders():
	db['control.flag'].update({'run': False}, {'$set': {'run': True}})

# Exit spiders
# if called then the spiders should be started again manualy by calling caller.py
def exitSpiders():
	db['control.flag'].update({'exit': False}, {'$set': {'exit': True}})

def _print(g, s=DEFAULT):
	t = PrettyTable(['match','1', 'X', '2', 'vig'])
	t.add_row([g['match'], g['o1']['bookmaker'], g['oX']['bookmaker'], g['o2']['bookmaker'], ''])
	t.add_row([g['time'], g['o1']['odd'], g['oX']['odd'], g['o2']['odd'], g['vig']])
	t.set_style(s)
	#t.border = True
	print(t)
	#print()

# Vig calculation
def ganiota1x2(a,b,c):
	return 100*(1-1/(1.0/a+1.0/b+1.0/c))

# Vig calculation
def vig(a0, *args):
	rest = 0.0
	for a in args:
		rest = rest + 1.0/a
	return 100*(1-1/(1.0/a0+rest))

# Internal
def _condition(viglimit,vig, b1,bx,b2, booksaccepted =books):
	con = con1 = conx = con2 = False
	for b in booksaccepted:
		con1 = con1 or (b1 == b)
		conx = conx or (bx == b)
		con2 = con2 or (b2 == b)
	con = con1 and conx and con2
	con = con and (viglimit>vig)
	return con

# Yield all matches from matches collection
def getAllMatches():
	cursor=db.matches.find({})
	for d in cursor:
		yield d
# Get match name
# match = match name
# returns match, date, league, time
def getMatch(match):
	matches = []
	date = {}
	for d in getAllMatches():
		for m in d['matches']:
			matches.append(m)
			date[m[0]] = [d['date'], m[1], m[2]]
	result = process.extract(match, (item[0] for item in matches), limit=1, scorer=fuzz.token_sort_ratio)[0][0]
	return result, date[result][0], date[result][1], date[result][2]

# Yield all matches from one collection 
def getCollMatches(col):
	cursor=db[col].find({})
	for d in cursor:
		yield d

# Yield all matches from all collections for specific day
def getDayMatchesFull(day):
	collections = db.list_collection_names()
	collections.remove('matches')
	for c in collections:
		cursor = db[c].find({'date': day})
		for m in cursor:
			yield m

def getDayMatches(day):
	cursor=db.matches.find({'date': day})
	for d in cursor:
		yield d['matches']

# Yield last retrieve of all matches from all collections for specific day
def getDayMatchesLast(day):
	for l in getDayMatches(day):
		td = {}
		for m in l:
			match = m[0]
			league = m[1]			
			td[match] = []
			counter = 0
			for book in books:
				pipeline = [ { '$match': { 'date': day,  } },
	    					{ '$group': { '_id': { 'DATE': '$date', 'TIME': '$matches.'+match+'.time', 'BOOK': book, 'MATCH': '$matches.'+match+'.bookmakers' } } },
	    					{ '$project': {'_id':0, 'date': '$_id.DATE', 'time': '$_id.TIME', 'book': '$_id.BOOK', 'odds': { '$arrayElemAt': [ '$_id.MATCH.'+book, -1 ] } } }
					   		]
				r = db[league].aggregate(pipeline)
				for e in r:
					if (e['odds']):
						td[match].append(e['odds'])
						td[match][counter]['bookmaker'] = e['book']
						td[match][counter]['time'] = e['time']
						counter = counter + 1
		yield td


# Yield last retrieve of all matches from all collections for specific day
def _getDayMatchesLast(day):
	for l in getDayMatchesFull(day):
		td = {}
		for matchname in l['matches']:
			td[matchname] = []
			i=0
			for o in list(l['matches'][matchname]['bookmakers'].keys()):
				td[matchname].append(l['matches'][matchname]['bookmakers'][o][-1])
				td[matchname][i]['bookmaker'] = o
				td[matchname][i]['time'] = l['matches'][matchname]['time']
				i=i+1
		yield td

# Yield dictionary with last retrieved matches combined odds
# from all collections for specific day with vig
def getDayVigsDict(day):
	for g in getDayMatchesLast(day):
		for matchname in g:
			for one in g[matchname]:
				for x in g[matchname]:
					for two in g[matchname]:
						vig = ganiota1x2(float(one['o1']), float(x['oX']), float(two['o2']))
						yield {'match': matchname, 'vig': vig, 'time': x['time'],
								'o1':{'bookmaker': one['bookmaker'], 'odd': one['o1']},
								'oX':{'bookmaker': x['bookmaker'], 'odd': x['oX']},
								'o2':{'bookmaker': two['bookmaker'], 'odd': two['o2']}}

# Prints combined odds for all matches for specific day
# day = day to search
# v = vig limit
# booksaccepted = array with the books from which to retrieve the odds
# doubles = if True then results with odds from the same bookmaker are also returned
def printDayVigs(day=None, v=20, booksaccepted =books, doubles = False):
	if not db: 
		if not connect(): return
	for g in getDayVigsDict(day):
		if _condition(v, g['vig'], g['o1']['bookmaker'], g['oX']['bookmaker'], g['o2']['bookmaker'], booksaccepted):
			if not ((not doubles) and 
			(g['o1']['bookmaker'] == g['oX']['bookmaker'] or 
			g['o2']['bookmaker'] == g['oX']['bookmaker'] or 
			g['o1']['bookmaker'] == g['o2']['bookmaker'])):
				_print(g)

# Prints matches with X odd from championsbet
# day = day to search
# v = vig limit
# booksaccepted = array with the books from which to retrieve the odds
# doubles = if True then results with odds from the same bookmaker are also returned
def printChampionsX(day=None, v=20, booksaccepted=books, doubles=False):
	if not db: 
		if not connect(): return
	for g in getDayVigsDict(day):
		if g['vig'] < v and g['oX']['bookmaker'] == 'championsbet':
			if _condition(v, g['vig'], g['o1']['bookmaker'], g['oX']['bookmaker'], g['o2']['bookmaker'], booksaccepted):
				if not ((not doubles) and 
				(g['o1']['bookmaker'] == g['oX']['bookmaker'] or 
				g['o2']['bookmaker'] == g['oX']['bookmaker'] or 
				g['o1']['bookmaker'] == g['o2']['bookmaker'])):
					_print(g)

# Prints combined odds for one matche
# match = match to search (fuzzy)
# v = vig limit
# booksaccepted = array with the books from which to retrieve the odds
# doubles = if True then results with odds from the same bookmaker are also returned
def printMatchVigs(match, v=20, booksaccepted =books, doubles = False):
	if not db: 
		if not connect(): return
	m,day,league,time = getMatch(match)
	print('Date: ',day, 'Match:', m, 'League:', league, 'Time', time)
	for g in getDayVigsDict(day):
		if _condition(v, g['vig'], g['o1']['bookmaker'], g['oX']['bookmaker'], g['o2']['bookmaker'], booksaccepted):
			if not ((not doubles) and 
			(g['o1']['bookmaker'] == g['oX']['bookmaker'] or 
			g['o2']['bookmaker'] == g['oX']['bookmaker'] or 
			g['o1']['bookmaker'] == g['o2']['bookmaker'])):
				if g['match'] == m:
					_print(g)

# Prints all bookmakers odds for one match
# match = match to search (fuzzy)
def printMatchOdds(match):
	if not db: 
		if not connect(): return
	m, day, league, time = getMatch(match)
	print('Date: ',day, 'Time: ', time, 'Match:', m, 'League:', league)
	odds = db[league].find_one({'date': day})['matches'][m]['bookmakers']
	t = PrettyTable(['bookmaker','1', 'X', '2', 'over', 'under', 'vig', 'retrieved'])
	for o in odds.keys():
		t.add_row([o, odds[o][-1]['o1'], odds[o][-1]['oX'], odds[o][-1]['o2'], odds[o][-1]['over'], odds[o][-1]['under'], odds[o][-1]['ganiota1x2'], odds[o][-1]['retrieved']])
	#t.border = True
	print(t)

# Prints all matches of specific day
# day = date
def printDayMatches(day):
	if not db: 
		if not connect(): return
	print('Date: ',day)
	t = PrettyTable(['League', 'Match', 'Time'])
	for m in db['matches'].find_one({'date': day})['matches']:
		t.add_row([m[1], m[0], m[2]])
	print(t)

# Prints all matches of specific day
# day = date
def printDayMatchesOdds(day):
	if not db: 
		if not connect(): return
	print('Date: ',day)
	for m in db['matches'].find_one({'date': day})['matches']:
		printMatchOdds(m[0])