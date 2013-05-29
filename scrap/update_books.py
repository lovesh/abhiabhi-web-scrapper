import pymongo
from requests import async
import sys
import datetime
import time
import requests
from fk_books_scrap_directory import parseBookPage as fk_parseBookPage
from infibeam_books_scrap1 import parseBookPage as ib_parseBookPage
from hs18_books_scrap1 import parseBookPage as hs18_parseBookPage
from booksadda_scrap import parseBookPage as bk_parseBookPage
from indiaplaza_books import parseBookPage as ip_parseBookPage

ago_time = time.time() - 7*24*60*60
ago_time = datetime.datetime.fromtimestamp(ago_time)

site=sys.argv[1]
if len(sys.argv) > 2:
    proxy = {'http':sys.argv[2]}
else:
    proxy = None
    
DBName='abhiabhi'
con=pymongo.Connection()
coll=con[DBName]['scraped_books']
docs=coll.find({'site':site, 'last_modified_datetime':{'$lte':ago_time}}, timeout = False)

if len(sys.argv) > 2:
    proxy = {'http':sys.argv[2]}
else:
    proxy = None

for doc in docs:
    if 'url' in doc:
	if site=='flipkart':
	    book=fk_parseBookPage(url=doc['url'])
	if site=='infibeam':
	    book=ib_parseBookPage(url=doc['url'])
	if site=='homeshop18':
	    html = requests.get(doc['url']).content
	    if len(html) < 2000:
		continue
	    book=hs18_parseBookPage(string = html)
	if site=='bookadda':
	    book=bk_parseBookPage(url=doc['url'])
	if site=='indiaplaza':
            print doc['url']
	    if proxy:
		html = requests.get(doc['url'], proxies=proxy).content
		book=ip_parseBookPage(string=html)
	    else:
		book=ip_parseBookPage(url=doc['url'])
	    
	if book:
	    upd={'last_modified_datetime':datetime.datetime.now()}
	    if 'availability' in book:
		upd['availability']=book['availability']
	    if 'price' in book:
		upd['price']=book['price']
	    if 'shipping' in book:
		upd['shipping']=book['shipping']
	    if 'offer' in book:
		upd['offer']=book['offer']
	    else:
		upd['offer']=''
	    coll.update({'_id':doc['_id']},{'$push':{'product_history':book['product_history'][0]},'$set':upd})
	    
	    

