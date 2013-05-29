import pymongo
import sys
import datetime

con=pymongo.Connection()

product_type = sys.argv[1]

s_coll=con.abhiabhi['scraped_'+product_type+'s']
p_coll=con.new_final.products

counter = 0

docs = p_coll.find({'root_category':product_type})
for doc in docs:
    stores = []
    lowest_price = 0
    if 'lowest_price' in doc:
	if 'price' in doc['lowest_price']:
	    lowest_price = doc['lowest_price']['price']
	if 'store' in doc['lowest_price']:
	    lowest_price_store = doc['lowest_price']['store']
	if 'url' in doc['lowest_price']:
	    lowest_price_url = doc['lowest_price']['url']
    
    if 'scraped_product_ids' in doc:
	scraped_product_ids = doc['scraped_product_ids']
	scraped_products = [p for p in s_coll.find({'_id':{'$in':scraped_product_ids}})]
	for scraped_product in scraped_products:
	    store={}			#this dictionary is used for filling the store information
	    store['name'] = scraped_product['site']
	    store['url'] = scraped_product['url']
	    if 'offer' in scraped_product:
		store['offer'] = scraped_product['offer']
	    if 'price' in scraped_product:
		store['price'] = scraped_product['price']
		if lowest_price is not 0:
		    if store['price'] < lowest_price:
			lowest_price = scraped_product['price']
			lowest_price_store = scraped_product['site']
			lowest_price_url = scraped_product['url']
		else:
		    lowest_price = scraped_product['price']
		    lowest_price_store = scraped_product['site']
		    lowest_price_url = scraped_product['url']
	    if 'shipping' in scraped_product:
		store['shipping'] = scraped_product['shipping']
	    if 'availability' in scraped_product:
		store['availability'] = scraped_product['availability']
	    stores.append(store)
	p_coll.update({'_id':doc['_id']},{
	    '$set':{
		'lowest_price':{
		    'price':lowest_price,
		    'store':lowest_price_store,
		    'url':lowest_price_url
		    },
		'last_updated_datetime':datetime.datetime.now(),
		'stores':stores
	    },
	    '$push':{
		'price_history':{
		    'price':lowest_price,
		    'store':lowest_price_store,
		    'datetime':datetime.datetime.now()
		    }
	    }
	},safe = True)
	print "product ",doc['_id']," updated"
	counter += 1

print "%d products updated"%counter
