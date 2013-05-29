import pymongo
from bson.code import Code
from bson.son import SON
import datetime

DBName='abhiabhi'
con=pymongo.Connection()
db=con[DBName]
db.authenticate('root','hpalpha1911')
s_coll=db.scraped_books
#prod=pymongo.Connection()[DBName].products

def mr():
    map=Code("function () {"
	    "emit(this.isbn13, {id: this._id});"
	    "}")

    reduce=Code("function(key,values) {"
	    "var result={'id':[]};"
	    "values.forEach(function(value){ "
	    "result.id=result.id.concat(value.id);"
	    "});"
	    "return result;"
	    "}")

    result = s_coll.map_reduce(map, reduce,out=SON([("replace", "results"), ("db", "inter")]), full_response=True)


def prepareFinal():
    i_coll=con.inter.results
    p_coll=con.final.products

    site_preference=['flipkart','bookadda','homeshop18','infibeam','indiaplaza']
    
    docs=i_coll.find({},timeout=False)
    
    total=0
    for doc in docs:
	book={}
	isbn13=doc['_id']
	scraped_books=[sb for sb in s_coll.find({'isbn13':isbn13})]
	stores=[]
	lowest_price=0
	description=""
	book['scraped_product_ids']=[]
	book['key_features']={}
	book['key_features']['isbn-13']=isbn13
	for scraped_book in scraped_books:
	    if 'name' not in book and 'name' in scraped_book:
		book['name'] = scraped_book['name']
	    book['scraped_product_ids'].append(scraped_book['_id'])
	    store={}
	    store['name']=scraped_book['site']
	    if 'url' in scraped_book:
		store['url']=scraped_book['url']
	    store['datetime']=scraped_book['last_modified_datetime']
	    if 'availability' in scraped_book:
		store['availability']=scraped_book['availability']
	    else:
		store['availability']=0
	    if 'price' in scraped_book:
		store['price']=scraped_book['price']
		if lowest_price==0:
		    lowest_price=store['price']
		    lowest_price_store=store['name']
		    lowest_price_url=store['url']
		else:
		    if store['price']<lowest_price:
			lowest_price=store['price']
			lowest_price_store=store['name']
			lowest_price_url=store['url']
	    if 'shipping' in scraped_book:
	       store['shipping']=scraped_book['shipping']
	    stores.append(store)
	
	book['description']={}
	
	for site in site_preference:
	    for scraped_book in scraped_books:
		if scraped_book['site']==site:
		    if 'description' in scraped_book:
			    book['description'][site]=scraped_book['description']
		    if 'isbn-10' not in book['key_features']:
			if 'isbn' in scraped_book:
			    book['key_features']['isbn-10']=scraped_book['isbn']
		    if 'publication date' not in book['key_features']:
			if 'pubdate' in scraped_book:
			    book['key_features']['publication date']=scraped_book['pubdate']
		    if 'publisher' not in book['key_features']:
			if 'publisher' in scraped_book:
			    book['key_features']['publisher']=scraped_book['publisher']
		    if 'language' not in book['key_features']:
			if 'language' in scraped_book:
			    book['key_features']['language']=scraped_book['language']
		    if 'number of pages' not in book['key_features']:
			if 'num_pages' in scraped_book:
			    book['key_features']['number of pages']=scraped_book['num_pages']
		    if 'author' not in book['key_features']:
			if 'author' in scraped_book:
			    book['key_features']['author']=scraped_book['author']
		    if 'format' not in book['key_features']:
			if 'format' in scraped_book:
			    book['key_features']['format']=scraped_book['format']

	
	book['stores']=stores
	book['root_category']='book'
	book['status']=1
	book['added_datetime']=datetime.datetime.now()
	book['lowest_price']={'store':lowest_price_store,'price':lowest_price,'url':lowest_price_url}
	price_history=book['lowest_price']
	price_history['datetime']=datetime.datetime.now()
	book['price_history']=price_history
	p_coll.insert(book,safe=True)
	total+=1
	if total % 500 == 0:
	    print "Total books inserted %d"%total

def go():
    #mr()
    prepareFinal()

if __name__ == '__main__':
    go()  







