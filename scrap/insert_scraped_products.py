import pymongo

scrapDB='abhiabhi'
log_coll='scrap_log'
catalog='final'

def insertScrapedProductsIntoDB(product_type,products,log=True):
    con=pymongo.Connection('localhost',27017)
    db=con[scrapDB]
    product_coll=db['scraped_'+product_type]
    product_coll.create_index('url',unique=True)s
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    for product in products:
        try:
            product_coll.insert(product,safe=True)
            inserted_count+=1
            inserted_urls.append(product['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' 'availability'in product:
                upd['availability']=product['availability']
            if 'price' in product:
                upd['price']=product['price']
            if 'shipping' in product:
                upd['shipping']=product['shipping']
	    if 'offer' in product:
		upd['offer']=product['offer']
            product_coll.update({'url':product['url']},{'$push':{'product_history':product['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(product['url'])
    
    if log:
        scrap_log=db[log_coll]
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':product_type,'products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
