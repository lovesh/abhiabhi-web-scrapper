import re
import difflib
import pymongo 
import datetime

color_list=['blue','ocean blue', 'brown', 'silver','dark silver','Metalic Silver','red','Ruby Red','black','Metalic Black','white', 'ploar white', 'stealth black','Sparkling Black','orange','golden','graphite','yellow','grey','gray','Metalic Gray','Charcoal Grey','slate grey','dark grey','aqua','aqua blue','pink','green','pearl','purple']

con = pymongo.Connection()
p_coll = con.new_final.products
brands_coll = con.new_final.brands
cats_coll = con.new_final.categories

site_preference=['flipkart','buythprice','saholic','homeshop18','infibeam','indiaplaza']

def group_by(iterable,key):
    result={}
    for item in iterable:
        if key in item:
            if item[key] in result:
                result[item[key]].append(item)
            else:
                result[item[key]] = [item,]
    return result

def have_key(iterable,key):
    result=[item for item in iterable if key in item]
    return result

def remove_junk(string,junk):
    junk = set(junk)
    result = string
    for item in junk:
        result = re.sub(item,' ',result,flags = re.I)
    return result.strip()
    
def remove_duplicate_words(string):
    result=[]
    [result.append(word) for word in string.split() if result.count(word)==0]
    result=' '.join(result)
    return result.strip()

def get_close_matches_to_length(word,possibilities, n = 3, cutoff = .8, length = 0):
    if length==0:
        length = len(word)
    new_possibilities={}
    for possibility in possibilities:
        new_possibilities[possibility[:length]] = possibility
    matches = difflib.get_close_matches(word[:length],new_possibilities.keys(),n,cutoff)
    result=[]
    for match in matches:
        result.append(new_possibilities[match])
    return result

def remove_colors(text,colors=[]):
    result = text.strip()
    colors_found=[]
    if len(colors)==0:
        colors = color_list
    colors = set(colors)
    for color in colors:
        m = re.subn("\W"+color+"(\W|$)",' ',result,flags = re.I)
        result = m[0].strip()
        if m[1] > 0:
            colors_found.append(color)
    result = re.sub('\(? ?and ?\)?','',result,flags = re.I)
    result = result.strip(')(, \n\t\r')
    return (result,colors_found)

def reduce_spaces(text):
    result=''
    words = text.split()
    for word in words:
        result = result + word.strip()+' '
    return result.strip()

def unordered_comparison(string1,string2):
    string1 = reduce_spaces(string1)
    string2 = reduce_spaces(string2)
    string1_parts = set(string1.split())
    string2_parts = set(string2.split())
    if string1_parts==string2_parts:
        return True
    return False

def prepareProductGroups(products):
    result={}
    grouped_products = group_by(products,'brand')
    return grouped_products

def is_in(item,list_of_sets):
    for offset,set in enumerate(list_of_sets):
        if set.__contains__(item):
            return offset + 1
    return False

def combine1(result):
    new_final=[]
    for r in result:
        r = set(r)
        flag = 0
        a,b = r
        p = is_in(a,new_final)
        if p:
            new_final[p-1].add(b)
            flag = 1
        p = is_in(b,new_final)
        if p:
            new_final[p-1].add(a)
            flag = 1
        if flag==0:
            new_final.append(r)

def combine2(result):
    list_of_sets = list(result)
    temp=[]
    for offset,item in enumerate(list_of_sets):
        s = set()
        s.update(item)
        for other_offset,other_item in enumerate(list_of_sets):
            if len(s.intersection(other_item))>0:
                s.update(other_item)
                list_of_sets.remove(other_item)
        temp.append(s)
        

    '''
    new_final=[]
    for offset,item in enumerate(temp):
        s = set()
        s.update(item)
        for other_offset,other_item in enumerate(temp[offset + 1:]):
            if len(s.intersection(other_item))>0:
                s.update(other_item)
                temp.remove(other_item)
        new_final.append(s)
    '''
    return temp
        

def combine(result):
    list_of_sets = list(result)
    count = 1
    while count>0:
	count = 0
	result=[]
	while len(list_of_sets)>0:
	    current = list_of_sets[0]
	    others = list_of_sets[1:]
	    list_of_sets=[]
	    for id_set in others:
		if len(current.intersection(id_set))==0:
		    list_of_sets.append(id_set)
		else:
		    count+=1
		    current = current.union(id_set)
	    result.append(current)
	list_of_sets = result
    return result
    
    
def getMatchingProductIdSets(product_type,compare):
    '''
    Returns a list of set of ids of products which are same 
    Takes the product type and a callback comparison function as parameters
    '''
    s_coll = con.abhiabhi['scraped_'+product_type+'s']
    result = set()
    docs = s_coll.find()
    products=[doc for doc in docs]
    print "%d products found in collection"%len(products)
    products = prepareProductGroups(products)
    print len(products)
    #sites = set(products.keys())
    
    for brand in products:
	print "comparing for brand %s"%brand
	products_of_brand = products[brand]
	print "products",len(products_of_brand)
	for product1 in products_of_brand:
	    for product2 in products_of_brand:
		if product1['_id'] != product2['_id']:
		    if compare(product1,product2):
			result.add(frozenset([product1['_id'],product2['_id']]))
    result = combine(result)
    return result

def getProductsFromIdSets(product_type, product_id_sets):
    s_coll = con.abhiabhi['scraped_'+product_type+'s']
    final_products = []
    for id_set in product_id_sets:
        final_product = {}
        products = [p for p in s_coll.find({'_id':{'$in':list(id_set)}})]
	stores = []
        final_product['specification'] = {}
	final_product['description'] = {}
	final_product['key_features'] = getKeyFeatures(product_type, products)
        lowest_price = 0
	if product_type == 'camera':
	    final_product['category'] = set()
        for product in products:
	    if 'name' not in final_product:
		final_product['name'] = product['name']
	    if 'brand' not in final_product:
		final_product['brand'] = product['brand']
	    if product_type == 'camera':
		final_product['category'].update(set(product['category']))
            store = {}
            store['name'] = product['site']
            store['url'] = product['url']
            store['datetime'] = product['last_modified_datetime']
            if 'availability' in product:
                store['availability'] = product['availability']
            else:
                store['availability'] = 0
	    if 'offer' in product:
                store['offer'] = product['offer']
            if 'price' in product:
                store['price'] = product['price']
                if lowest_price == 0:
                    lowest_price = store['price']
                    lowest_price_store = store['name']
		    lowest_price_url = store['url']
                else:
                    if store['price']<lowest_price:
                        lowest_price = store['price']
                        lowest_price_store = store['name']
			lowest_price_url = store['url']
            if 'shipping' in product:
		store['shipping'] = product['shipping']
	    if 'specification' in product and len(product['specification'])>0:
		final_product['specification'][product['site']] = product['specification']
            if 'description' in product:
		final_product['description'][product['site']] = product['description'].replace('\n\t\r','').strip()
            stores.append(store)
        final_product['stores'] = stores
	store_names = []
	for store in stores:
	    if store['name'] not in store_names:
		store_names.append(store['name'])
	final_product['store_count'] = len(store_names)
	lw={}
	
	if lowest_price == 0:
	    lw={'price':0}
	else:
	    lw={'store':lowest_price_store, 'price':lowest_price, 'url':lowest_price_url}
	    
	final_product['lowest_price']=lw
        final_product['scraped_product_ids'] = list(id_set)
        final_product['root_category'] = product_type
	final_product['categories'] = [product_type,]
        if product_type == 'camera':
	    final_product['categories'].append(list(final_product['category'])[0])
	    del final_product['category']
        final_products.append(final_product)

    return final_products

def firstTimeProductFilling(product_type, callback):
    
    p_coll.create_index([("_id", pymongo.ASCENDING), ("status",pymongo.ASCENDING)])
    p_coll.create_index([("root_category", pymongo.ASCENDING), ("status",pymongo.ASCENDING)])
    p_coll.create_index([("categories", pymongo.ASCENDING),("added_dateime", pymongo.DESCENDING), ("status",pymongo.ASCENDING)])
    p_coll.create_index([("categories", pymongo.ASCENDING),("last_updated_dateime", pymongo.DESCENDING), ("status",pymongo.ASCENDING)],sparse = True)
    p_coll.create_index([("added_dateime", pymongo.DESCENDING), ("status",pymongo.ASCENDING)])
    p_coll.create_index([("last_updated_dateime", pymongo.DESCENDING), ("status",pymongo.ASCENDING)],sparse = True)
    p_coll.create_index([("lowest_price.price", pymongo.DESCENDING), ("status",pymongo.ASCENDING)],sparse = True)
    p_coll.create_index([("lowest_price.price", pymongo.ASCENDING), ("status",pymongo.ASCENDING)],sparse = True)
    p_coll.create_index("scraped_product_ids")
    p_coll.create_index([("brand", pymongo.ASCENDING), ("status",pymongo.ASCENDING)])
    p_coll.create_index([("name", pymongo.ASCENDING), ("status",pymongo.ASCENDING)])
    
    s_coll = con.abhiabhi['scraped_'+product_type+'s']
    matched_product_id_sets = getMatchingProductIdSets(product_type, callback)
    #return matched_product_id_sets
    all_product_ids = set(d['_id'] for d in s_coll.find({},{'_id':1}))
    
    all_matched_ids = set(matched_product_id_sets)
    #for id_set in matched_product_id_sets:
    #    all_matched_ids.update(id_set)
    unmatched_product_id_sets = [set([product_id, ]) for product_id in (all_product_ids - all_matched_ids)]
    
    all_id_sets = []
    all_id_sets.extend(matched_product_id_sets)
    all_id_sets.extend(unmatched_product_id_sets)
    print len(all_id_sets)
    products = getProductsFromIdSets(product_type,all_id_sets)
    
    brands={}
    
    print "%d %ss found"%(len(products), product_type)
    
    lowest_price = 0  
    highest_price = 0
    for product in products:
	if 'price' in product['lowest_price']:
	    if lowest_price is not 0:
		if product['lowest_price']['price'] < lowest_price:
		    lowest_price = product['lowest_price']['price']
	    else:
		lowest_price = product['lowest_price']['price']
		
	if 'price' in product['lowest_price']:
	    if highest_price is not 0:
		if product['lowest_price']['price'] > highest_price:
		    highest_price = product['lowest_price']['price']
	    else:
		highest_price = product['lowest_price']['price']
	    
        price_history = product['lowest_price']   		#product['lowest_price'] is a dictionary of lowest price, corresponding store name and its url
        price_history['datetime'] = datetime.datetime.now()
        product['price_history'] = [price_history, ]
	product['added_datetime'] = datetime.datetime.now()
	product['status'] = 1
	while(True):
	    try:
		product_id = p_coll.insert(product,safe = True)
		break
	    except pymongo.errors.OperationFailure:
		print 'OperationFailure Exception raised while inserting product into catalog'
		pass
		
	'''
	The following lines are used for updating brands
	'''
	brand = product['brand']
	if brand in brands:
	    #brands[brand]['product_ids'].append(product_id)
	    brands[brand]['num_products'] += 1
	    if brands[brand]['lowest_price']>product['lowest_price']['price']:
		brands[brand]['lowest_price'] = product['lowest_price']['price']
		
	    if brands[brand]['highest_price']<product['lowest_price']['price']:
		brands[brand]['highest_price'] = product['lowest_price']['price']
	else:
	    brands[brand]={}
	    brands[brand]['num_products'] = 1
	    #brands[brand]['product_ids'] = [product_id,]
	    brands[brand]['lowest_price'] = product['lowest_price']['price']
	    brands[brand]['highest_price'] = product['lowest_price']['price']
    
    updateBrands(product_type,brands)
    update_categories(product_type,len(products),lowest_price,highest_price)
    
def nextTimeProductFilling(product_type, callback):
    s_coll = con.abhiabhi['scraped_' + product_type + 's']
    matched_product_id_sets = getMatchingProductIdSets(product_type, callback)
    all_product_ids = set(d['_id'] for d in s_coll.find({}, {'_id':True}))
    all_matched_ids = set(matched_product_id_sets)
    #for id_set in matched_product_id_sets:
     #   all_matched_ids.update(id_set)
    unmatched_product_id_sets=[set([product_id,]) for product_id in (all_product_ids - all_matched_ids)]
    
    all_product_id_sets=[]
    all_product_id_sets.extend(matched_product_id_sets)
    all_product_id_sets.extend(unmatched_product_id_sets)
    
    existing_products = [doc for doc in p_coll.find({'root_category':product_type, 'status':1, 'upcoming':{'$exists':False}}, {'scraped_product_ids':True,'_id':True,'lowest_price':True})]
    new_product_id_sets = []
    
    update_count = 0
    insert_count = 0
    
    for id_set in all_product_id_sets:
	
	for existing_product in existing_products:
	    
	    scraped_product_ids = set(existing_product['scraped_product_ids'])
	    
	    if len(scraped_product_ids.intersection(id_set)) > 0:
		
		new_ids = list(id_set - scraped_product_ids)		#ids of newly added products simalar to existing product
		docs = s_coll.find({'_id':{'$in':list(id_set)}},{'shipping':True, 'price':True, 'availability':True, 'site':True, 'url':True})
		stores = []
		if 'price' in existing_product['lowest_price']:
		    lowest_price = existing_product['lowest_price']['price']
		else:
		    lowest_price = 0
		if 'store' in existing_product['lowest_price']:
		    lowest_price_store = existing_product['lowest_price']['store']
		if 'url' in existing_product['lowest_price']:
		    lowest_price_url = existing_product['lowest_price']['url']
		for doc in docs:
		    store={}			#this dictionary is used for filling the store information
		    store['name'] = doc['site']
		    store['url'] = doc['url']
		    if 'offer' in doc:
			store['offer'] = doc['offer']
		    if 'price' in doc:
			store['price'] = doc['price']
			if lowest_price is not 0:
			    if store['price'] < lowest_price:
				lowest_price = doc['price']
				lowest_price_store = doc['site']
				lowest_price_url = doc['url']
			else:
			    lowest_price = doc['price']
			    lowest_price_store = doc['site']
			    lowest_price_url = doc['url']
		    if 'shipping' in doc:
			store['shipping'] = doc['shipping']
		    if 'availability' in doc:
			store['availability'] = doc['availability']
		    stores.append(store)
		p_coll.update({'_id':existing_product['_id']},{
		    '$set':{
			'lowest_price':{
			    'price':lowest_price,
			    'store':lowest_price_store,
			    'url':lowest_price_url
			    },
			'last_updated_datetime':datetime.datetime.now(),
			'stores':stores
		    },
		    '$addToSet':{
			'scraped_product_ids':{'$each':new_ids}
		    },
		    '$push':{
			'price_history':{
			    'price':lowest_price,
			    'store':lowest_price_store,
			    'datetime':datetime.datetime.now()
			    }
		    }
		},safe = True)
		update_count += 1
		break
	else:
	   new_product_id_sets.append(id_set)
	    
    new_products = getProductsFromIdSets(product_type, new_product_id_sets)
    
    brands={}
    
    lowest_price = 0  
    highest_price = 0
    for product in new_products:
	if 'price' in product['lowest_price']:
	    if lowest_price is not 0:
		if product['lowest_price']['price'] < lowest_price:
		    lowest_price = product['lowest_price']['price']
	    else:
		lowest_price = product['lowest_price']['price']
		
	if 'price' in product['lowest_price']:
	    if highest_price is not 0:
		if product['lowest_price']['price'] > highest_price:
		    highest_price = product['lowest_price']['price']
	    else:
		highest_price = product['lowest_price']['price']
	    
        price_history = product['lowest_price']   		#product['lowest_price'] is a dictionary of lowest price, corresponding store name and its url
        price_history['datetime'] = datetime.datetime.now()
        product['price_history'] = [price_history, ]
	product['added_datetime'] = datetime.datetime.now()
	product['status'] = 1
	while(True):
	    try:
		product_id = p_coll.insert(product,safe = True)
		insert_count += 1
		break
	    except pymongo.errors.OperationFailure:
		print 'OperationFailure Exception raised while inserting product into catalog'
		pass
	
	'''
	The following lines are used for updating brands
	'''
	brand = product['brand']
	if brand in brands:
	    #brands[brand]['product_ids'].append(product_id)
	    brands[brand]['num_products'] += 1
	     
	    if brands[brand]['lowest_price'] > product['lowest_price']['price']:
		brands[brand]['lowest_price'] = product['lowest_price']['price']
		
	    if brands[brand]['highest_price'] < product['lowest_price']['price']:
		brands[brand]['highest_price'] = product['lowest_price']['price']
	else:
	    brands[brand]={}
	    brands[brand]['num_products'] = 1
	    #brands[brand]['product_ids'] = [product_id,]
	    brands[brand]['lowest_price'] = product['lowest_price']['price']
	    brands[brand]['highest_price'] = product['lowest_price']['price']
    
    print "%d new products inserted"%insert_count
    print "%d existing products updated"%update_count
    
    updateBrands(product_type,brands)
    update_categories(product_type,len(new_products),lowest_price,highest_price)


def updateBrands(product_type,brands):
    for brand in brands:
	num_products = brands[brand]['num_products']
	doc = brands_coll.find_one({'name':brand})
	if doc:
	    cats = doc['categories']
	    if product_type in cats:
		while(True):
		    try:
			if doc['categories'][product_type]['lowest_price'] < brands[brand]['lowest_price']:
			    brands[brand]['lowest_price'] = doc['categories'][product_type]['lowest_price']
			    
			if doc['categories'][product_type]['highest_price'] > brands[brand]['highest_price']:
			    brands[brand]['highest_price'] = doc['categories'][product_type]['highest_price']
			    
			brands_coll.update({'_id':doc['_id']},{
			'$inc':{
			    'num_products':num_products,
			    'categories.'+product_type+'.num_products':num_products
			    },
			#'$addToSet':{
			#    'categories.'+product_type+'.product_ids':{
			#	'$each':brands[brand]['product_ids']
			#	}
			 #   },
			'$set':{
			    'categories.'+product_type+'.lowest_price':brands[brand]['lowest_price'],
			    'categories.'+product_type+'.highest_price':brands[brand]['highest_price']
			    }
			},safe = True)
			break
		    except pymongo.errors.OperationFailure:
			print 'OperationFailure Exception raised while updating brand into catalog'
			pass
	    else:
		while(True):
		    try:
			brands_coll.update({'_id':doc['_id']},{
			'$inc':{
			    'num_products':num_products
			},
			'$set':{
			    'categories.'+product_type:{
				'num_products':num_products,
				#'products':brands[brand]['product_ids'],
				'lowest_price':brands[brand]['lowest_price'],
				'highest_price':brands[brand]['highest_price']
				}
			    }
			},safe = True)
			break
		    except pymongo.errors.OperationFailure:
			print 'OperationFailure Exception raised while updating brand into catalog'
			pass
			
	else:
	    brand_doc={}
	    brand_doc['name'] = brand
	    brand_doc['num_products'] = num_products
	    #brand_doc['lowest_price'] = lowest_price
	    #brand_doc['highest_price'] = highest_price
	    
	    brand_doc['categories']={product_type:{'num_products':num_products,'lowest_price':brands[brand]['lowest_price'],'highest_price':brands[brand]['highest_price']}}
	    while(True):
		try:
		    brands_coll.insert(brand_doc,safe = True)
		    break
		except pymongo.errors.OperationFailure:
		    print 'OperationFailure Exception raised while inserting brand into catalog'
		    pass    
	    

def update_categories(cat_name,num_products,lowest_price,highest_price):
    '''
    This function is used for updating the root categories only
    '''
    doc = cats_coll.find_one({'name':cat_name})
    if doc:
	upd={}
	
	if lowest_price < doc['lowest_price']:
	    upd['lowest_price'] = lowest_price
	    
	if highest_price > doc['highest_price']:
	    upd['highest_price'] = highest_price
	    
	cats_coll.update({'_id':doc['_id']},{'$inc':{'num_products':num_products},'$set':upd},safe = True)
    
    else:
	doc={}
	doc['is_root'] = True
	doc['name'] = cat_name
	doc['lowest_price'] = lowest_price
	doc['highest_price'] = highest_price
	doc['num_products'] = num_products
	doc['status'] = 1
	cats_coll.insert(doc,safe = True)
	
def getKeyFeatures(product_type,products):
    kf={}
    
	
    if product_type == 'harddisk':
	for product in products:
	    if 'capacity' not in kf and 'capacity' in product['specification']:
		kf['capacity'] = product['specification']['capacity']
		
	    if 'interface' not in kf and 'interface' in product['specification']:
		kf['interface'] = product['specification']['interface']
		
	    if 'size' not in kf and 'size' in product['specification']:
		kf['size'] = product['specification']['size']
		
	    if 'rpm' not in kf and 'rpm' in product['specification']:
		kf['rpm'] = product['specification']['rpm']
		
	    if set(['capacity','interface','rpm','size']).issubset(set(kf.keys())):
		return kf
			
	return kf
	
	
    if product_type == 'pendrive':
	for product in products:
	    if 'capacity' not in kf and 'capacity' in product['specification']:
		kf['capacity'] = product['specification']['capacity']
		    
	    if 'interface' not in kf and 'interface' in product['specification']:
		kf['interface'] = product['specification']['interface']
	    
	    if set(['capacity','interface']).issubset(set(kf.keys())):
		return kf
		
	return kf

    
    if product_type == 'mobile':
	for product in products:
	    if 'os' not in kf and 'os' in product['specification']:
		kf['os'] = product['specification']['os']
		return kf
    
    
    if product_type == 'laptop':
	for product in products:
	    if 'ram' not in kf:
		if 'memory' in product['specification']:
		    if 'ram' in product['specification']['memory']:
			kf['ram'] = product['specification']['memory']['ram']
		else:
		    if 'ram' in product['specification']:
			kf['ram'] = product['specification']['ram']
			
	    if 'storage capacity' not in kf:
		if 'storage' in product['specification']:
		    if 'capacity' in product['specification']['storage']:
			kf['storage capacity'] = product['specification']['storage']['capacity']
		else:
		    if 'storage capacity' in product['specification']:
			kf['storage capacity'] = product['specification']['storage capacity']
			
	    if 'processor' not in kf:
		if 'processor' in product['specification']:
		    if type(product['specification']['processor']) == dict:
			if 'processor' in product['specification']['processor']:
			    kf['processor'] = product['specification']['processor']['processor']
			else:
			    if 'brand' in product['specification']['processor']:
				kf['processor'] = product['specification']['processor']['brand']
			    else:
				if 'clock speed' in product['specification']['processor']:
				    kf['processor'] = product['specification']['processor']['clock speed']
		    else:
			kf['processor'] = product['specification']['processor']
			
	    if 'os' not in kf:
		if 'software' in product['specification']:
		    if 'os' in product['specification']['software']:
			kf['os'] = product['specification']['software']['os']
		else:
		    if 'os' in product['specification']:
			kf['os'] = product['specification']['os']
			
	    if 'screen size' not in kf:
		if 'display' in product['specification']:
		    if 'size' in product['specification']['display']:
			kf['screen size'] = product['specification']['display']['size']
		else:
		    if 'screen size' in product['specification']:
			kf['screen size'] = product['specification']['screen size']
			
	    if 'processor clock speed' not in kf:
		if 'processor' in product['specification']:
		    if 'clock speed' in product['specification']['processor']:
			kf['processor clock speed'] = product['specification']['processor']['clock speed']
		else:
		    if 'clock speed' in product['specification']:
			kf['processor clock speed'] = product['specification']['clock speed']
	    
	    if set(['storage capacity','ram','os','screen size','processor clock speed']).issubset(set(kf.keys())):
		return kf
		    
	return kf
    
    
    if product_type == 'tablet':
	for product in products:
	    if 'ram' not in kf:
		if 'ram' in product['specification']:
		    kf['ram'] = product['specification']['ram']
		    
	    if 'storage' not in kf:
		if 'storage' in product['specification']:
		    kf['storage'] = product['specification']['storage']
		    
	    if 'os' not in kf:
		if 'os' in product['specification']:
		    kf['os'] = product['specification']['os']
	    
	    if set(['storage','ram','os']).issubset(set(kf.keys())):
		return kf
		
	return kf


    if product_type == 'camera':
	return kf
    
