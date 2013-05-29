import pymongo
import gridfs
from requests import async
import sys

site = sys.argv[1]
root_category = sys.argv[2]

def downloadImages(data):
    img_urls = [d for d in data]
    images = []
    requests = [async.get(u) for u in img_urls] 
    responses = async.map(requests,size = 30)
    for response in responses:
        if response.ok:
            images.append((response.status_code,response.content,data[response.request.url]))
    return images

def getProductData(doc):
    data={}
    if type(doc['img_url']['0']) != list:
	data['url']=doc['img_url']['0']
    else:
	return False
    if 'url' in doc:
	data['page_url']=doc['url']
    data['site']=doc['site']
    data['type']=root_category
    data['product_id']=[doc['_id'], ]	
    if root_category=='book':
	if 'isbn' in doc:
	    data['isbn']=doc['isbn']
	if 'isbn13' in doc:
	    data['isbn13']=doc['isbn13']
    return data

def go():
    con = pymongo.Connection()
    db = con.abhiabhi
    gfs = gridfs.GridFS(db,collection='images')
    coll = db['scraped_'+root_category+'s']
    image_coll = db.images.files
    counter = 0
    total = 0
    dup_count = 0
    docs = coll.find({'img_url.0':{'$exists':True},'site':site},timeout = False)
    data={}
    for doc in docs:
	temp = {}
	temp = getProductData(doc)
	if temp == False:
	    continue
	data[temp['url']]=temp
	counter+=1
	if counter==500:
	    images = downloadImages(data)
	    for image in images:
		try:
		    gfs.put(image[1],metadata = image[2])
		    total+=1
		    print "image with url %s inserted count is %d"%(image[2]['url'],total)
		except gridfs.errors.FileExists:
		    dup_count+=1
		    print "Image exists %d"%dup_count
		    pass
		coll.update({'_id':image[2]['product_id']},{'$set':{'img_url'+str(image[0]):image[2]['url']},'$unset':{'img_url.0':1}})
	    counter = 0
	    data={}
	    print '500 processed total is %d'%total
	    
    if counter != 0:
	images = downloadImages(data)
	for image in images:
	    try:
		gfs.put(image[1],metadata = image[2])
		total+=1
		print "image with url %s inserted count is %d"%(image[2]['url'],total)
	    except gridfs.errors.FileExists:
		image_coll.update({'metadata.url':image[2]['url']},{'$addToSet':{'metadata.product_id':image[2]['product_id'][0]}}, safe = True)
		dup_count+=1
		print "Image exists %d"%dup_count
		pass
	    coll.update({'_id':image[2]['product_id'][0]},{'$set':{'img_url.'+str(image[0]):image[2]['url']},'$unset':{'img_url.0':1}}, safe = True)
	    
    print "%d processed"%total
    
if __name__ == '__main__':
    go()
