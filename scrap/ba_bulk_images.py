import pymongo
import gridfs
from requests import async
import re
con=pymongo.Connection()
db=con.abhiabhi
coll=db.scraped_books
gfs=gridfs.GridFS(db,collection='images')
pat=re.compile('(\w{13})\.')
docs=coll.find({'img_url':{'$exists':False},'site':'bookadda'},snapshot=True)
counter=0
total=0
urls=[]
for doc in docs:
    if 'isbn13' in doc:
        isbn13=doc['isbn13']
        if isbn13[-1]=='X':
	    continue
        last_3=isbn13[-3:]
        mod=str(int(last_3)%20)
        if len(mod)==1:
            mod='0'+mod
        url='http://images'+mod+'.bookadda.com/images/bk_images/'+last_3+'/'+isbn13+'.jpg'
        urls.append(url)
        counter+=1
        total+=1
        if counter==500:
            requests=[async.get(u) for u in urls]
            responses = async.map(requests,size=40)
            image={}
            for r in responses:
                if r.ok:
                    image['site']='bookadda'
                    image['url']=r.request.url
                    image['type']='book'
                    m=pat.search(image['url'])
                    if m:
                        image['isbn13']=m.group(1)
                        try:
                                gfs.put(r.content,metadata=image)
                                print "image with url %s inserted"%image['url']
                        except gridfs.errors.FileExists:
                                pass
                        coll.update({'isbn13':image['isbn13']},{'$set':{'img_url'+str(r.status_code):image['url']},'$unset':{'img_url.0':1}})
            counter=0
            urls=[]
