from hs18_books_scrap1 import *
import sys

categ_name=sys.argv[1]
temporary=pymongo.Connection().DBName.hs18_temporary
con=pymongo.Connection()
coll=con[DBName]['scraped_books']
count=1                 #so that the following loop starts
total=0                 #keeps a track of total downloaded books
start=time.time()
while count>0:
    docs=temporary.find({'status':0,'categories':categ_name}).limit(1000)
    count=docs.count()
    urls=[]
    processed={}
    urls=[doc['url'] for doc in docs]
    print len(urls)
    dl.putUrls(urls,20)
    result=dl.download()
    books=[]
    for r in result:
        status=str(result[r][0])
        html=result[r][1]
        if html is None or len(html)<1000:
            status=str(0)
        if int(status) > 199 and int(status) < 400:
            print r
            book=parseBookPage(string=html)
        if book==False:
            status=str(1000)				# for book urls that contain multiple books
        if book:
            book['url']=r.strip()
            if 'author' in book and book['author'][0]>5000:
                del(book['author'])
            books.append(book)
        if status in processed:
            processed[status].append(r)
        else:
            processed[status]=[r,]
    try:
        if len(books)>0:
            c=0
            for book in books:
                try:
                    coll.insert(book,safe=True)
                    c+=1
                except:
                    pass
            total+=total+len(books)
            print "%d books inserted"%c
            for status in processed:
                temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':int(status)}},multi=True,safe=True)

    except bson.errors.InvalidStringData:
        print "Invalid String data "
        processed={'2000':[]}
        for book in books:
            processed['2000'].append(book['url'])			#for invalid string data
        temporary.update({'url':{'$in':processed['2000']}},{'$set':{'status':2000}},multi=True,safe=True)

finish=time.time()
print "%d inserted in %f"%(total,(finish-start))
