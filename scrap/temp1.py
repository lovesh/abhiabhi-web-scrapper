import pymongo
con=pymongo.Connection()
coll=con.abhiabhi['scraped_books']

docs=coll.find({'site':'homeshop18'},{'author':1,'_id':1})
ids=[]
counter=0
total=0

for doc in docs:
    if 'author' in doc:
        author=doc['author']
        id=doc['_id']
        if len(author[0])>5000:
            ids.append(id)
            total+=1
            counter+=1

    if counter==500:
        coll.update({'_id':{'$in':ids}},{'$unset':{'author':1}},safe=True,multi=True)
        print "500 updated and total is %d"%total
        ids=[]
        counter=0
