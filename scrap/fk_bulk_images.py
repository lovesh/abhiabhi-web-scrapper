import pymongo
import downloader
import re
import lxml.html

books_home='http://www.flipkart.com/books'
headers={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
         'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
         'Accept-Language':'en-us,en;q=0.5',
         'Connection':'keep-alive',
         'User-Agent':'Mozilla/5.0 (Ubuntu; X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0',
	 'Host':'www.homeshop18.com','Referer':books_home}

dl=downloader.Downloader()
#dl.addHeaders({'Host':'www.flipkart.com','Referer':books_home})

con=pymongo.Connection()
db=con.abhiabhi
coll=db.scraped_books
#img_coll=con.DBName.hs18_imgs
#pat=re.compile('isbn\:(\w{13})\/')
docs=coll.find({'img_url':{'$exists':False},'site':'flipkart'},timeout=False)
counter=0
total=0
urls=[]
for doc in docs:
    url=doc['url'] 
    urls.append(url)
    counter+=1
    
    if counter==500:
        dl.putUrls(urls,20)
        responses=dl.download()
        c=0
        for r in responses:
            status=responses[r][0]
            html=responses[r][1]
            if html is None or len(html)<1000:
                status=0
            if status > 199 and status < 400: 
                total+=1
                page=lxml.html.document_fromstring(html)
                img=page.xpath('//meta[@property="og:image"]')
	        if len(img)>0:
		    img=img[0].get('content').strip()
                    #m=pat.search(r)
                    #if m:
                        #isbn13=m.group(1)
		        #img_coll.insert({'bk_url':r,'img_url':img,'isbn13':isbn13})
                    coll.update({'url':r},{'$set':{'img_url':{'0':img}}},safe=True)
                    c+=1
        print "%d image urls inserted"%c
        print total
        counter=0
        urls=[]



