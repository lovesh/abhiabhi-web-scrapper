import downloader
import dom
import urllib
import re
import time
import datetime
import math
import simplejson as json
import pymongo
from collections import defaultdict
import util

siteurl='http://www.infibeam.com'

books=[]
book_urls=set()                        
logfile=open('infibeam_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)

debug=True
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.ib_temporary
temporary.create_index('url',unique=True)
subcat_coll=pymongo.Connection().DBName.ib_subcats
isbn_pattern=re.compile('(\d+).html$')
shipping_pattern=re.compile('Ships in (\d+)-(\d+)')

def getIncompleteSubcats():
    subcats=[{'subcat_url':subcat['subcat_url'],'num_books':subcat['num_books'],'cat':subcat['cat'],'subcat':subcat['subcat']} for subcat in subcat_coll.find({'num_books':{'$exists':True}})]
    incomplete=[]
    incomplete_subcats=[]
    for subcat in subcats:
        count=temporary.find({'subcat_url':subcat['subcat_url']}).count()
        if subcat['num_books']>count:
            incomplete.append(subcat['subcat_url'])
            incomplete_subcats.append(subcat)
    subcat_coll.update({'subcat_url':{'$in':incomplete}},{'$set':{'status':0}},multi=True)
    return incomplete_subcats

def getBookUrlsFromPage(html):
    book_url_path='//ul[@class="search_result"]//span[@class="title"]/h2/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(book_url_path))
    return links

def getBookUrlsOfSubcategory(subcategory_url):
    subcategory_dom=dom.DOM(url=subcategory_url)
    book_url_path='//ul[@class="results"]//div[@class="details"]//h4/a'
    book_urls=set(l[1] for l in subcategory_dom.getLinksWithXpath(book_url_path))
    count_path='//div[@id="search_result"]/div/b[2]'
    count_node=subcategory_dom.getNodesWithXpath(count_path)
    if count_node:
        count=int(subcategory_dom.getNodesWithXpath(count_path)[0].text.replace(',','').replace(u'\xa0',u''))  
        print count
        subcat_col=pymongo.Connection().DBName.ib_subcats
        subcat_col.update({'subcat_url':subcategory_url},{'$set':{'num_books':count}})
        if count>20:
            num_pages=int(math.ceil(count/20.0))
            page_urls=set(subcategory_url+'/search?page='+str(page) for page in xrange(1,num_pages))
            dl.putUrls(page_urls)
            subcategory_pages=dl.download()
            for s in subcategory_pages:
                status=subcategory_pages[s][0]
                html=subcategory_pages[s][1]
                if status > 199 and status < 400:
                    book_urls.update(getBookUrlsFromPage(html))
                    #print book_urls
    return book_urls

def parseBookPage(url=None,string=None):
    book={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')

    url_path='/html/head/meta[@property="og:url"]'
    url=doc.getNodesWithXpath(url_path)
    if url:
        book['url']=url[0].get('content').strip()
    else:
        url_path='/html/head/link[@rel="canonical"]'
        url=doc.getNodesWithXpath(url_path)
        if url:
            book['url']=url[0].get('href').strip()

    if addBox:                           #availability check
        book['availability']=1
        details_path='//div[@id="ib_details"]'
        details=doc.getNodesWithXpath(details_path)
        if details:
            details=details[0].text_content()
            shipping=shipping_pattern.search(details)
            if shipping: 
                book['shipping']=[shipping.group(1),shipping.group(2)]
    
    #name_path='//div[@class="main_text"]/h1'
    #book['name']=doc.getNodesWithXpath(name_path)[0].text_content().strip()
    
    
    price_path='//span[@class="infiPrice amount price"]'
    price=doc.getNodesWithXpath(price_path)
    if price:
        book['price']=int(price[0].text.replace(',',''))
    img_path="//img[@id='imgMain']"
    book['img_url']=doc.getImgUrlWithXpath(img_path)

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        book['description']=desc[0].text_content.strip()
    
    book['last_modified_datetime']=datetime.datetime.now()
    
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    if 'availabilty' in book:
        product_history['availabilty']=1
    product_history['time']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='infibeam'

    tbody_path='//div[@id="ib_products"]/table'

    tbody=doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        isbn=isbn_pattern.search(book['url'])
        if isbn:
            if len(isbn.group(1))==10:
                book['isbn']=isbn.group(1)
            if len(isbn.group(1))==13:
                book['isbn13']=isbn.group(1)
        return book
    data=doc.parseTBody(tbody_path)

    if 'author' in data:
        data['author']=data['author'].split(',')
    
    util.replaceKey(data,'no. of pages','num_pages')
    util.replaceKey(data,'publish date','pubdate')
    util.replaceKey(data,'ean','isbn13')
    if 'isbn13' in data:
        data['isbn13']=data['isbn13'].split(',')[0].replace('-','').strip()
    util.replaceKey(data,'title','name')

    book.update(data)
    return book

def go():
    #incomplete=getIncompleteSubcats()
    #print "%d incomplete subcategories found"%len(incomplete)
    #logfile.write("%d incomplete subcategories found"%len(incomplete))
    #for inc in incomplete:
        #print inc['subcat_url']
        #urls=getBookUrlsOfSubcategory(inc['subcat_url'])
        #for url in urls:
         #   try:
          #      temporary.insert({'url':url,'subcat_url':inc['subcat_url'],'categories':[[inc['cat'],inc['subcat']],],'status':0})
           # except pymongo.errors.DuplicateKeyError:
            #    temporary.update({'url':url},{'$addToSet':{'categories':[inc['cat'],inc['subcat']]}})
        #print "done with subcategory %s"%inc['subcat_url']
        #logfile.write("done with subcategory %s\n\n"%inc['subcat_url'])
        #subcat_coll.update({'subcat_url':inc['subcat_url']},{'$set':{'status':1}})
    con=pymongo.Connection()
    coll=con[DBName]['scraped_books']
    count=1                 #so that the following loop starts
    total=0                 #keeps a track of total downloaded books
    start=time.time()
    while count>0:
        docs=temporary.find({'status':0}).limit(1000)
        count=docs.count()
        urls=[]
        processed={}
        urls=[doc['url'] for doc in docs]
        dl.putUrls(urls,20)
        si=time.time()
        result=dl.download()
        fi=time.time()
        books=[]
        s=time.time()
        for r in result:
            status=result[r][0]
            html=result[r][1]
            print r
            if status > 199 and status < 400:
                book=parseBookPage(string=html)
                if book:
                    books.append(book)
            if status in processed:
                processed[status].append(r)
            else:
                processed[status]=[r,]            
        f=time.time()
        print "%d books parsed in %f"%(len(books),f-s)
        print "%d pages downloaded in %f"%(len(result),fi-si)
        s=time.time()
        if len(books)>0:
            c=0
            for book in books:
                try:
                    coll.insert(book,safe=True)
                    c+=1
                except:
                    pass 
            total+=total+len(books)
        for status in processed:
            temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':status}},multi=True)
        f=time.time()
        print "%d books inserted in %f"%(c,f-s)        
    finish=time.time()
    logfile.write("All books parsed in %s"%str(finish-start))

if __name__ == '__main__':
    go()

