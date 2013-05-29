import downloader
import dom
import urllib
import re
import time
import datetime
import simplejson as json
import pymongo
from collections import defaultdict
import util
import string

siteurl='http://www.bookadda.com/'

books=[]
book_urls=set()                        
logfile=open('bookadda_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

debug=False
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.ba_temporary
subcat_coll=pymongo.Connection().DBName.ba_subcats

def getIncompleteSubcats():
    subcats=[{'subcat_url':subcat['subcat_url'],'num_books':subcat['num_books'],'cat':subcat['cat'],'subcat':subcat['subcat']} for subcat in subcat_coll.find()]
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
    book_url_path='//ul[@class="results"]//div[@class="details"]//h4/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(book_url_path))
    return links

def getBookUrlsOfSubcategory(subcategory_url):
    subcategory_dom=dom.DOM(url=subcategory_url)
    book_url_path='//ul[@class="results"]//div[@class="details"]//h4/a'
    book_urls=set(l[1] for l in subcategory_dom.getLinksWithXpath(book_url_path))
    result_count_path='//div[@id="search_container"]//div[@class="contentbox"]//div[@class="head"]'
    count_node=subcategory_dom.getNodesWithXpath(result_count_path)
    if count_node:
        count_string=count_node[0].text_content()
        print count_string
        count=int(re.search('(\d+) of (\d+) result',count_string).group(1))
        total=int(re.search('(\d+) of (\d+) result',count_string).group(2))
        subcat_col=pymongo.Connection().DBName.ba_subcats
        subcat_col.update({'subcat_url':subcategory_url},{'$set':{'num_books':total}})
        done=set()
        if total>count:
            page_urls=set(subcategory_url+'?pager.offset='+str(x) for x in xrange(0,total,count))
            dl.putUrls(page_urls,10)
            subcategory_pages=dl.download()
            for s in subcategory_pages:
                status=subcategory_pages[s][0]
                html=subcategory_pages[s][1]
                if status > 199 and status < 400:
                    book_urls.update(getBookUrlsFromPage(html))
                    done.add(s)
                    #print book_urls
            if done:
                failed=list(page_urls-done)
                subcat_col.update({'subcat_url':subcategory_url},{'$set':{'failed_pages':failed}})
    return book_urls

def go():
    incomplete=getIncompleteSubcats()
    print "%d incomplete subcategories found"%len(incomplete)
    logfile.write("%d incomplete subcategories found"%len(incomplete))
    for inc in incomplete:
        urls=getBookUrlsOfSubcategory(inc['subcat_url'])
        for url in urls:
            try:
                temporary.insert({'url':url,'subcat_url':inc['subcat_url'],'categories':[[inc['cat'],inc['subcat']],],'status':0},safe=True)
            except pymongo.errors.DuplicateKeyError:
                temporary.update({'url':url},{'$addToSet':{'categories':[inc['cat'],inc['subcat']]}})
        print "done with subcategory %s"%inc['subcat_url']
        logfile.write("done with subcategory %s\n\n"%inc['subcat_url'])
        subcat_coll.update({'subcat_url':inc['subcat_url']},{'$set':{'status':1}})
    con=pymongo.Connection()
    coll=con[DBName]['scraped_books']
    count=1                 #so that the following loop starts
    total=0                 #keeps a track of total downloaded books
    start=time.time()
    while count>0:
        docs=temporary.find({'status':0}).limit(500)
        count=docs.count()
        urls=[]
        processed={}
        urls=[doc['url'] for doc in docs]
        dl.putUrls(urls,30)
        result=dl.download()
        books=[]
        for r in result:
            status=str(result[r][0])
            html=result[r][1]
            if int(status) > 199 and int(status) < 400:
                book=parseBookPage(string=html)
                if book:
                    books.append(book)
            if status in processed:
                processed[status].append(r)
            else:
                processed[status]=[r,]
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
            temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':int(status)}},multi=True)
    finish=time.time()
    logfile.write("All books parsed in %s"%str(finish-start))

def parseBookPage(url=None,string=None):
    book={}
    if url:
        try:
            doc=dom.DOM(url=url)
        except:
            return False
    else:
        try:
            doc=dom.DOM(string=string,utf8=True)
        except:
            return False
    addBox=doc.getNodesWithXpath('//a[@id="addBox"]')

    url_path='//meta[@property="og:url"]'
    book['url']=doc.getNodesWithXpath(url_path)[0].get('content').strip()
    if debug:
        print book['url']
    
   
    if addBox:                           #availability check
        book['availability']=1
        shipping_path='//div[@class="paymentText1"]/strong'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=re.search('(\d+)-(\d+)',shipping[0].text)
            book['shipping']=[shipping.group(1),shipping.group(2)]
    
    #name_path='//div[@class="main_text"]/h1'
    #book['name']=doc.getNodesWithXpath(name_path)[0].text_content().strip()
    
    image_path='//meta[@property="og:image"]'
    image=doc.getNodesWithXpath(image_path)
    if image:
        book['img_url']=image[0].get('content').strip()

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        book['description']=desc[0].text_content().strip()

    price_path='//div[@class="pricingbox_inner"]/div[@class="text"]'
    price_nodes=doc.getNodesWithXpath(price_path)
    for node in price_nodes:
        span1=node.getchildren()[0].text
        if span1.strip()=='Our Price':
            price=node.getchildren()[1].text
            book['price']=int(re.search('(\d+)',price).group(1))
            break

    
    book['last_modified_datetime']=datetime.datetime.now()
    
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    if 'availability' in book:
        product_history['availability']=1
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='bookadda'

    tbody_path='//div[@class="grey_background"]/table/tbody'

    tbody=doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        isbns=re.search('(\d{10})?-?(\d{13})?$',book['url'])
        if isbns:
            if isbns.group(1):
                book['isbn']=isbns.group(1)
            if isbns.group(2):
                book['isbn13']=isbns.group(2)
        return book
    data=doc.parseTBody(tbody_path)

    if 'author' in data:
        data['author']=map(string.strip,data['author'].encode('utf8').split('\xc2\xa0')[:-1])
    
    util.replaceKey(data,'number of pages','num_pages')
    util.replaceKey(data,'date of publication','pubdate')
    util.replaceKey(data,'isbn-13','isbn13')
    if 'isbn13' in data:
        data['isbn13']=data['isbn13'].split(',')[0].replace('-','').strip()
    

    book.update(data)
    return book

if __name__ == '__main__':
    go()

