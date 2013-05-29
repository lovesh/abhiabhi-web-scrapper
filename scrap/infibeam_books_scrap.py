import downloader
import dom
import urllib
import re
import time
import datetime
import math
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

debug=False
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.ib_temporary
temporary.create_index('url',unique=True)
isbn_pattern=re.compile('(\d+).html$')

def getAllSubcategories():
    doc=dom.DOM(url='http://www.infibeam.com/Books/BrowseCategories.action')
    category_path='//div[@id="allcategories"]/h3/a'
    subcategory_path='//div[@id="allcategories"]/ul/li/a'
    categories=[[c[0],siteurl+c[1]] for c in doc.getLinksWithXpath(category_path)]
    subcategories=[[s[0],siteurl+s[1]] for s in doc.getLinksWithXpath(subcategory_path)]
    allcats=[]
    for cat in categories:
        pat=re.compile(cat[1])
        for sub in subcategories:
            if pat.match(sub[1]):
                allcats.append({'cat':cat[0],'subcat':sub[0],'subcat_url':sub[1]})
    return allcats

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

def getAllBookUrls():
    subcats=getAllSubcategories()
    subcat_col=pymongo.Connection().DBName.ib_subcats
    subcat_col.create_index('subcat_url',unique=True)
    temporary=pymongo.Connection().DBName.ib_temporary
    temporary.create_index('url',unique=True)
    for subcat in subcats:
        try:
            subcat_col.insert({'subcat':subcat['subcat'].strip(),'subcat_url':subcat['subcat_url'],'cat':subcat['cat'].strip(),'status':0})
        except:
            pass
    subcats=[{'cat':subcat['cat'],'subcat':subcat['subcat'],'subcat_url':subcat['subcat_url']} for subcat in subcat_col.find({'status':0})]
    start=time.time()
    for subcat in subcats:
        print 'Getting book urls of subcategory %s\n\n'%subcat['subcat_url']
        logfile.write('Getting book urls of subcategory %s\n\n'%subcat['subcat_url'])
        logfile.flush()
        urls=getBookUrlsOfSubcategory(subcat['subcat_url'])
        for url in urls:
            try:
                temporary.insert({'url':url,'subcat_url':subcat['subcat_url'],'categories':[[subcat['cat'],subcat['subcat']],],'status':0})
            except pymongo.errors.DuplicateKeyError:
                temporary.update({'url':url},{'$push':{'categories':[subcat['cat'],subcat['subcat']]}})
        print "done with subcategory %s"%subcat['subcat_url']
        logfile.write("done with subcategory %s\n\n"%subcat['subcat_url'])
        subcat_col.update({'subcat_url':subcat['subcat_url']},{'$set':{'status':1}})
    finish=time.time()
    print "All book urls(%d) fetched in %s\n\n"%(len(book_urls),str(finish-start))
    logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    logfile.flush()

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

    if debug:
        print book['url']

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
    if 'availability' in book:
        product_history['availability']=1
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='infibeam'

    tbody_path='//div[@id="ib_products"]/table'
    tbody=doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        tbody_path='//div[@id="ib_products"]/li/table'
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
    global books
    getAllBookUrls()
    temporary=pymongo.Connection().DBName.ib_temporary
    con=pymongo.Connection()
    coll=con[DBName]['scraped_books']
    count=1                 #so that the following loop starts
    total=0                 #keeps a track of total downloaded books
    start=time.time()
    while count>0:
        docs=temporary.find({'status':0}).limit(500)
        count=docs.count()
        urls=[]
        done=[]
        urls=[doc['url'] for doc in docs]
        dl.putUrls(urls,30)
        result=dl.download()
        books=[]
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                book=parseBookPage(string=html)
                if book:
                    books.append(book)
                    done.append(r)
        if len(books)>0:
            coll.insert(books)
            temporary.update({'url':{'$in':done}},{'$set':{'status':1}},multi=True)
            total+=total+len(books)
    finish=time.time()
    logfile.write("All books parsed in %s"%str(finish-start)) 
    


def prepareXMLFeed():
    go()
    root=dom.XMLNode('books')
    start=time.time()
    for book in books:
        child=root.createChildNode('book')
        child.createChildNodes(book)
    f=open('booksadda.xml','w')
    f.write(root.nodeToString())
    f.close()
    finish=time.time()
    logfile.write("XML file created in %s"%str(finish-start)) 

if __name__ == '__main__':
    go()






