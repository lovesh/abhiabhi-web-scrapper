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

siteurl='http://www.bookadda.com/'

books=[]
book_urls=set()                        
logfile=open('bookadda_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

debug=True

DBName='abhiabhi'
temporary=pymongo.Connection().DBName.ba_temporary
temporary.create_index('url',unique=True)

url_pattern = re.compile('bookadda.com', re.I)

def getCategories():
    doc=dom.DOM(url=siteurl)
    category_path='//div[@id="body_container"]//ul[@class="left_menu"][1]/li/a'
    categories=[[c[0],c[1]] for c in doc.getLinksWithXpath(category_path) if c[1] != 'http://www.bookadda.com/view-books/medical-books']
    return categories

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
        count=int(re.search('\d+ of (\d+) result',count_string).group(1))
        subcat_col=pymongo.Connection().DBName.ba_subcats
        subcat_col.update({'subcat_url':subcategory_url},{'$set':{'num_books':count}})
        if count>20:
            page_urls=set(subcategory_url+'?pager.offset='+str(x) for x in xrange(20,count,20))
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
    global book_urls
    subcategory_path='//div[@id="left_container"]/ul[@class="left_menu"][1]/li/a'
    cats=getCategories()
    subcat_col=pymongo.Connection().DBName.ba_subcats
    subcat_col.create_index('subcat_url',unique=True)
    for cat in cats:
        page=dom.DOM(url=cat[1])
        subcats=page.getLinksWithXpath(subcategory_path)
        for subcat in subcats:
            try:
                subcat_col.insert({'subcat':subcat[0].strip('\n\t\r '),'subcat_url':subcat[1],'cat':cat[0],'cat_url':cat[1],'status':0})
            except:
                pass
    try:
        subcat_col.insert({'subcat':'Medical','subcat_url':'http://www.bookadda.com/view-books/medical-books','cat':'Medical','status':0})
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
    return book_urls

def parseBookPage(url=None,string=None):
    book={}
    if url:
        try:
            doc=dom.DOM(url=url,utf8=True)
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
    if url_pattern.search(book['url']) is None:
        return False

    if addBox:                           #availability check
        book['availability']=1
        shipping_path='//span[@class="numofdys"]/strong'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=re.search('(\d+)-(\d+)',shipping[0].text)
            book['shipping']=[shipping.group(1),shipping.group(2)]
    
    else:
        book['availability']=0
        
    name_path='//div[@class="prdcol2"]/h1'
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
        book['name']=name[0].text_content().strip()
    
    image_path='//meta[@property="og:image"]'
    image=doc.getNodesWithXpath(image_path)
    if image:
        book['img_url']=image[0].text_content().strip()

    desc_path='//div[@class="reviews-box-cont-inner"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        book['description']=desc[0].text_content().strip()

    price_path='//span[@class="actlprc"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price) > 0:
        price = price[0].text.strip()
        book['price']=int(re.search('(\d+)',price).group(1))
    
    book['scraped_datetime']=datetime.datetime.now()
    book['last_modified_datetime']=datetime.datetime.now()
    
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping'] = book['shipping']
    product_history['availability'] = book['availability']
    product_history['datetime'] = book['last_modified_datetime']
    book['product_history'] = [product_history,]
    book['site']='bookadda'

    tbody_path='//div[@class="grey_background"]/table/tbody'
    if len(doc.getNodesWithXpath(tbody_path)) == 0:
        tbody_path='//div[@class="grey_background"]/table'
    
    data=doc.parseTBody(tbody_path)

    if 'author' in data:
        data['author']=data['author'].encode('utf8').split('\xc2\xa0')
    
    util.replaceKey(data,'number of pages','num_pages')
    util.replaceKey(data,'publishing date','pubdate')
    util.replaceKey(data,'isbn-13','isbn13')
    if 'isbn13' in data:
        data['isbn13']=data['isbn13'].split(',')[0].replace('-','').strip()
    util.replaceKey(data,'book','name')

    book.update(data)
    return book

def go():
    global books
    getAllBookUrls()
    temporary=pymongo.Connection().DBName.ba_temporary
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
        coll.insert(books)
        total+=total+len(books)
        for status in processed:
            temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':int(status)}},multi=True)
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





