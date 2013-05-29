import downloader
import dom
import urllib2
import re
import time
import datetime
import math
import pymongo
from collections import defaultdict
import util

siteurl='http://www.homeshop18.com'
books_home='http://www.homeshop18.com/shop/faces/jsp/search.jsp?categoryid=10000'

books=[]
book_urls=set()                        
logfile=open('homeshop18_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':books_home})
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)
headers={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
         'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
         'Accept-Language':'en-us,en;q=0.5',
         'Connection':'keep-alive',
         'User-Agent':'Mozilla/5.0 (Ubuntu; X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0',
	 'Host':'www.homeshop18.com','Referer':books_home}

debug=False
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.hs18_temporary
temporary.create_index('url',unique=True)

def getCategories():
    req=urllib2.Request(books_home, {}, headers)
    response = urllib2.urlopen(req).read()
    doc=dom.DOM(string=response)
    category_path='//ul[@id="tree"]/li/ul/li[@class="srch_ctgry_item"]/a'
    categories=[[c[0],c[1]] for c in doc.getLinksWithXpath(category_path)] 
    return categories

def getBookUrlsFromPage(html):
    book_url_path='//div[@class="listView_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(book_url_path))
    return links

def getBookUrlsOfSubcategory(subcategory_url):
    subcategory_dom=dom.DOM(url=subcategory_url)
    book_url_path='//div[@class="listView_title"]/a'
    book_urls=set(l[1] for l in subcategory_dom.getLinksWithXpath(book_url_path))
    result_count_path='//div[@class="browse_result_title"]'
    count_node=subcategory_dom.getNodesWithXpath(result_count_path)
    if count_node:
        count_string=count_node[0].text_content()
        print count_string
        count=int(re.search('\((\d+)\)',count_string).group(1))
        subcat_col=pymongo.Connection().DBName.hs18_subcats
        subcat_col.update({'subcat_url':subcategory_url},{'$set':{'num_books':count}})
        if count>24:
            subcatgory_url=subcategory_url.replace('category:','categoryid:')
            pager_base_url=re.search('(.*?)listView:true/',subcategory_url).group(1)
            page_urls=set(pager_base_url+'search:*/listView:true/start:'+str(x) for x in xrange(24,count,24))
            dl.putUrls(page_urls)
            subcategory_pages=dl.download()
            for s in subcategory_pages:
                status=subcategory_pages[s][0]
                html=subcategory_pages[s][1]
                if status > 199 and status < 400:
                    print "getting book urls from %s"%s
                    book_urls.update(getBookUrlsFromPage(html))
                    #print book_urls
    return book_urls

#def getAllBookUrls():
    #global book_urls
    #subcategory_path='//ul[@id="tree"]/li/ul/li/ul/li/a'
    #cats=getCategories()
    #subcat_col=pymongo.Connection().DBName.hs18_subcats
    #subcat_col.create_index('subcat_url',unique=True)
    #global temporary
    #for cat in cats:
        #page=dom.DOM(url=cat[1])
        #subcats=page.getLinksWithXpath(subcategory_path)
        #for subcat in subcats:
            #try:
                #subcat_col.insert({'subcat':subcat[0].strip('\n\t\r '),'subcat_url':subcat[1],'cat':cat[0],'cat_url':cat[1],'status':0})
            #except:
                #pass
    #try:
        #subcat_col.insert({'subcat':'Miscellaneous','subcat_url':'http://www.homeshop18.com/miscellaneous/category:14567/listView:true/','cat':'Miscellaneous','status':0})
    #except:
        #pass

    #subcats=[{'cat':subcat['cat'],'subcat':subcat['subcat'],'subcat_url':subcat['subcat_url']} for subcat in subcat_col.find({'status':0})]
    #start=time.time()
    #for subcat in subcats:
        #print 'Getting book urls of subcategory %s\n\n'%subcat['subcat_url']
        #logfile.write('Getting book urls of subcategory %s\n\n'%subcat['subcat_url'])
        #logfile.flush()
        #urls=getBookUrlsOfSubcategory(subcat['subcat_url'])
        #for url in urls:
            #try:
                #temporary.insert({'url':url,'subcat_url':subcat['subcat_url'],'categories':[[subcat['cat'],subcat['subcat']],],'status':0})
            #except pymongo.errors.DuplicateKeyError:
                #temporary.update({'url':url},{'$push':{'categories':[subcat['cat'],subcat['subcat']]}})
        #print "done with subcategory %s"%subcat['subcat_url']
        #logfile.write("done with subcategory %s\n\n"%subcat['subcat_url'])
        #subcat_col.update({'subcat_url':subcat['subcat_url']},{'$set':{'status':1}})
    #finish=time.time()
    #print "All book urls(%d) fetched in %s\n\n"%(len(book_urls),str(finish-start))
    #logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    #logfile.flush()
    #return book_urls
    
def getAllBookUrls():
    global book_urls
    cats=getCategories()
    cat_col=pymongo.Connection().DBName.hs18_cats
    cat_col.create_index('cat_url',unique=True)
    for cat in cats:
        if cat[0][1]=='http://www.homeshop18.com/miscellaneous/category:14567/listView:true/':
            continue
        m=re.search('(.*?)\((\d+)\)',cat[0])
        cat_name=m.group(1)
        num_books=m.group(2)
        try:
            cat_col.insert({'cat':cat_name.strip(''),'cat_url':cat[1],'num_books':num_books,'status':0})
        except:
            pass
	
	cats=[{'cat':cat['cat'],'cat_url':cat['cat_url']} for cat in cat_col.find({'status':0})]
    start=time.time()
    for cat in cats:
        print 'Getting book urls of category %s\n\n'%cat['cat_url']
        logfile.write('Getting book urls of category %s\n\n'%cat['cat_url'])
        logfile.flush()
        urls=getBookUrlsOfSubcategory(cat['cat_url'])
        for url in urls:
            try:
                temporary.insert({'url':url,'cat_url':cat['cat_url'],'categories':[cat['cat'],],'status':0})
            except pymongo.errors.DuplicateKeyError:
                temporary.update({'url':url},{'$push':{'categories':cat['cat']}})
        print "done with category %s"%cat['cat_url']
        logfile.write("done with category %s\n\n"%cat['cat_url'])
        cat_col.update({'cat_url':cat['cat_url']},{'$set':{'status':1}})
    finish=time.time()
    print "All book urls(%d) fetched in %s\n\n"%(len(book_urls),str(finish-start))
    logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    logfile.flush()
    return book_urls
		

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
    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    url_path='/html/head/meta[@property="og:url"]'
    book['url']=doc.getNodesWithXpath(url_path)[0].get('content').strip()
    if debug:
        print book['url']

    if addBox:                           #availability check
        book['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            book['shipping']=[shipping.group(1),shipping.group(2)]

    name_path='//h1[@id="productLayoutForm:pbiName"]'
    book['name']=doc.getNodesWithXpath(name_path)[0].text
    
    image_path='html/head/meta[@property="og:image"]'
    image=doc.getNodesWithXpath(image_path)
    if image:
        book['img_url']=image[0].text_content().strip()

    desc_path='//div[@class="product_dscrpt_product_summary_area"]'
    desc=doc.getNodesWithXpath(desc_path)
    if desc:
        book['description']=desc[0].text_content().strip()

    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price_node=doc.getNodesWithXpath(price_path)
    if price_node:
        price=price_node[0].text
        book['price']=int(re.search('(\d+)',price).group(1))
    
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
    book['site']='homeshop18'

    tbody_path='//table[@class="productKeywords"]'

    tbody=doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        isbns=re.search('(\d{13})',book['url'])
        if isbns:
            if isbns.group(1):
                book['isbn13']=isbns.group(1)
        return book
    data=doc.parseTBody(tbody_path)

    if 'author' in data:
        data['author']=data['author'].encode('utf8').split(',')
    
    util.replaceKey(data,'no. of pages','num_pages')
    util.replaceKey(data,'publishing date','pubdate')
    util.replaceKey(data,'isbn','isbn13')
    util.replaceKey(data,'isbn-10','isbn')
    if 'isbn13' in data:
        data['isbn13']=data['isbn13'].split(',')[0].replace('-','').strip()
    util.replaceKey(data,'cover','format')
    
    book.update(data)
    return book

def go():
    global books
    getAllBookUrls()
    temporary=pymongo.Connection().DBName.hs18_temporary
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
        dl.putUrls(urls,10)
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

if __name__ == '__main__':
    go()

