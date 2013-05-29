import downloader
import dom
import urllib2
import re
import time
import math
import pymongo
from collections import defaultdict
import datetime

siteurl='http://www.infibeam.com'
category_browser='http://www.infibeam.com/Books/BrowseCategories.action'
subcategory_browser='http://www.infibeam.com/Books/BrowseCategories.action'
books=[]
book_urls=defaultdict(list)                        
logfile=open('infibeam_books_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})

shipping_pattern = re.compile('in (\d+) business days', re.I)

def getCategoryUrls():
    category_page=dom.DOM(url=category_browser)
    category_path='//div[@id="allcategories"]//h3/a'
    category_urls=dict((link[0],'http://www.infibeam.com'+link[1]) for link in category_page.getLinksWithXpath(category_path))
    return category_urls

def getSubCategoryUrls():
    category_page=dom.DOM(url=subcategory_browser)
    subcategory_path='//div[@id="allcategories"]//ul/li/a'
    subcategory_urls=set('http://www.infibeam.com'+link[1] for link in category_page.getLinksWithXpath(subcategory_path))
    return subcategory_urls

def getBookUrlsFromPage(html):
    book_url_path='//ul[@class="search_result"]//span[@class="title"]/h2/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(book_url_path))
    return links

def getBookUrlsOfCategory(cat,category_url):
    page=urllib2.urlopen(category_url)
    html=page.read()
    page.close()
    page=dom.DOM(string=html)
    urls=getBookUrlsFromPage(html)                      #get book urls from first page 
    count_path='//div[@id="search_result"]/div/b[2]'
    count=int(page.getNodesWithXpath(count_path)[0].text.replace(',',''))  
    print count
    if count>20: 
        num_pages=int(math.ceil(count/20.0))
        page_urls=set(category_url+'/search?page='+str(page) for page in xrange(2,num_pages))
        print page_urls
        dl.putUrls(page_urls)
        result=dl.download()                      
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                urls.update(getBookUrlsFromPage(html))
    url_dict={}
    for url in urls:
        url_dict[url]=cat
    return url_dict

def getAllBookUrls():
    global book_urls
    category_urls=getCategoryUrls()
    start=time.time()
    for cat in category_urls:
        print('Getting book urls of category %s\n\n'%cat)
        urls=getBookUrlsOfCategory(cat,category_urls[cat])
        print('Witring book urls of category %s\n\n'%cat)
        logfile.write('Witring book urls of category %s\n\n'%cat)
        for url in urls:
          logfile.write(url+'\n')
          book_urls[url].append(urls[url])
        logfile.write('\n\n\n\n')
    finish=time.time()
    print "All book urls(%s) fetched in %s\n\n",(len(book_urls),str(finish-start))
    logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    logfile.flush()
    return book_urls
 
def parseBookPage(url=None,string=None):
    book={}
    print url 
    if url:
        try:
            doc=dom.DOM(url=url)
        except urllib2.HTTPError:
            return False
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//input[@class="buyimg "]')
    if url:
        book['url']=url
    
    if addBox:                           #availability check
        book['availability']=1           # availability 1 signals "in stock"
        m = shipping_pattern.search(doc.html)
        if m:
            book['shipping']=(int(m.group(1)), )
    else:
        book['availability']=0
    price_path = '//span[@class="infiPrice amount price"]'
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
        book['price']=int(price[0].text.replace(',', ''))
        
    img_path="//img[@id='imgMain']"
    book['img_url']=doc.getImgUrlWithXpath(img_path)
    tbody_path='//div[@id="ib_products"]/table/tbody'
    if len(doc.getNodesWithXpath(tbody_path)) == 0:
        tbody_path='//div[@id="ib_products"]/table'
        if len(doc.getNodesWithXpath(tbody_path)) == 0:
            tbody_path='//table[@style="color:#333; font:verdana,Arial,sans-serif;"]'
    data=doc.parseTBody(tbody_path)
    if data:
        if 'author' in data:
            data['author']=data['author'].split(',')
        if 'publish date' in data:
            m=re.search('(\d+)-(\d+)-(\d+)',data['publish date'])
            if m:
                data['pubdate']=datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        book.update(data)
    book['scraped_datetime']=datetime.datetime.now()
    book['last_modified_datetime']=datetime.datetime.now()
    book['site']='infibeam'
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    product_history['availability']=book['availability']
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    

    return book

def go():
    global books
    urls=getAllBookUrls()
    dl.putUrls(urls,10)
    start=time
    start=time.time()
    result=dl.download()
    finish=time.time()
    logfile.write("All books(%s) downloaded in %s"%(len(books),str(finish-start))) 
    start=time.time()
    for r in result:
         status=result[r][0]
         html=result[r][1]
         if status > 199 and status < 400:
            book=parseBookPage(string=html)
            book['url']=r
            if r.find('/Books/') == -1:
                book['type']='ebook'
            else:
                book['type']='book'
            books.append(book)
    finish=time.time()
    logfile.write("All books parsed in %s"%str(finish-start)) 
    return books

def prepareXMLFeed():
    books=go()
    root=dom.XMLNode('books')
    start=time.time()
    for book in books:
        child=root.createChildNode('book')
        child.createChildNodes(book)
    f=open('infibeam_books.xml','w')
    f.write(root.nodeToString())
    f.close()
    finish=time.time()
    logfile.write("XML file created in %s"%str(finish-start)) 
