import downloader
import dom
import urllib
import re
import time
import simplejson as json
import pymongo
from collections import defaultdict
import util
from datetime import datetime

siteurl='http://www.flipkart.com'
referer='http://www.flipkart.com/books'
debug=True
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.fk_temporary
temporary.create_index('url',unique=True)

books=[]
book_urls=set()                        
logfile=open('fk_books_log.txt','w')
dl=downloader.Downloader()
#dl.addHeaders({'Host':siteurl,'Referer':referer})
ajax_dl=downloader.Downloader()
#ajax_dl.addHeaders({'Host':siteurl,'X-Requested-With':'XMLHttpRequest'})

def getCategoryUrls():
    doc=dom.DOM(url='http://www.flipkart.com/all-categories-books')
    category_path='//div[@class="fk-acat-item-text"]/a'
    subcategory1_path='//li[@class="listitem last fk-lcat-level-2"]/a'
    subcategory2_path='//li[@class="listitem last fk-lcat-level-3"]/a'
    subcategory1_urls=set()           #urls for subcategories at level 1
    subcategory2_urls=set()           #urls for subcategories at level 2
    all_cat_urls=set()

    category_urls=set(siteurl+c[1] for c in doc.getLinksWithXpath(category_path))
    print "%d root cats found"%len(category_urls)
    all_cat_urls.update(category_urls)

    dl.putUrls(category_urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            sub1doc=dom.DOM(string=html)
            subcategory1_urls.update(set(siteurl+re.search('/.+\??',c[1]).group().strip('? ') for c in sub1doc.getLinksWithXpath(subcategory1_path)))
    print "%d sub1 cats found"%len(subcategory1_urls)
    all_cat_urls.update(subcategory1_urls)

    dl.putUrls(subcategory1_urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            sub2doc=dom.DOM(string=html)
            subcategory2_urls.update(set(siteurl+re.search('/.+\??',c[1]).group().strip('? ') for c in sub2doc.getLinksWithXpath(subcategory2_path)))
    print "%d sub2 cats found"%len(subcategory2_urls)
    all_cat_urls.update(subcategory2_urls)

    if debug:
        print 'Total %d categories found\n'%len(all_cat_urls)
    logfile.write('Total %d categories found\n'%len(all_cat_urls))
    logfile.flush()
    return all_cat_urls

def getBookUrlsOfPage(string):
    book_url_path='//div[@class="line bmargin10"]/h2/a'
    doc=dom.DOM(string=string)
    links=set()
    for link in doc.getLinksWithXpath(book_url_path):
        links.add(siteurl+re.search('(.+?/p/.+?)\&',link[1]).group(1).rstrip('/ '))
    return links

def getBookUrlsOfCategory(category_url):
    ajax_dl.addHeaders({'Referer':category_url})
    urls=set()
    marker=0
    flag=True
    while flag:
        page_urls=[category_url+'?response-type=json&inf-start='+str(x) for x in xrange(marker,marker+200,20)]
        ajax_dl.putUrls(page_urls,20)
        print page_urls
        pages=ajax_dl.download()
        if debug:
            print '%d Pages'%len(pages)
            print ajax_dl.responses
        for p in pages:
            status=pages[p][0]
            html=pages[p][1]
            links=set()
            if status > 199 and status < 400:
                json_response=json.loads(html)
                count=json_response['count']
                print count
                if count==0:
                    flag=False
                    continue
                links=getBookUrlsOfPage(string=json_response['html'])
                urls.update(links)
        marker+=200
    return urls

def writeBookUrlsToTemporary(urls):
    global temporary
    for url in urls:
        try:
            temporary.insert({'url':url,'status':0})
        except pymongo.errors.DuplicateKeyError:
            pass
    
def getAllBookUrls():
    #global book_urls
    count_book_urls=0
    category_urls=getCategoryUrls()
    start=time.time()
    for cu in category_urls:
        print 'Getting book urls of category %s\n\n'%cu
        urls=getBookUrlsOfCategory(cu)
        writeBookUrlsToTemporary(urls)
        count_book_urls+=len(urls)
        print 'Witring book urls of category %s\n\n'%cu
        logfile.write('Witring book urls of category %s\n\n'%cu)
        for url in urls:
          logfile.write(url+'\n')
        logfile.write('\n\n\n\n')
    finish=time.time()
    print "All book urls(%s) fetched in %s\n\n"%(count_book_urls,str(finish-start))
    logfile.write("All book urls fetched in %s\n\//your attributesn"%str(finish-start))
    logfile.flush()
    #book_urls=set('http://www.flipkart.com'+book_url for book_url in book_urls)
    #return book_urls

def parseBookPage(url=None,string=None):
    book={}
    if url:
        doc=dom.DOM(url=url)
        book['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        book['url']=url[0].get('href')
    if debug:
        print book['url']

    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]') 
    if addBox:                           #availability check
        book['availability']=1
    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        book['price']=int(price[0].text_content().strip('Rs. '))
    else:
        book['price']=-1
    
    desc_path='//div[@id="description_text"]'
    desc=doc.getNodesWithXpath(desc_path)
    if len(desc)>0:
        book['desc']=desc[0].text_content().strip()

    tbody_path='//div[@id="details"]/table'
    data=doc.parseTBody(tbody_path)
    if 'author' in data:
        data['author']=data['author'].split(',')
    
    util.replaceKey(data,'number of pages','num_pages')
    util.replaceKey(data,'publishing date','pubdate')
    util.replaceKey(data,'isbn-13','isbn13')
    if 'isbn13' in data:
        data['isbn13']=data['isbn13'].split(',')[0].replace('-','').strip()
    util.replaceKey(data,'book','name')
    
    if 'pubdate' in data:
        if len(data['pubdate'])==4 and re.search('\d{4}',data['pubdate']):
            data['pubdate']=datetime.strptime(data['pubdate'],'%Y')
        elif len(data['pubdate'])==8 and re.search('\w{3} \d{4}',data['pubdate']):
            data['pubdate']=datetime.strptime(data['pubdate'],'%b %Y')

    category_list_path='//div[@class="item_details"]/div[@class="line bread-crumbs fksk-bread-crumbs"]'
    category_lists=doc.getNodesWithXpath(category_list_path)
    book['categories']=[]
    for clist in category_lists:
        subcats=[]
        cats=clist.xpath('span/a/span')
        for cat in cats:
            subcats.append(cat.text.strip())
        book['categories'].append(subcats[1:])

    book.update(data)
    return book

def go():
    global temporary
    getAllBookUrls()
    con=pymongo.Connection()
    coll=con[DBName]['scraped_books']
    count=1                 #so that the following loop starts
    total=0                 #keeps a track of total downloaded books
    start=time.time()
    while count>0:
        docs=temporary.find({'status':0}).limit(500)
        count=docs.count()
        ids=[doc['_id'] for doc in docs]        #used for updating doc['status'] later
        urls=[doc['url'] for doc in docs]
        dl.putUrls(urls,30)
        result=dl.download()
        books=[]
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                book=parseBookPage(string=html)
                books.append(book)
        coll.insert(books)
        temporary.update({'_id':{'$in':ids}},{'$set' : {'status':1}},multi=True)
        total+=total+len(books)
    finish=time.time()
    logfile.write("All books(%s) downloaded in %s"%(total,str(finish-start)))
    
def prepareXMLFeed():
    books=go()
    root=dom.XMLNode('books')
    start=time.time()
    for book in books:
        child=root.createChildNode('book')
        child.createChildNodes(book)
    f=open('fk_books.xml','w')
    f.write(root.nodeToString())
    f.close()
    finish=time.time()
    logfile.write("XML file created in %s"%str(finish-start)) 

if __name__ == '__main__':
    go()

