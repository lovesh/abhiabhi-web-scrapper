import downloader
import dom
import urllib
import re
import time
import pymongo
from collections import defaultdict
import util
from datetime import datetime

siteurl='http://www.flipkart.com'
dir_home='http://www.flipkart.com/books-directory/0-0'
debug=True
DBName='abhiabhi'
temporary=pymongo.Connection().DBName.fk_temporary
temporary.create_index('url',unique=True)

logfile=open('fk_books_log.txt','w')
dl=downloader.Downloader()

def getRootDirectories():
    root_dir_path='//div[@class="browse_results"]/a'
    doc=dom.DOM(url=dir_home)
    root_dirs=[siteurl+link[1] for link in doc.getLinksWithXpath(root_dir_path)]
    return root_dirs

def getSubDirectoriesOfRoot(dir):
    sub_dir_path='//div[@class="browse_results"]/a'
    doc=dom.DOM(url=dir)
    sub_dirs=[siteurl+link[1] for link in doc.getLinksWithXpath(sub_dir_path)]
    return sub_dirs

def getAllBookUrls():
    root_dirs=getRootDirectories()
    sub_dirs=[]
    book_url_path='//td/a'
    count_book_urls=0

    #for dir in root_dirs:
        #sub_dirs.extend(getSubDirectoriesOfRoot(dir))
    sub=pymongo.Connection().DBName.fk_subdirs
    for d in sub.find({'status':0}):
        sub_dirs.append(d['url'])
    

    #start=time.time()
    #for dir in sub_dirs:
     #   print 'getting book urls of subdirectory %s\n\n'%dir
      #  logfile.write('getting book urls of subdirectory %s\n\n'%dir)
       # doc=dom.DOM(url=dir,utf8=True)
        #urls=[siteurl+link[1] for link in doc.getLinksWithXpath(book_url_path)]
        #writeBookUrlsToTemporary(urls)
        #sub.update({'url':dir},{'$set' : {'status':1}})
        #count_book_urls+=len(urls)
    #finish=time.time()
    
    marker=0
    while len(sub_dirs[marker:marker+500])>0:
        dl.putUrls(sub_dirs[marker:marker+500])
        result=dl.download()
        done=[]
        failed=[]
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                doc=dom.DOM(string=html,utf8=True)
                print 'getting book urls of subdirectory %s\n\n'%r
                logfile.write('getting book urls of subdirectory %s\n\n'%r)
                urls=[siteurl+link[1] for link in doc.getLinksWithXpath(book_url_path)]
                writeBookUrlsToTemporary(urls)
                count_book_urls+=len(urls)
                done.append(r)
            else:
                logfile.write('could not get book urls of subdirectory %s.Status %s\n\n'%(r,status))
                failed.append(r)
        sub.update({'url':{'$in':done}},{'$set' : {'status':1}},multi=True)
        marker+=500

    print "All book urls(%d) fetched in %s\n\n"%(count_book_urls,str(finish-start))
    logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    logfile.flush()

def writeBookUrlsToTemporary(urls):
    global temporary
    for url in urls:
        try:
            temporary.insert({'url':url,'status':0},safe=True)
        except pymongo.errors.DuplicateKeyError:
            pass

def parseBookPage(url=None,string=None):
    book={}
    if url:
        doc=dom.DOM(url=url)
        book['url']=url
    else:
        doc=dom.DOM(string=string,utf8=True)
        url_path='//link[@rel="canonical"]'        
        url=doc.getNodesWithXpath(url_path)
        book['url']=url[0].get('href')

    if debug:
        print book['url']

    valid=re.search('/p/',book['url'])
    if valid is None:
        return False

    addBox=doc.getNodesWithXpath('//div[@id="mprod-buy-btn"]') 
    if addBox:                           #availability check
        book['availability']=1
        shipping_path='//div[@class="shipping-details"]/span[@class="boldtext"]'
        shipping=doc.getNodesWithXpath(shipping_path)[0].text
        if shipping:
            m=re.search('(\d+)-(\d+)',shipping)
            if m:
                book['shipping']=(m.group(1),m.group(2))
            else:
                m=re.search('(\d+)',shipping)
                if m:
                    book['shipping']=m.group(1)
    else:
	book['availability']=0
	
    image_path='//meta[@property="og:image"]'
    image=doc.getNodesWithXpath(image_path)
    if image:
        book['img_url']=image[0].get('content').strip()

    price_path='//span[@id="fk-mprod-our-id"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        book['price']=int(price[0].text_content().strip('Rs. '))
    else:
        book['price']=-1
    
    book['last_modified_datetime']=datetime.now()
    desc_path='//div[@id="description_text"]'
    desc=doc.getNodesWithXpath(desc_path)
    if len(desc)>0:
        book['desc']=desc[0].text_content().strip()

    tbody_path='//div[@id="details"]/table'

    tbody=doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        isbn=re.search('flipkart.com/.+-(.+)/p/',book['url']).group(1)
        if len(isbn)==10:
            book['isbn']=isbn
        else:
            book['isbn13']=isbn
        return book
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
            if cat.text:
                subcats.append(cat.text.strip())
        book['categories'].append(subcats[1:])
    
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    product_history['availability']=book['availability']
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='flipkart'

    book.update(data)
    return book

def go():
    global temporary
    getAllBookUrls()
    insertIntoDB()

def insertIntoDB():    
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
    logfile.write("All books(%d) downloaded in %s"%(total,str(finish-start)))

if __name__ == '__main__':
    go()

