import downloader
import dom
import urllib
import re
import time
import sqlite3 as lite

siteurl='http://www.bookadda.com/'

books=[]
book_urls=set()                        
logfile=open('bookadda_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
con=lite.connect('bookadda.db')
cur=con.cursor()

def getCategoryUrls():
    doc=dom.DOM(url=siteurl)
    category_path='//div[@id="body_container"]//ul[@class="left_menu"][1]/li/a'
    category_urls=[c[1] for c in doc.getLinksWithXpath(category_path)]
    category_urls.remove('http://www.bookadda.com/view-books/medical-books')
    return category_urls

def getSubcategoryUrls():
    category_urls=getCategoryUrls()
    subcategory_path='//div[@id="body_container"]//ul[@class="left_menu"][1]/li/a'
    dl.putUrls(category_urls,len(category_urls))
    category_pages=dl.download()
    subcategory_urls=set()
    for c in category_pages:
        status=category_pages[c][0]
        html=category_pages[c][1]
        if status > 199 and status < 400:
            urls=set(l[1] for l in dom.DOM(string=html).getLinksWithXpath(subcategory_path))
            subcategory_urls.update(urls)
            print "urls of category %s"%c
            print urls
    subcategory_urls.add('http://www.bookadda.com/view-books/medical-books')
    return subcategory_urls

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
    subcategory_urls=getSubcategoryUrls()
    cur.execute("create table subcat_urls(id integer primary key,url text unique,status integer)")
    temp=set((surl,0) for surl in subcategory_urls)
    cur.executemany("insert into subcat_urls(url,status) values(?,?)",temp)
    cur.execute("create table book_urls(id integer primary key,url text unique,status integer)")
    con.commit()
    start=time.time()
    for su in subcategory_urls:
        print('Getting book urls of subcategory %s\n\n'%su)
        urls=getBookUrlsOfSubcategory(su)
        temp=set((url,0) for url in urls)
        cur.executemany("insert or ignore into book_urls(url,status) values(?,?)",temp)
        cur.execute("update subcat_urls set status=1 where url like ?",(su,))
        con.commit()
        #book_urls.update(urls)
        print('Writing book urls of subcategory %s\n\n'%su)
        #logfile.write('Witring book urls of subcategory %s\n\n'%su)
        #for url in urls:
          #logfile.write(url+'\n')
        #logfile.write('\n\n\n\n')
    finish=time.time()
    print "All book urls(%s) fetched in %s\n\n",(len(book_urls),str(finish-start))
    logfile.write("All book urls fetched in %s\n\n",str(finish-start))
    logfile.flush()
    return book_urls

def parseBookPage(url=None,string=None):
    book={}
    if url:
        doc=dom.DOM(url=url)
    else:
        doc=dom.DOM(string=string)
    addBox=doc.getNodesWithXpath('//a[@id="addBox"]')
    if addBox:                           #availability check
        if url:
            book['url']=url

        name_path='//div[@class="main_text"]/h1'
        book['name']=doc.getNodesWithXpath(name_path)[0].text_content().strip()

        desc_path='//div[@class="reviews-box-cont-inner"]'
        book['desc']=doc.getNodesWithXpath(desc_path)[0].text_content().strip()

        price_path='//div[@class="pricingbox_inner"]/div[@class="text"]'
        price_nodes=doc.getNodesWithXpath(price_path)
        for node in price_nodes:
            span1=node.getchildren()[0].text
            if span1.strip()=='Our Price':
                price=node.getchildren()[1].text
                break
        book['price']=int(re.search('.+(\d+)',price).group(1))

        authors_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[2]/td[2]'
        authors=doc.getNodesWithXpath(authors_path)
        if authors:
            authors=authors[0].text.encode('ascii','ignore').strip().split('\n')
            book['authors']=[author.strip() for author in authors if author.strip()]
        
        isbn_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[3]/td[2]'
        isbn=doc.getNodesWithXpath(isbn_path)
        if isbn:
            book['isbn']=isbn[0].text_content().strip()
        
        isbn13_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[4]/td[2]'
        isbn13=doc.getNodesWithXpath(isbn13_path)
        if isbn13:
            book['isbn13']=isbn13[0].text_content().strip()        
        
        binding_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[5]/td[2]'
        binding=doc.getNodesWithXpath(binding_path)
        if binding:
            book['binding']=binding[0].text_content().strip()
        
        pubdate_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[6]/td[2]'             
        pubdate=doc.getNodesWithXpath(pubdate_path)
        if pubdate:
            book['pubdate']=pubdate[0].text_content().strip() 
        
        publisher_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[7]/td[2]'
        publisher=doc.getNodesWithXpath(publisher_path)
        if publisher:
            book['publisher']=publisher[0].text_content().strip()        
        
        language_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[9]/td[2]'             
        language=doc.getNodesWithXpath(language_path)
        if language:
            book['language']=language[0].text_content().strip()
    
    return book

def go():
    global books
    getAllBookUrls()
    cur.execute("create table books(id integer primary key,url text unique,name text,price integer,desc text,authors text,isbn text unique,isbn13 text unique,publisher text,pubdate text,language text,binding text)")
    con.commit()
    book_urls=cur.execute('select url from books_url where status=0')
    urls=book_urls.fetchmany(1000)
    while urls:
        dl.putUrls(urls,10)
        start=time
        start=time.time()
        result=dl.download()
        finish=time.time()
        #logfile.write("All books(%s) downloaded in %s"%(len(books),str(finish-start))) 
        start=time.time()
        for r in result:
             status=result[r][0]
             html=result[r][1]
             if status > 199 and status < 400:
                book=parseBookPage(string=html)
                book['url']=r
                cur.execute("insert into books values(?,?,?,?,?,?,?,?,?,?,?)",(r,book['name'],book['price'],book['desc'],','.join(book['authors']),book['isbn'],book['isbn13'],book['publisher'],book['pubdate'],book['language'],book['binding']))
                cur.execute("update book_urls set status=1 where url like ?",r) 
                #books.append(book)
        con.commit()
        urls=book_urls.fetchmany(1000)
    finish=time.time()
    logfile.write("All books parsed in %s"%str(finish-start)) 
    return books

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






