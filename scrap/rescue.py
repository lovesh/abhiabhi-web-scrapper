import dom
import downloader
import re
import time
import sqlite3 as lite

siteurl='http://www.bookadda.com/'

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
con=lite.connect('bookadda.db')
cur=con.cursor()

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
        desc=doc.getNodesWithXpath(desc_path)
        if desc:
            book['desc']=desc[0].text_content().encode('ascii','ignore').strip()
        else:
            book['desc']=''

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
        else:
            book['authors']=''
           
        isbn_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[3]/td[2]'
        isbn=doc.getNodesWithXpath(isbn_path)
        if isbn:
            book['isbn']=isbn[0].text_content().strip()
        else:
            book['isbn']=''

        isbn13_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[4]/td[2]'
        isbn13=doc.getNodesWithXpath(isbn13_path)
        if isbn13:
            book['isbn13']=isbn13[0].text_content().strip()        
        else:
            book['isbn13']=''

        binding_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[5]/td[2]'
        binding=doc.getNodesWithXpath(binding_path)
        if binding:
            book['binding']=binding[0].text_content().strip()
        else:
            book['binding']=''

        pubdate_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[6]/td[2]'             
        pubdate=doc.getNodesWithXpath(pubdate_path)
        if pubdate:
            book['pubdate']=pubdate[0].text_content().strip() 
        else:
            book['pubdate']=''

        publisher_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[7]/td[2]'
        publisher=doc.getNodesWithXpath(publisher_path)
        if publisher:
            book['publisher']=publisher[0].text_content().strip()        
        else:
            book['publisher']=''

        language_path='//div[@class="contentbox_extreme_inner"]//tbody/tr[9]/td[2]'             
        language=doc.getNodesWithXpath(language_path)
        if language:
            book['language']=language[0].text_content().strip()
        else:
            book['language']=''
 
    return book

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

def tryy():
    r=cur.execute("select url from subcat_urls where status=0")
    subcategory_urls=[u[0] for u in r.fetchall()]
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
    print "All book urls fetched in %s\n\n"%str(finish-start)
    book_urls=cur.execute('select url from book_urls where status=0 limit 1000')
    urls=[u[0] for u in book_urls.fetchall()]
    while urls:
        dl.putUrls(urls,20)
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
                if book:
                    book['url']=r
                    cur.execute("insert into books(url,name,price,desc,authors,isbn,isbn13,publisher,pubdate,language,binding) values(?,?,?,?,?,?,?,?,?,?,?)",(r,book['name'],book['price'],book['desc'],','.join(book['authors']),book['isbn'],book['isbn13'],book['publisher'],book['pubdate'],book['language'],book['binding']))
                    cur.execute("update book_urls set status=1 where url like ?",(r,))
                else:
                    cur.execute("update book_urls set status=2 where url like ?",(r,))
                #books.append(book)
        con.commit()
        book_urls=cur.execute('select url from book_urls where status=0 limit 1000')
        urls=[u[0] for u in book_urls.fetchall()]
    finish=time.time()
    print "done"

