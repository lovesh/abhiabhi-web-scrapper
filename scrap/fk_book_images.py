#from fk_books_scrap import getCategoryUrls,getBookUrlsOfCategory
import downloader
import dom
import re
import time
import simplejson as json
import pymongo
from datetime import datetime
import urllib

isbn_pattern=re.compile('pid=(\d+x?)',re.I)

con=pymongo.Connection()
img_cats=con.DBName.fk_img_cats
img_urls=con.DBName.fk_img_urls

siteurl='http://www.flipkart.com'
referer='http://www.flipkart.com/books'

logfile=open('fk_books_log.txt','w')
dl=downloader.Downloader()
ajax_dl=downloader.Downloader()

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

def getBookUrlsOfCategory(category_url):
    ajax_dl.addHeaders({'Referer':category_url})
    urls=[]
    marker=0
    flag=True
    while flag:
        page_urls=[category_url+'?response-type=json&inf-start='+str(x) for x in xrange(marker,marker+200,20)]
        ajax_dl.putUrls(page_urls,20)
        print page_urls
        pages=ajax_dl.download()
        print '%d Pages'%len(pages)
        print ajax_dl.responses
        for p in pages:
            status=pages[p][0]
            html=pages[p][1]
            if status > 199 and status < 400:
                json_response=json.loads(html)
                count=json_response['count']
                print count
                if count==0:
                    flag=False
                    continue
                links=getBookUrlsOfPage(string=json_response['html'])
                urls.extend(links)
        marker+=200
    return urls

def getBookUrlsOfPage(url=None,string=None):                #name is screwed to use the imported file
    book_block_path='//div[@class="line fksd-bodytext "]'
    url_path='.//h2[@class="fk-srch-item-title fksd-bodytext"]/a'
    img_path='.//div[@class="lastUnit rposition"]/a/img'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    book_blocks=page.getNodesWithXpath(book_block_path)
    books=[]
    for book_block in book_blocks:
        book={}
        image=book_block.xpath(img_path)
        if image:
            book['img_url']=image[0].get('src')
        url=book_block.xpath(url_path)
        if url:
            book['url']=siteurl+url[0].get('href')
        
        m=isbn_pattern.search(url[0].get('href'))
        if m:
            isbn=m.group(1)
            if len(isbn)==10:
                book['isbn']=isbn
            if len(isbn)==13:
                book['isbn13']=isbn
        books.append(book)
    print "%d books returned"%len(books)
    return books

def go():
    #category_urls=getCategoryUrls()
    #for url in category_urls:
     #   img_cats.insert({'url':url,'status':0})
    
    category_urls=[c['url'] for c in img_cats.find({'status':0})]
    start=time.time()
    for cu in category_urls:
        print 'Getting book urls of category %s\n\n'%cu
        urls=getBookUrlsOfCategory(cu)
        print "found %d books"%len(urls)
        img_urls.insert(list(urls),safe=True)
        img_cats.update({'url':cu},{'$set':{'status':1}},safe=True)
    finish=time.time()

if __name__ == '__main__':
    go()
