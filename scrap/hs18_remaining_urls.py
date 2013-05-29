import re
import dom
import downloader
import pymongo

best_seller_url='http://www.homeshop18.com/books/categoryid:10000/search:*/listView:true/sort:Price+Low-to-High/start:'  #'http://www.homeshop18.com/bestseller/categoryid:10000/start:'
dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':best_seller_url})
headers={'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
         'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
         'Accept-Language':'en-us,en;q=0.5',
         'Connection':'keep-alive',
         'User-Agent':'Mozilla/5.0 (Ubuntu; X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0',
	 'Host':'www.homeshop18.com','Referer':best_seller_url}

debug=True
temporary=pymongo.Connection().DBName.hs18_temporary

misc_url_pattern=re.compile('books\/miscellaneous\/')

def getBookUrlsFromListingPage(url=None,string=None):
    if string:
        doc=dom.DOM(string=string)
    book_url_path='//div[@class="listView_title"]/a'
    if doc.document:
        links=set(l[1].strip() for l in doc.getLinksWithXpath(book_url_path))
        return links
    return False

def getBestSellerUrls():
    page_urls=[best_seller_url+str(n) for n in xrange(0,300000,24)]
    mark=0
    while(page_urls[mark:mark+100]):
        misc_urls=set()
        other_urls=set()
        processed_pages={}
        dl.putUrls(page_urls[mark:mark+100])
        category_pages=dl.download()
        for c in category_pages:
            status=category_pages[c][0]
            html=category_pages[c][1]
            if html is not None and len(html)<1000:
                status=0
            if status > 199 and status < 400:
                print "getting book urls from %s"%c
                urls=getBookUrlsFromListingPage(string=html)
                if urls:
                    for url in urls:
                        m=misc_url_pattern.search(url)
                        if m:
                            misc_urls.add(url)
                        else:
                            other_urls.add(url)

        for url in misc_urls:
            try:
                temporary.insert({'url':url,'cat_url':best_seller_url,'categories':['Miscellaneous',],'status':0})
            except pymongo.errors.DuplicateKeyError:
                temporary.update({'url':url},{'$addToSet':{'categories':'Miscellaneous'}})

        for url in other_urls:
            try:
                temporary.insert({'url':url,'cat_url':best_seller_url,'status':0})
            except pymongo.errors.DuplicateKeyError:
                pass
        mark+=100

getBestSellerUrls()
        


