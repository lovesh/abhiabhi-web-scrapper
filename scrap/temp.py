import pymongo
import downloader
import dom
import urllib
import re
import time
import datetime
import util

debug=True
siteurl='http://www.infibeam.com'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
shipping_pattern=re.compile('(\d+)-(\d+)',re.I)
isbn_pattern=re.compile('(\d+).html$')


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
    if 'availabilty' in book:
        product_history['availabilty']=1
    product_history['time']=book['last_modified_datetime']
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

temporary=pymongo.Connection().DBName.ib_temporary
bks=pymongo.Connection().abhiabhi.scraped_books
docs=bks.find({'isbn13':{'$exists':False},'site':'infibeam'})
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

if len(books)>0:
    bks.remove({'isbn13':{'$exists':False},'site':'infibeam'})
    bks.insert(books)
for status in processed:
    temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':int(status)}},multi=True)


