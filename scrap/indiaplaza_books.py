import downloader
import dom
import re
import urllib2
import math
import time
import datetime
import pymongo
import util

siteurl='http://www.indiaplaza.com'
referer='http://www.indiaplaza.com/books/'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
debug=False
DBName='abhiabhi'
con=pymongo.Connection()
book_coll=con[DBName]['scraped_books']
book_coll.create_index('url',unique=True,background=True)
temporary=pymongo.Connection().DBName.ip_temporary

shipping_pattern=re.compile('ships in (\d+) (?:to (\d+))?',re.I)

def getCategories():
    category_urls={}
    categories_page=dom.DOM(url='http://www.indiaplaza.com/books/')
    category_path='//div[@class="boxed"][2]//ul/li/a'
    categories=[[cat[0],siteurl+cat[1]] for cat in categories_page.getLinksWithXpath(category_path)]
    return categories

def insertBooksFromCategoryPage(url=None,string=None):
    book_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    book_blocks=page.getNodesWithXpath(book_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a[1]'
    price_path='.//div[@class="ourPrice"]/span'
    specs_path='.//div[@class="bksbrowsedtlsArea"]/ul/li'
    shipping_path='.//span[@class="delDateQuest"]'
    books=[]
    for book_block in book_blocks:
        book={}
        
        image=book_block.xpath(img_path)
        if image:
            book['img_url']=image[0].get('src')
        
        name=book_block.xpath(name_path)[0].text_content().encode('ascii','ignore').strip()
        #junk=name_junk_pattern.search(name)
        #if junk:
            #junk=junk.group(1)
            #name=name.replace(junk,'').strip()
        book['name']=name
        book['url']=siteurl+book_block.xpath(name_path)[0].get('href')
        
        price_string=book_block.xpath(price_path)[0].text
        price=re.search('(\d+)',price_string)
        if price:
            book['price']=int(price.group(1))
        
        shipping=re.search('Ships In (\d+)',book_block.xpath(shipping_path)[0].text)
        if shipping:
            book['shipping']=(shipping.group(1),)
        else:
            shipping=re.search('Ships In (\d+) To (\d+)',book_block.xpath(shipping_path)[0].text)
            if shipping:
                book['shipping']=[shipping.group(1),shipping.group(2)]

        spec_nodes=book_block.xpath(specs_path)
        specs=[]
        
        if spec_nodes is not None:
            for node in spec_nodes:
                if node is not None:
                    if node.xpath('b') and node.xpath('span[@class="greyFont"]'):
                        book[node.xpath('b')[0].text_content().strip(': ').lower()]=node.xpath('span[@class="greyFont"]')[0].text_content().strip()
        util.replaceKey(book,'no. of pages','num_pages')
        util.replaceKey(book,'publishing date','pubdate')
        util.replaceKey(book,'isbn-13','isbn13')
        if 'isbn13' in book:
            book['isbn13']=book['isbn13'].split(',')[0].replace('-','').strip()
        util.replaceKey(book,'format','binding')

        book['last_modified_datetime']=datetime.datetime.now()
        product_history={}
        if 'price' in book:
            product_history['price']=book['price']
        if 'shipping' in book:
            product_history['shipping']=book['shipping']
        if 'availability' not in book:                     #because all books listed are available
            product_history['availability']=1
        product_history['datetime']=book['last_modified_datetime']
        book['product_history']=[product_history,]
        book['site']='indiaplaza'
        books.append(book)
    return books
        #try:
            #book_coll.insert(book)
        #except pymongo.errors.DuplicateKeyError:
            #book['categories']=book['categories'][0]
            #book_coll.update({url:book['url']},{'$push':{'categories':book['categories']}})
       
    
def insertBooksofUrlsOfCategory(category):
    category_url=category[1]
    category_page=dom.DOM(url=category_url)
    if debug:
        print "getting books of category %s"%category_url
    count_path='//div[@class="prodNoArea"]'
    count_string=category_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('of (\d+)\D',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        category_url=re.sub('\d+\.htm','',category_url)
        page_urls=[category_url+str(n)+'.htm' for n in xrange(1,num_pages)]
        url_docs=[{'url':url,'category':category[0],'status':0} for url in page_urls]
        temporary.insert(url_docs)
    
        #dl.putUrls(page_urls)
        #result=dl.download()
        #for r in result:
            #status=result[r][0]
            #html=result[r][1]
            #if status > 199 and status < 400:
                #if debug:
                    #print r
                #insertBooksFromCategoryPage(category=category[0],string=html)
            
def go():
    cats=getCategories()
    cat_col=pymongo.Connection().DBName.ip_cats
    for cat in cats:
        cat_col.insert({'url':cat[1],'status':0,'pages':[]})
    
    for cat in cats:
        insertBooksofUrlsOfCategory(cat)

    con=pymongo.Connection()
    coll=con[DBName]['scraped_books']
    count=1                 #so that the following loop starts
    total=0                 #keeps a track of total downloaded books
    start=time.time()
    while count>0:
        docs=temporary.find({'status':0}).limit(100)
        urls=[]
        done=[]
        empty=[]
        urls=[doc['url'] for doc in docs]
        count=len(urls)
        dl.putUrls(urls,30)
        result=dl.download()
        books=[]
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                if debug:
                    print r
                bks=insertBooksFromCategoryPage(string=html)
                if len(bks)>0:
                    books.extend(bks)
                    done.append(r)
                else:
                    empty.append(r)
                
        if len(books)>0:
            c=0
            for book in books:
                try:
                    coll.insert(book,safe=True)
                    c+=1
                except:
                    pass
                temporary.update({'url':{'$in':done}},{'$set':{'status':1}},multi=True)
            total+=total+len(books)
            print "%d books inserted"%c
        if len(empty)>0:
            temporary.update({'url':{'$in':empty}},{'$set':{'status':-1}},multi=True)
    finish=time.time()
    logfile.write("All books(%d) parsed in %s"%(total,str(finish-start)))

def parseBookPage(url = None, string = None):
    book={}
    if url:
        try:
            doc = dom.DOM(url = url, utf8=True)
            book['url'] = url
            print url
        except:
            return False
    else:
        try:
            doc = dom.DOM(string = string,utf8 = True)
        except:
            return False
    
    url_path='//link[@rel="canonical"]'
    img_path='//img[@id="my_image"]'
    name_path='//div[@class="descColSkuNamenew"]/h1'
    price_path='//span[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_lblOurPrice"]/span[2]'
    url_path='//link[@rel="canonical"]'
    
    book['url']=doc.getNodesWithXpath(url_path)[0].get('href')
    
    book['img_url']={'0':doc.getImgUrlWithXpath(img_path)[0]}
    name=doc.getNodesWithXpath(name_path)[0].text_content().encode('ascii','ignore').strip()
    book['name']=name
    price_string=doc.getNodesWithXpath(price_path)
    if len(price_string)>0:
        price_string=price_string[0].text_content()
        book['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
    
    addBox=doc.getNodesWithXpath('//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_AddtoCartDiv"]')[0]

    if addBox.get('style') == "display:block;":                           #availability check
        book['availability']=1
        shipping_path='//span[@class="delDateQuest"]'
        shipping=shipping_pattern.search(doc.getNodesWithXpath(shipping_path)[0].text_content())
	if shipping:
	    book['shipping']=(shipping.group(1),)
        if shipping.group(2):
            book['shipping'][1]=shipping.group(2)
    else:
        book['availability']=0
    
    specs_path = '//div[@class="bksfdpltrArea"]/ul/li'
    specs = doc.getNodesWithXpath(specs_path)
    data = {}
    if len(specs) > 0:
        for spec in specs:
            key = spec.xpath('b')[0].text_content()
            value = spec.xpath('span')[0].text_content()
            data[key] = value
        book.update(data)
    
    book['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    product_history['availability']=book['availability']
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='indiaplaza'
    return book
    

if __name__ == '__main__':
    go()

