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
import string

siteurl='http://www.homeshop18.com'
books_home='http://www.homeshop18.com/shop/faces/jsp/search.jsp?categoryid=10000'

books=[]
book_urls = set()                        
logfile = open('homeshop18_log.txt','w')
dl = downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':books_home})
shipping_pattern = re.compile('(\d+)-(\d+)')
pager_base_url_pattern = re.compile('(.*?)listView:true/')
url_to_page_pattern = re.compile('start:(\d+)')
headers={'Accept':'text/html,application/xhtml+xml,application/xml;q = 0.9,*/*;q = 0.8',
         'Accept-Charset':'ISO-8859-1,utf-8;q = 0.7,*;q = 0.7',
         'Accept-Language':'en-us,en;q = 0.5',
         'Connection':'keep-alive',
         'User-Agent':'Mozilla/5.0 (Ubuntu; X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0',
	 'Host':'www.homeshop18.com','Referer':books_home}

debug = True
DBName = 'abhiabhi'
temporary = pymongo.Connection().DBName.hs18_temporary
temporary.create_index('url',unique = True)
cat_col = pymongo.Connection().DBName.hs18_cats
cat_col.create_index('cat_url',unique = True)

def getCategories():
    req = urllib2.Request(books_home, {}, headers)
    response = urllib2.urlopen(req).read()
    doc = dom.DOM(string = response)
    category_path='//ul[@id="tree"]/li/ul/li[@class="srch_ctgry_item"]/a'
    categories=[[c[0],c[1]] for c in doc.getLinksWithXpath(category_path)] 
    return categories

def getBookUrlsFromPage(html):
    book_url_path='//div[@class="listView_title"]/a'
    page_dom = dom.DOM(string = html)
    if page_dom.document:
        links = set(l[1].strip() for l in page_dom.getLinksWithXpath(book_url_path))
        return links
    return False

def insertBookUrlsOfCategory(category):
    category_url = category['cat_url']
    doc=[doc for doc in cat_col.find({'cat_url':category_url},{'pages':1})]
    pages = doc[0]['pages']
    pager_base_url = pager_base_url_pattern.search(category_url.replace('category:','categoryid:')).group(1)
    page_urls=[pager_base_url+'search:*/listView:true/start:'+str(x) for x in pages if pages[x]==0]
    mark = 0
    while(page_urls[mark:mark+100]):
        book_urls = set()
        processed_pages={}
        dl.putUrls(page_urls[mark:mark+100])
        category_pages = dl.download()
        for c in category_pages:
            status = category_pages[c][0]
            html = category_pages[c][1]
            if len(html)<1000:
                status = 0
            if status > 199 and status < 400:
                print "getting book urls from %s"%c
                urls = getBookUrlsFromPage(html)
                if urls:
                    book_urls.update(urls)
                    processed_pages[url_to_page_pattern.search(c).group(1)]=status

        for url in book_urls:
            try:
                temporary.insert({'url':url,'cat_url':category_url,'categories':[category['name'],],'status':0})
            except pymongo.errors.DuplicateKeyError:
                temporary.update({'url':url},{'$addToSet':{'categories':category['name']}})
        pages.update(processed_pages)
        cat_col.update({'cat_url':category_url},{'$set':{'pages':pages}})
        mark+=100
    for p in pages:
        if pages[p]==0:
            return
    cat_col.update({'cat_url':category_url},{'$set':{'status':1}}) 
        

#def getBookUrlsFromBookLists(url = url,string = string):
 #   if url:
  #      try:
   #         doc = dom.DOM(url = url)
    #    except:
     #       return False
    #else:
     #   try:
      #      doc = dom.DOM(string = string)
       # except:
        #    return False
    #book_url_path='//div[@class="listView_title"]/a'
    #if doc.document:
     #   links = set(l[1].strip() for l in doc.getLinksWithXpath(book_url_path))
      #  return links
   # return False


#def getAllBookUrls():
    #global book_urls
    #subcategory_path='//ul[@id="tree"]/li/ul/li/ul/li/a'
    #cats = getCategories()
    #subcat_col = pymongo.Connection().DBName.hs18_subcats
    #subcat_col.create_index('subcat_url',unique = True)
    #global temporary
    #for cat in cats:
        #page = dom.DOM(url = cat[1])
        #subcats = page.getLinksWithXpath(subcategory_path)
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
    #start = time.time()
    #for subcat in subcats:
        #print 'Getting book urls of subcategory %s\n\n'%subcat['subcat_url']
        #logfile.write('Getting book urls of subcategory %s\n\n'%subcat['subcat_url'])
        #logfile.flush()
        #urls = getBookUrlsOfSubcategory(subcat['subcat_url'])
        #for url in urls:
            #try:
                #temporary.insert({'url':url,'subcat_url':subcat['subcat_url'],'categories':[[subcat['cat'],subcat['subcat']],],'status':0})
            #except pymongo.errors.DuplicateKeyError:
                #temporary.update({'url':url},{'$push':{'categories':[subcat['cat'],subcat['subcat']]}})
        #print "done with subcategory %s"%subcat['subcat_url']
        #logfile.write("done with subcategory %s\n\n"%subcat['subcat_url'])
        #subcat_col.update({'subcat_url':subcat['subcat_url']},{'$set':{'status':1}})
    #finish = time.time()
    #print "All book urls(%d) fetched in %s\n\n"%(len(book_urls),str(finish-start))
    #logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    #logfile.flush()
    #return book_urls
    
def getAllBookUrls():
    global book_urls
    #cats = getCategories()
    global cat_col
    #for cat in cats:
     #   if cat[0][1]=='http://www.homeshop18.com/miscellaneous/category:14567/listView:true/':
      #      continue
       # m = re.search('(.*?)\((\d+)\)',cat[0])
        #cat_name = m.group(1)
        #num_books = m.group(2)
        #try:
         #   cat_col.insert({'cat':cat_name.strip(''),'cat_url':cat[1],'num_books':num_books,'status':0})
        #except:
            #pass
	
    cats=[{'name':cat['name'],'cat_url':cat['cat_url']} for cat in cat_col.find({'status':0})]
    start = time.time()
    for cat in cats:
        print 'Getting book urls of category %s\n\n'%cat['cat_url']
        logfile.write('Getting book urls of category %s\n\n'%cat['cat_url'])
        logfile.flush()
        insertBookUrlsOfCategory(cat)
        
        print "done with category %s"%cat['cat_url']
        logfile.write("done with category %s\n\n"%cat['cat_url'])
        
    finish = time.time()
    print "All book urls(%d) fetched in %s\n\n"%(len(book_urls),str(finish-start))
    logfile.write("All book urls fetched in %s\n\n"%str(finish-start))
    logfile.flush()
    return book_urls
		

def parseBookPage(url = None,string = None):
    book={}
    
    if url:
        try:
            doc = dom.DOM(url = url, utf8=True)
            book['url'] = url
        except:
            return False
    else:
        try:
            doc = dom.DOM(string = string,utf8 = True)
        except:
            return False
    
    if doc.getNodesWithXpath('//div[@class="bg_header"]'):
        
        return False

    addBox = doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        book['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping = doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping = shipping_pattern.search(shipping[0].text)
            book['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        book['availability']=0
        
    name_path='//h1[@id="productLayoutForm:pbiName"]'
    name = doc.getNodesWithXpath(name_path)
    if len(name) > 0:
        book['name']=name[0].text.strip()
    
    image_path='//meta[@property="og:image"]'
    image = doc.getNodesWithXpath(image_path)
    if image:
        book['img_url']=image[0].get('content').strip()
        
    url_path='//meta[@property="og:url"]'
    url = doc.getNodesWithXpath(url_path)
    if url:
        book['url']=url[0].get('content').strip()
        
    print book['url']
    
    desc_path='//div[@class="product_dscrpt_product_summary_area"]'
    desc = doc.getNodesWithXpath(desc_path)
    if desc:
        book['description']=desc[0].text_content().strip()

    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price_node = doc.getNodesWithXpath(price_path)
    if price_node:
        price = price_node[0].text
        book['price']=int(re.search('(\d+)',price).group(1))
    
    book['last_modified_datetime']=datetime.datetime.now()
    
    product_history={}
    if 'price' in book:
        product_history['price']=book['price']
    if 'shipping' in book:
        product_history['shipping']=book['shipping']
    product_history['availability']=book['availability']
    product_history['datetime']=book['last_modified_datetime']
    book['product_history']=[product_history,]
    book['site']='homeshop18'

    tbody_path='//table[@class="productKeywords"]'

    tbody = doc.getNodesWithXpath(tbody_path)
    if len(tbody)==0:
        isbns = re.search('(\d{13})',book['url'])
        if isbns:
            if isbns.group(1):
                book['isbn13']=isbns.group(1)
        return book
    data = doc.parseTBody(tbody_path)

    if 'author' in data:
        try:
            data['author']=data['author'].encode('utf8').split(',')
        except UnicodeDecodeError:
            pass
    
    util.replaceKey(data,'no. of pages','num_pages')
    if 'num_pages' in data:
        data['num_pages']=int(data['num_pages'])
    util.replaceKey(data,'publishing date','pubdate')
    if 'pubdate' not in data:
        if 'date of publication' in data:
            try:
                d = datetime.datetime.strptime(data['date of publication'],'%b %Y')
                data['pubdate']=d
            except ValueError:
                try:
                    d = datetime.datetime.strptime(data['date of publication'],'%Y-%m-%d')
                    data['pubdate']=d
                except ValueError:
                    pass
                    
    if 'year of publication' in data and 'pubdate' in data:
        try:
            d = datetime.datetime.strptime(data['year of publication'],'%b %Y')
            data['pubdate']=d
        except ValueError:
            pass

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
    temporary = pymongo.Connection().DBName.hs18_temporary
    con = pymongo.Connection()
    coll = con[DBName]['scraped_books']
    count = 1                 #so that the following loop starts
    total = 0                 #keeps a track of total downloaded books
    start = time.time()
    while count>0:
        docs = temporary.find({'status':0}).limit(500)
        count = docs.count()
        urls=[]
        processed={}
        urls=[doc['url'] for doc in docs]
        dl.putUrls(urls,10)
        result = dl.download()
        books=[]
        for r in result:
            status = str(result[r][0])
            html = result[r][1]
            if html is None or len(html)<1000:
                status = str(0)
            if int(status) > 199 and int(status) < 400:
                print r
                book = parseBookPage(string = html)
                if book==False:
                    status = str(1000)				# for book urls that contain multiple books
                if book:
                    book['url']=r.strip()
                    if 'author' in book and book['author'][0]>5000:
                        del(book['author'])
                    books.append(book) 
            if status in processed:
                processed[status].append(r)
            else:
                processed[status]=[r,]
        try:
            if len(books)>0:
                c = 0
                for book in books:
                    try:
                        coll.insert(book,safe = True)
                        c+=1
                    except:
                        pass
            total+=total+len(books)
            print "%d books inserted"%c
            for status in processed:
                temporary.update({'url':{'$in':processed[status]}},{'$set':{'status':int(status)}},multi = True,safe = True) 

        except bson.errors.InvalidStringData:
            print "Invalid String data "
            processed={'2000':[]}
            for book in books:
                processed['2000'].append(book['url'])			#for invalid string data
            temporary.update({'url':{'$in':processed['2000']}},{'$set':{'status':2000}},multi = True,safe = True) 
    finish = time.time()
    logfile.write("All books parsed in %s"%str(finish-start))

if __name__ == '__main__':
    go()



