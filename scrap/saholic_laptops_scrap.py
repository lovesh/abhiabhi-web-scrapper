import downloader
import dom
import urllib
import re
import datetime
import math
import pymongo
import util
from saholic_common import *

siteurl='http://www.saholic.com'
laptop_home='http://www.saholic.com/all-laptops/10050'

debug=True
logfile=open('saholic_laptop_log.txt','w')
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':laptop_home})

brand_pattern=re.compile('\w+',re.I)
shipping_pattern=re.compile('(\d+)',re.I)

def getAllLaptopUrls():
    count_path='//span[@class="resultLimit"]'
    doc=dom.DOM(url=laptop_home)
    count=int(doc.getNodesWithXpath(count_path)[0].text)
    num_pages=int(math.ceil(count/20.0))
    page_urls=[laptop_home+'?page='+str(n) for n in xrange(1,num_pages+1)]
    dl.putUrls(page_urls)
    pages=dl.download()
    laptop_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            laptop_urls.extend(getLaptopUrlsFromPage(html))
    return laptop_urls

def getLaptopUrlsFromPage(html):
    laptop_url_path='//div[@class="title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(siteurl+l[1] for l in page_dom.getLinksWithXpath(laptop_url_path))
    return links

def getLaptopFromPage(url=None,string=None):
    laptop={}
    if url:
        doc=dom.DOM(url=url)
        laptop['url']=url
    else:
        doc=dom.DOM(string=string)
        url_path='/html/head/meta[@property="og:url"]'
        laptop['url']=doc.getNodesWithXpath(url_path)[0].get('content').strip()
        if debug:
            print laptop['url']
    
    brand_path='//div[@class="name"]/span[@class="brand"]'
    laptop['brand']=doc.getNodesWithXpath(brand_path)[0].text.strip().lower()
    name_path='//div[@class="name"]/span[@class="product-name"]'
    laptop['name']=doc.getNodesWithXpath(name_path)[0].text.strip()
    image_path='//meta[@property="og:image"]'
    laptop['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="sp"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        laptop['price']=int(price[0].text_content().strip())
	
    addBox=doc.getNodesWithXpath('//a[@id="addToCart"]')

    if addBox:                           #availability check
        laptop['availability']=1
        shipping_path='//div[@id="shipping_time"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping[0].text_content()
            shipping=shipping_pattern.search(shipping)
            if shipping:
                laptop['shipping']=[shipping.group(1)]
    else:
        laptop['availability']=0
	
    specification={}

    dimensions_path='//div[@id="vtab-130089"]/div[@class="desc"]/ul/li'
    specification['machine dimensions']=get_specs_main(doc,dimensions_path)    
   
    processor_path='//div[@id="vtab-130090"]/div[@class="desc"]/ul/li'
    specification['processor']=get_specs_main(doc,processor_path)

    memory_path='//div[@id="vtab-130091"]/div[@class="desc"]/ul/li'
    specification['memory']=get_specs_main(doc,memory_path)
     
    storage_path='//div[@id="vtab-130092"]/div[@class="desc"]/ul/li'
    specification['storage']=get_specs_sub(doc,storage_path,'Hard disk drive')
    
    display_path='//div[@id="vtab-130095"]/div[@class="desc"]/ul/li'
    specification['display']=get_specs_sub(doc,display_path,'Display')
    specification['graphics']=get_specs_sub(doc,display_path,'Graphics')

    software_path='//div[@id="vtab-130100"]/div[@class="desc"]/ul/li'
    specification['software']=get_specs_main(doc,software_path)
    
    util.replaceKey(specification['memory'],'ram type','memory type')
    util.replaceKey(specification['memory'],'expansion capacity','expandable memory')

    util.replaceKey(specification['machine dimensions'],'size','dimension')

    util.replaceKey(specification['processor'],'processor family','processor')
    util.replaceKey(specification['processor'],'processor model','variant')
    util.replaceKey(specification['processor'],'processor clock speed','clock speed')
    
    util.replaceKey(specification['storage'],'storage capacity','capacity')
    util.replaceKey(specification['storage'],'hard disk interface','type')
    util.replaceKey(specification['storage'],'hard disk rotational speed','rpm')

    util.replaceKey(specification['display'],'screen size','size')
    util.replaceKey(specification['display'],'display type','type')
    if 'display resolution' in specification['display']:
        specification['display']['display resolution']=specification['display']['display resolution'].strip('wxga:')
        util.replaceKey(specification['display'],'display resolution','resolution')
    
    util.replaceKey(specification['graphics'],'gpu','graphic processor')
    util.replaceKey(specification['software'],'operating system provided','os')

    laptop['specification']=specification
    laptop['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in laptop:
        product_history['price']=laptop['price']
    if 'shipping' in laptop:
        product_history['shipping']=laptop['shipping']
    product_history['availability']=laptop['availability']
    product_history['datetime']=laptop['last_modified_datetime']
    laptop['product_history']=[product_history,]
    laptop['site']='saholic'
    return laptop

def scrapAllLaptops():
    urls=getAllLaptopUrls()
    laptops=[]
    failed=[]
    dl.putUrls(urls)
    result=dl.download()
    for r in result:
        status=result[r][0]
        html=result[r][1]
        if status > 199 and status < 400:
            laptop=getLaptopFromPage(string=html)
            if laptop:
                laptops.append(laptop)
        else:
            failed.append('%s with %s'%(r,str(status)))
    return laptops

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    laptop_coll=db['scraped_laptops']
    laptop_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    laptops=scrapAllLaptops()
    for laptop in laptops:
        try:
            laptop_coll.insert(laptop,safe=True)
            inserted_count+=1
            inserted_urls.append(laptop['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in laptop:
                upd['availability']=laptop['availability']
            if 'price' in laptop:
                upd['price']=laptop['price']
            if 'shipping' in laptop:
                upd['shipping']=laptop['shipping']
	    if 'offer' in laptop:
		upd['offer']=laptop['offer']
	    else:
		upd['offer']=''
            laptop_coll.update({'url':laptop['url']},{'$push':{'product_history':laptop['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(laptop['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'laptop','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()



    
    











    


        


    

            




    
    

