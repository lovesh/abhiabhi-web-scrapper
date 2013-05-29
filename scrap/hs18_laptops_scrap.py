import downloader
import dom
import urllib2
import re
import time
import datetime
import math
import pymongo
import util

siteurl='http://www.homeshop18.com'
laptop_home='http://www.homeshop18.com/laptops/category:3291/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.homeshop18.com','Referer':laptop_home})
debug=True
DBName='abhiabhi'

count_pattern=re.compile('\((\d+)\)')
name_pattern=re.compile('(.*?)\(')
brand_pattern=re.compile('^\w+')
shipping_pattern=re.compile('(\d+)-(\d+)')
ram_pattern=re.compile('(1\d|(?:\W|^)\d) ?gb',re.I)
ram_type_pattern=re.compile('DDR\d?',re.I)
hdd_capacity_pattern=re.compile('(\d{3,4}G|\d\.?\d?T)B',re.I)
hdd_rpm_pattern=re.compile('\d+ ?RPM',re.I)
cpu_clockspeed_pattern=re.compile('\d\.?\d* ?GHz',re.I)
cpu_name_pattern=re.compile('(i(3|5|7))|pdc|pentium dual core|core ?2',re.I)
cpu_gen_pattern=re.compile('\d((nd)|(rd)|(st))? gen\w*',re.I)
display_resolution_pattern=re.compile('\d{3} ?x ?\d{3}',re.I)
display_size_pattern=re.compile('\d{2}\.?\d* ?(('')|"|inch)',re.I)

def getLaptopUrlsFromPage(html):
    laptop_url_path='//p[@class="product_title"]/a'
    page_dom=dom.DOM(string=html)
    links=set(l[1] for l in page_dom.getLinksWithXpath(laptop_url_path))
    return links

def getAllLaptopUrls():
    count_path='//div[@class="browse_result_title"]'
    doc=dom.DOM(url=laptop_home)
    count=doc.getNodesWithXpath(count_path)[0].text_content()
    m=count_pattern.search(count)
    if m:
        count=int(m.group(1))
    pager_base_url=laptop_home.replace('category:','categoryid:')
    page_urls=[pager_base_url+'search:*/start:'+str(n) for n in xrange(0,count,24)]
    dl.putUrls(page_urls)
    pages=dl.download()
    laptop_urls=[]
    for p in pages:
        status=pages[p][0]
        html=pages[p][1]
        if status > 199 and status < 400:
            laptop_urls.extend(getLaptopUrlsFromPage(html))
    return laptop_urls

def getLaptopFromPage(url=None,string=None):
    laptop={}
    if url:
        doc=dom.DOM(url=url)
        laptop['url']=url
    else:
        doc=dom.DOM(string=string)
        
    title_path='//h1[@id="productLayoutForm:pbiName"]'
    title=doc.getNodesWithXpath(title_path)[0].text.strip()
    m=name_pattern.search(title)
    if m:
        laptop['name']=m.group(1)
    else:
        laptop['name']=title

    laptop['brand']=brand_pattern.search(laptop['name']).group()
    image_path='//meta[@property="og:image"]'
    laptop['img_url']={'0':doc.getNodesWithXpath(image_path)[0].get('content')}
    price_path='//span[@id="productLayoutForm:OurPrice"]'
    price=doc.getNodesWithXpath(price_path)
    if len(price)>0:
        laptop['price']=int(price[0].text.strip('Rs. '))

    addBox=doc.getNodesWithXpath('//a[@id="productLayoutForm:addToCartAction"]')

    if addBox:                           #availability check
        laptop['availability']=1
        shipping_path='//div[@class="pdp_details_deliveryTime"]'
        shipping=doc.getNodesWithXpath(shipping_path)
        if shipping:
            shipping=shipping_pattern.search(shipping[0].text)
            laptop['shipping']=[int(shipping.group(1)),int(shipping.group(2))]
    else:
        laptop['availability']=0

    offer_path='//div[@class="pdp_details_offer_text"]'
    offer=doc.getNodesWithXpath(offer_path)
    if offer:
        laptop['offer']=offer[0].text.strip()

    specification={}
    specs_path='//table[@class="specs_txt"]/tbody'
    specs=doc.getNodesWithXpath(specs_path)
    if len(specs)>0:
        i=1
        for spec in specs:
            if spec.xpath('tr[1]/th'):
                if spec.xpath('tr[1]/th')[0].text:
                    if spec.xpath('tr[1]/th')[0].text.strip().lower()=='technical specifications':
                        break
            i+=1
       
        specs=doc.parseTBody('//table[@class="specs_txt"]['+str(i)+']/tbody')
           
        util.replaceKey(specs,'memory','ram')
        util.replaceKey(specs,'hard disk drive','hdd')
        util.replaceKey(specs,'hard drive','hdd')
        util.replaceKey(specs,'hard disk','hdd')
        util.replaceKey(specs,'operating system','os')
        util.replaceKey(specs,'display screen','display')
        util.replaceKey(specs,'screen size','display')
        util.replaceKey(specs,'cpu','processor')
        
        
        specification['memory']={}
        if 'ram' not in specs:
            st_path='//div[@class="summarytxt_features"]/ul'
            if doc.getNodesWithXpath(st_path):
                st=doc.getNodesWithXpath(st_path)[0].text_content()
                m=ram_pattern.search(st)
                if m:
                    ram=m.group()
                    specification['memory']['ram']=ram
                m=ram_type_pattern.search(st)
                if m:
                    type=m.group()
                    specification['memory']['memory type']=type
        else:
            m=ram_pattern.search(specs['ram'])
            if m:
                ram=m.group()
                specification['memory']['ram']=ram
            m=ram_type_pattern.search(specs['ram'])
            if m:
                type=m.group()
                specification['memory']['memory type']=type
            
            if len(specification['memory'])==0:
                specification['memory']=specs['ram']
        
        if 'hdd' in specs:
            specification['storage']={}
            m=hdd_capacity_pattern.search(specs['hdd'])
            if m:
                cap=m.group()
                specification['storage']['capacity']=cap
            m=hdd_rpm_pattern.search(specs['hdd'])
            if m:
                rpm=m.group()
                specification['storage']['rpm']=rpm
            
            if len(specification['storage'])==0:
                specification['storage']=specs['hdd']

        specification['display']={}
        if 'display' in specs:
			m=display_resolution_pattern.search(specs['display'])
			if m:
				res=m.group()
				specification['display']['resolution']=res
			m=display_size_pattern.search(specs['display'])
			if m:
				size=m.group()
				specification['display']['size']=size
			
			if len(specification['display'])==0:
				specification['display']=specs['display']

        specification['processor']={}
        m=cpu_clockspeed_pattern.search(specs['processor'])
        if m:
            speed=m.group()
            specification['processor']['clock speed']=speed
        m=cpu_gen_pattern.search(specs['processor'])
        if m:
            gen=m.group()
            specification['processor']['generation']=gen
        m=cpu_name_pattern.search(specs['processor'])
        if m:
            cpu_name=m.group()
            specification['processor']['processor']=cpu_name
        else:
            specification['processor']['processor']=specs['processor']
        
        if len(specification['processor'])==0:
            specification['processor']=specs['processor']
        
        for spec in specs:
            if spec not in ['processor','display','ram','hdd','operating system']:
                specification[spec]=specs[spec]
    
    else:
        summary_path='//div[@class="product_dscrpt_summarytxt_box"]'
        summary=doc.getNodesWithXpath(summary_path)[0].text_content().strip()
        specification['memory']={}
        m=ram_pattern.search(summary)
        if m:
            ram=m.group()
            specification['memory']['ram']=ram
        m=ram_type_pattern.search(summary)
        if m:
            type=m.group()
            specification['memory']['memory type']=type
        
        specification['storage']={}
        m=hdd_capacity_pattern.search(summary)
        if m:
            cap=m.group()
            specification['storage']['capacity']=cap
        m=hdd_rpm_pattern.search(summary)
        if m:
            type=m.group()
            specification['storage']['rpm']=type
            
        specification['display']={}
        m=display_resolution_pattern.search(summary)
        if m:
            res=m.group()
            specification['display']['resolution']=res
        m=display_size_pattern.search(summary)
        if m:
            size=m.group()
            specification['display']['size']=size
        
        specification['processor']={}
        m=cpu_clockspeed_pattern.search(summary)
        if m:
            res=m.group()
            specification['processor']['clock speed']=res
        m=cpu_gen_pattern.search(summary)
        if m:
            gen=m.group()
            specification['processor']['generation']=gen
        m=cpu_name_pattern.search(summary)
        if m:
            name=m.group()
            specification['processor']['processor']=name
        
    laptop['specification']=specification
    laptop['last_modified_datetime']=datetime.datetime.now()
    product_history={}
    if 'price' in laptop:
        product_history['price']=laptop['price']
    if 'shipping' in laptop:
        product_history['shipping']=laptop['shipping']
    product_history['availability']=laptop['availability']
    product_history['time']=laptop['last_modified_datetime']
    laptop['product_history']=[product_history,]
    laptop['site']='homeshop18'
    return laptop

def scrapAllLaptops():
    urls=getAllLaptopUrls()
    laptops=[]
    failed=[]
    dl.putUrls(urls,2)
    result=dl.download()
    for r in result:
        print r
        status=result[r][0]
        html=result[r][1]
        if html is None or len(html) < 2000:
            print "bad data with status %s found"%str(status)
            status = 0
            failed.append(r)
        if status > 199 and status < 400:
            laptop=getLaptopFromPage(string=html)
            if laptop:
                laptop['url']=r
                laptops.append(laptop)
                    
    while len(failed) > 0:
        dl.putUrls(failed, 2)
        result = dl.download()
        failed = []
        for r in result:
            print r
            status=result[r][0]
            html=result[r][1]
            if html is None or len(html) < 2000:
                print "bad data with status %s found"%str(status)
                status = 0
                failed.append(r)
            if status > 199 and status < 400:
                laptop=getLaptopFromPage(string = html)
                if laptop:
                    laptop['url'] = r
                    laptops.append(laptop)
                
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
