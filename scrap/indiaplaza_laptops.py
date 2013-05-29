import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import sys

siteurl='http://www.indiaplaza.com'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy={'http':'92.62.161.205:8080'}

laptop_home_page='http://www.indiaplaza.com/laptops-pc-1.htm'
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'','store':'PC','category':'Laptops','subcategory':'','title':'','brand':''}

shipping_pattern=re.compile('ships in (\d+)',re.I)
name_pattern=re.compile('(^[\w \d\[\]-]+?)(\/|2nd|3rd|Ci|dual|quad|c2d|core|dc|pentium|intel|amd|celeron|pdc|\(|$|#)',re.I)
brand_junk_pattern=re.compile('\(\d+\)')
sku_pattern=re.compile('pc-(\w+)-')
ram_pattern=re.compile('(1\d|(?:\W|^)\d) ?gb',re.I)
table_ram_pattern=re.compile('Memory standard ?(\d+ ?gb)',re.I)
ram_type_pattern=re.compile('DDR\d?',re.I)
hdd_capacity_pattern=re.compile('(\d{3,4} ?G ?(ssd)?|\d\.?\d? ?T)B',re.I)
hdd_rpm_pattern=re.compile('\d{4} ?rpm',re.I)
cpu_clockspeed_pattern=re.compile('\d\.?\d{0,2} ?GHz',re.I)
cpu_name_pattern=re.compile('((i(3|5|7))|pdc|pentium dual core|celeron dual core|atom dual core|core ?2|amd|apu dual core)\W',re.I)
cpu_gen_pattern=re.compile('\d((nd)|(rd)|(st))? gen\w*',re.I)
display_size_pattern=re.compile('1[0-8]\.?\d? ?(\'\'?|\"|inch)',re.I)
os_pattern=re.compile('windows|w7|dos|linux',re.I)

def getBrands():
    html=requests.get(url=laptop_home_page,proxies=proxy).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(),'http://www.indiaplaza.com'+link[1]) for link in doc.getLinksWithXpath(brand_path))
    util.replaceKey(brands,'sony vaio','sony')
    return brands

def getLaptopsFromBrandPage(url=None,string=None):
    laptops=[]
    laptop_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    laptop_blocks=page.getNodesWithXpath(laptop_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a'
    price_path='.//div[@class="ourPrice"]/span'
    shipping_path='.//span[@class="delDateQuest"]'
    name_junk_pattern=re.compile('.+(\(.+\)).*')
    features_path='.//div[@class="col2"]/ul/li'
    ajax_specs_path='//div[@id="litDesc"]/table'
    for laptop_block in laptop_blocks:
        laptop={}
        laptop['img_url']={'0':laptop_block.xpath(img_path)[0].get('src')}
        name=laptop_block.xpath(name_path)[0].text.encode('ascii','ignore').strip()
        m=name_pattern.search(name)
        if m:
            laptop['name']=m.group(1).strip(' -(#')
        else:
            laptop['name']=name
        laptop['url']=siteurl+laptop_block.xpath(name_path)[0].get('href')
        price_string=laptop_block.xpath(price_path)[0].text
        laptop['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
        shipping=shipping_pattern.search(laptop_block.xpath(shipping_path)[0].text)
        if shipping:
            laptop['shipping']=(shipping.group(1),)
	laptop['availability']=1
        laptop['specification']={}
	print laptop['url']
        sku=sku_pattern.search(laptop['url']).group(1)
        ajax_post_data['sku']=sku
	while(True):
	    try:
		ajax_res=requests.post(ajax_url,data=ajax_post_data,proxies=proxy).content
		break
	    except requests.exceptions.ConnectionError:
		print 'exception raised'
		pass
	    
        ajax_dom=dom.DOM(string=ajax_res)
        specs_node=ajax_dom.getNodesWithXpath(ajax_specs_path)
	if len(specs_node)==0:						#for pages that dont have specification table
	    specs_node=ajax_dom.getNodesWithXpath('//div[@id="litDesc"]')
        
	if len(specs_node)>0:
            specs=specs_node[0].text_content()
	    ram=False
	    m=ram_pattern.search(name)
	    if m:
		ram=m.group()
		
	    if ram==False:
		m=table_ram_pattern.search(specs)
		if m:
		    ram=m.group(1)
		else:
		    m=ram_pattern.search(specs)
		    if m:
			ram=m.group()

	    ram_type=False
	    m=ram_type_pattern.search(specs)
	    if m:
		ram_type=m.group()
		    
	    if ram_type==False:
		m=ram_type_pattern.search(name)
		if m:
		    ram_type=m.group()

	    cap=False
	    m=hdd_capacity_pattern.search(specs)
	    if m:
		cap=m.group()
		
	    if cap==False:
		m=hdd_capacity_pattern.search(name)
		if m:
		    cap=m.group()

	    size=False
	    m=display_size_pattern.search(specs)
	    if m:
		size=m.group()
		    
	    if size==False:
		m=display_size_pattern.search(name)
		if m:
		    size=m.group()

	    speed=False
	    m=cpu_clockspeed_pattern.search(specs)
	    if m:
		speed=m.group()
		
	    if speed==False:
		m=cpu_clockspeed_pattern.search(name)
		if m:
		    speed=m.group()

	    gen=False
	    m=cpu_gen_pattern.search(specs)
	    if m:
		gen=m.group()
		    
	    if gen==False:
		m=cpu_gen_pattern.search(name)
		if m:
		    gen=m.group()

	    cpu_name=False
	    m=cpu_name_pattern.search(specs)
	    if m:
		cpu_name=m.group()
		    
	    if cpu_name==False:
		m=cpu_name_pattern.search(name)
		if m:
		    cpu_name=m.group()

	    os=False
	    m=os_pattern.search(specs)
	    if m:
		os=m.group()
		    
	    if os==False:
		m=os_pattern.search(name)
		if m:
		    os=m.group()
        
	    if ram:
		laptop['specification']['memory']={}
		laptop['specification']['memory']['ram']=ram

	    if ram_type:
		if 'memory' in laptop['specification']:
		    laptop['specification']['memory']['memory type']=ram_type
		else:
		    laptop['specification']['memory']={}
		    laptop['specification']['memory']['memory type']=ram_type
	    
	    if cap:
		laptop['specification']['storage']={}
		laptop['specification']['storage']['capacity']=cap

	    if size:
		laptop['specification']['display']={}
		laptop['specification']['display']['size']=size

	    if speed:
		laptop['specification']['processor']={} 
		laptop['specification']['processor']['clock speed']=speed

	    if cpu_name:
		if 'processor' in laptop['specification']:
		    laptop['specification']['processor']['processor']=cpu_name
		else:
		    laptop['specification']['processor']={}
		    laptop['specification']['processor']['processor']=cpu_name

	    if gen:
		if 'processor' in laptop['specification']:
		    laptop['specification']['processor']['generation']=gen
		else:
		    laptop['specification']['processor']={}
		    laptop['specification']['processor']['generation']=gen

	    if os:
		laptop['specification']['software']={}
		laptop['specification']['software']['os']=os

        laptop['last_modified_datetime']=datetime.datetime.now()
        product_history={}
        if 'price' in laptop:
            product_history['price']=laptop['price']
        if 'shipping' in laptop:
            product_history['shipping']=laptop['shipping']
        product_history['availability']=laptop['availability']
        product_history['datetime']=laptop['last_modified_datetime']
        laptop['product_history']=[product_history,]
        laptop['site']='indiaplaza'
        laptops.append(laptop)
    print len(laptops)
    return laptops

def getLaptopsOfBrand(brand,get_details=False):
    html=requests.get(url=brand[1],proxies=proxy).content
    first_page=dom.DOM(string=html)
    laptops=[]
    laptops.extend(getLaptopsFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('Showing.+of (\d+)',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls)
        result=dl.download(proxy=proxy)
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                laptops.extend(getLaptopsFromBrandPage(string=html))
    for laptop in laptops:
        laptop['brand']=brand[0]
    print "%d laptops of brand %s"%(len(laptops),brand[0])
    return laptops

def scrapAllLaptops():
    f=open('indiaplaza_laptops_log.txt','w')
    laptops=[]
    brands=getBrands()
    for brand in brands:
        laptops.extend(getLaptopsOfBrand((brand,brands[brand])))
        f.write("Got laptops of brand %s\n"%brand)
        f.flush()
    return laptops

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    laptop_coll=db['scraped_laptops']
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

    
