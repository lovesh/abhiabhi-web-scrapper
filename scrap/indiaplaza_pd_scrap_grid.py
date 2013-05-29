import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import sys
import util

siteurl='http://www.indiaplaza.com'
pd_home = 'http://www.indiaplaza.com/pen-drives-pc-2.htm?Category=data-storage'
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':pd_home})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy=None
    
count_pattern = re.compile('\d+ of (\d+)', re.I)
brand_junk_pattern=re.compile('\(\d+\)')
name_junk_pattern=re.compile('Pendrive',re.I)
cruze_pattern=re.compile('\((cruze\w+)\)',re.I)
shipping_pattern = re.compile('in (\d+)', re.I)
price_pattern = re.compile('\d+')
sku_pattern = re.compile('pc-(\w+)-')
capacity_pattern=re.compile('\d+ ?(G|T)B',re.I)
interface_pattern=re.compile('usb ?\d\.?\d?',re.I)

ajax_url = 'http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data = {'Gettabcontrol':'','store':'PC','category':'Data+Storage','subcategory':'','title':'','brand':''}

def getBrands():
    if proxy:
        html=requests.get(url = pd_home, proxies = proxy).content
    else:
        html=requests.get(url = pd_home).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(), siteurl + link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands
  
def getPDUrlsFromBrandPage(url = None, string = None):
    doc = dom.DOM(string = string)
    pd_url_path = '//div[@class="skuImg"]/a'
    urls = [siteurl + link[1] for link in doc.getLinksWithXpath(pd_url_path)]
    return urls
    
def getPDsOfBrand(brand,get_details=False):
    if proxy:
        html = requests.get(url = brand[1], proxies=proxy).content
    else:
        html = requests.get(url = brand[1]).content
        
    first_page = dom.DOM(string=html)
    pd_urls = []
    pd_urls.extend(getPDUrlsFromBrandPage(string = first_page.html))
    count_path = '//div[@class="prodNoArea"]'
    count_string = first_page.getNodesWithXpath(count_path)[0].text
    count = int(count_pattern.search(count_string).group(1))
    if count>20:
        num_pages = int(math.ceil(count/20.0))
        page_urls = [brand[1] + '&PageNo=' + str(n) for n in xrange(2, num_pages+1)]
        dl.putUrls(page_urls)
	if proxy:
	    result = dl.download(proxy=proxy)
	else:
	    result = dl.download()
        for r in result:
            status = result[r][0]
            html = result[r][1]
            if status > 199 and status < 400:
                pd_urls.extend(getPDUrlsFromBrandPage(string = html))
    
    pds = []
    
    dl.putUrls(pd_urls)
    pds = []
    if proxy:
        result = dl.download(proxy = proxy)
    else:
        result = dl.download()
    
    for r in result:
	status = result[r][0]
	html = result[r][1]
	if status > 199 and status < 400:
	    print r
	    pd = getPDFromPage(string=html)
	    if pd:
		pd['url'] = r
		pds.append(pd)
    
    for pd in pds:
        pd['brand']=brand[0]
    print "%d pds of brand %s"%(len(pds),brand[0])
    return pds

def getPDFromPage(url = None, string = None):
    pd = {}
    
    if url:
	if proxy:
	    html = requests.get(url, proxies = proxy).content
	else:
	    html = requests.get(url).content
    else:
	html = string
    
    doc = dom.DOM(string = html)
    
    url_path = '//link[@rel="canonical"]'
    name_path = '//div[@class="descColSkuNamenew"]/h1'
    warranty_path = '//div[@class="warrantyBg"]'
    price_path = '//span[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_lblOurPrice"]/span[@class="blueFont"]'
    shipping_path = '//span[@class="delDateQuest"]'
    availability_path = '//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_AddtoCartDiv"]'
    description_path = '//div[@id="ContentPlaceHolder1_FinalControlValuesHolder_ctl00_FDPMainSection_fdpDescDiv"]'
    ajax_specs_path = '//div[@id="litDesc"]/table'
    img_path = '//img[@id="my_image"]'
    
    image = doc.getImgUrlWithXpath(img_path)
    if len(image) > 0:
        pd['img_url'] = {'0':image[0]}
	
    url = doc.getNodesWithXpath(url_path)
    if len(url) > 0:
	pd['url'] = url[0].get('href')
    name=doc.getNodesWithXpath(name_path)[0].text.encode('ascii','ignore').strip()
    name=name_junk_pattern.sub('',name)
    cruzer=cruze_pattern.search(name)
    if cruzer:
	cruzer=cruzer.group(1)
	name=re.sub('\(','',name)
	name=re.sub('\)','',name)
    
    pd['name']=name
    warranty = doc.getNodesWithXpath(warranty_path)
    if len(warranty) > 0:
        pd['warranty'] = warranty[0].text_content()
    price = doc.getNodesWithXpath(price_path)
    if len(price) > 0:
        pd['price'] = price_pattern.search(price[0].text).group()
    description = doc.getNodesWithXpath(description_path)
    if len(description) > 0:
        pd['description'] = description[0].text_content()
	
    availability = doc.getNodesWithXpath(availability_path)
    if len(availability) > 0:
        presence = availability[0].get('style')
        if presence == '"display:block;"':
            pd['availability'] = 1
            shipping = doc.getNodesWithXpath(shipping_path)
            if len(shipping) > 0:
                shipping = shipping_pattern.search(shipping[0].text)
                if shipping:
		    pd['shipping']=(shipping.group(1), )
	else:
	    pd['availability'] = 0
    
    sku = sku_pattern.search(pd['url']).group(1)
    ajax_post_data['sku'] = sku
    while(True):
	try:
	    if proxy:
		ajax_res = requests.post(ajax_url, data = ajax_post_data,proxies=proxy).content
	    else:
		ajax_res = requests.post(ajax_url, data = ajax_post_data).content
	    break
	except requests.exceptions.ConnectionError:
	    print 'exception raised'
	    pass
    specification = {}
    ajax_dom = dom.DOM(string = ajax_res)
    specs_node = ajax_dom.getNodesWithXpath(ajax_specs_path)
    if len(specs_node) > 0:
        divs = specs_node[0].xpath('//div')
        for div in divs:
            clas = div.get('class')
            if clas == 'fdpSpecCol1r':
                key = div.text_content().strip().lower()
            if clas == 'fdpSpecCol2r':
                value = div.text_content().strip()
                specification[key] = value
    
    util.replaceKey(specification, 'storage capacity', 'capacity')
    
    pd['specification'] = specification
    
    if 'capacity' not in pd['specification']:
	m=capacity_pattern.search(name) 
	if m:
	    pd['specification']['capacity']=m.group()
	    
    if 'interface' not in pd['specification']:
	m = interface_pattern.search(name)
	if m:
	    pd['specification']['interface']=m.group()
    
    pd['last_modified_datetime'] = datetime.datetime.now()
    product_history = {}
    if 'price' in pd:
        product_history['price'] = pd['price']
    if 'shipping' in pd:
        product_history['shipping'] = pd['shipping']
    product_history['availability'] = pd['availability']
    product_history['datetime'] = pd['last_modified_datetime']
    pd['product_history'] = [product_history, ]
    pd['site']='indiaplaza'
    
    return pd

def scrapAllPDs():
    f=open('indiaplaza_pds_log.txt','w')
    pds=[]
    brands=getBrands()
    for brand in brands:
        pds.extend(getPDsOfBrand((brand,brands[brand])))
        f.write("Got pds of brand %s\n"%brand)
        f.flush()
    return pds
    
def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    pd_coll=db['scraped_pendrives']
    pd_coll.create_index('url',unique=True)
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    pds=scrapAllPDs()
    for pd in pds:
        try:
            pd_coll.insert(pd,safe=True)
            inserted_count+=1
            inserted_urls.append(pd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability' in pd:
                upd['availability']=pd['availability']
            if 'price' in pd:
                upd['price']=pd['price']
            if 'shipping' in pd:
                upd['shipping']=pd['shipping']
	    if 'offer' in pd:
                upd['offer']=pd['offer']
	    else:
		upd['offer']=''
            pd_coll.update({'url':pd['url']},{'$push':{'product_history':pd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(pd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'pendrive','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)
	
    print "%d inserted and %d updated"%(inserted_count,updated_count)

if __name__=='__main__':
    insertIntoDB()

