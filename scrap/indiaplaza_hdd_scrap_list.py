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
referer='http://www.indiaplaza.com/hard-drives-pc-2.htm?Category=data-storage'
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'','store':'PC','category':'Data+Storage','subcategory':'','title':'','brand':''}

dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':referer})
if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy=None
    
brand_junk_pattern=re.compile('\(\d+\)')
sku_pattern=re.compile('pc-(\w+)-')
shipping_pattern=re.compile('ships in (\d+)',re.I)
capacity_pattern=re.compile('(\d+\.?\d?) ?(g|t)b',re.I)
interface_pattern=re.compile('usb ?\d\.?\d?',re.I)

def getBrands():
    if proxy:
	html=requests.get(url=referer,proxies=proxy).content
    else:
	html=requests.get(url=referer).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip(),'http://www.indiaplaza.com'+link[1]) for link in doc.getLinksWithXpath(brand_path))
    return brands

def getHDDsFromBrandPage(url=None,string=None):
    hdds=[]
    hdd_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    hdd_blocks=page.getNodesWithXpath(hdd_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a'
    price_path='.//div[@class="ourPrice"]/span'
    shipping_path='.//span[@class="delDateQuest"]'
    features_path='.//div[@class="col2"]/ul/li'
    for hdd_block in hdd_blocks:
        hdd={}
        hdd['img_url']={'0':hdd_block.xpath(img_path)[0].get('src')}
        name=hdd_block.xpath(name_path)[0].text.encode('ascii','ignore').strip()
        hdd['name']=name
        hdd['url']=siteurl+hdd_block.xpath(name_path)[0].get('href')
        price_string=hdd_block.xpath(price_path)[0].text
        hdd['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
        shipping=shipping_pattern.search(hdd_block.xpath(shipping_path)[0].text)
        if shipping:
            hdd['shipping']=(shipping.group(1),)
	hdd['availability']=1
        feature_nodes=hdd_block.xpath(features_path)
        features=[]
        hdd['specification']={}
    
        m=interface_pattern.search(name)
        if m:
            hdd['specification']['interface']=m.group()

        m=capacity_pattern.search(name) 
        if m:
            hdd['specification']['capacity']=m.group()
	
	if set(hdd['specification'].keys()).issubset(set(['interface','capacity'])) == False:
	    print hdd['url']
	    sku=sku_pattern.search(hdd['url']).group(1)
	    ajax_post_data['sku']=sku
	    while(True):
		try:
		    if proxy:
			ajax_res=requests.post(ajax_url,data=ajax_post_data,proxies=proxy).content
		    else:
			ajax_res=requests.post(ajax_url,data=ajax_post_data).content
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
	    if 'capacity' not in hdd['specification']:
		m=capacity_pattern.search(specs)
		if m:
		    hdd['specification']['capacity']=m.group()
	    if 'interface' not in hdd['specification']:
		m=capacity_pattern.search(specs)
		if m:
		    hdd['specification']['interface']=m.group()
	
        hdd['last_modified_datetime']=datetime.datetime.now()
        product_history={}
        if 'price' in hdd:
            product_history['price']=hdd['price']
        if 'shipping' in hdd:
            product_history['shipping']=hdd['shipping']
        product_history['availability']=hdd['availability']
        product_history['datetime']=hdd['last_modified_datetime']
        hdd['product_history']=[product_history,]
        hdd['site']='indiaplaza'
        hdds.append(hdd)
    print len(hdds)
    return hdds

def getHDDsOfBrand(brand,get_details=False):
    if proxy:
	html=requests.get(url=brand[1],proxies=proxy).content
    else:
	html=requests.get(url=brand[1]).content
    first_page=dom.DOM(string=html)
    hdds=[]
    hdds.extend(getHDDsFromBrandPage(string=first_page.html))
    count_path='//div[@class="prodNoArea"]'
    count_string=first_page.getNodesWithXpath(count_path)[0].text
    count=int(re.search('Showing.+of (\d+)',count_string).group(1))
    if count>20:
        num_pages=int(math.ceil(count/20.0))
        page_urls=[brand[1]+'&PageNo='+str(n) for n in xrange(2,num_pages+1)]
        dl.putUrls(page_urls)
        result=dl.download()
        for r in result:
            status=result[r][0]
            html=result[r][1]
            if status > 199 and status < 400:
                hdds.extend(getHDDsFromBrandPage(string=html))
    for hdd in hdds:
        hdd['brand']=brand[0]
    print "%d hdds of brand %s"%(len(hdds),brand[0])
    return hdds

def scrapAllHDDs():
    f=open('indiaplaza_hdds_log.txt','w')
    hdds=[]
    brands=getBrands()
    for brand in brands:
        hdds.extend(getHDDsOfBrand((brand,brands[brand])))
        f.write("Got hdds of brand %s\n"%brand)
        f.flush()
    return hdds

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    db.authenticate('root','hpalpha1911')
    hdd_coll=db['scraped_harddisks']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    hdds=scrapAllHDDs()
    for hdd in hdds:
        try:
            hdd_coll.insert(hdd,safe=True)
            inserted_count+=1
            inserted_urls.append(hdd['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability'in hdd:
                upd['availability']=hdd['availability']
            if 'price' in hdd:
                upd['price']=hdd['price']
            if 'shipping' in hdd:
                upd['shipping']=hdd['shipping']
	    if 'offer' in hdd:
		upd['offer']=hdd['offer']
	    else:
		upd['offer']=''
            hdd_coll.update({'url':hdd['url']},{'$push':{'product_history':hdd['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(hdd['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'harddisk','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
