import downloader
import dom
import re
import urllib2
import math
import datetime
import pymongo
import requests
import util
import sys

if len(sys.argv)>1:
    proxy={'http':sys.argv[1]}
else:
    proxy={'http':'92.62.161.205:8080'}

siteurl='http://www.indiaplaza.com'
referer='http://www.indiaplaza.com/tablet-pc-2.htm?Category=tablet'
ajax_url='http://www.indiaplaza.com/buildfdppage.aspx'
ajax_post_data={'Gettabcontrol':'','store':'PC','category':'Tablet','subcategory':'Tablet','title':'','brand':''}
dl=downloader.Downloader()
dl.addHeaders({'Origin':siteurl,'Referer':siteurl})
if len(sys.argv)>2:
    proxy={'http':sys.argv[2]}
else:
    proxy={'http':'92.62.161.205:8080'}
    
shipping_pattern=re.compile('ships in (\d+)',re.I)
name_junk_pattern=re.compile('.+(\(.+\)).*')
brand_junk_pattern=re.compile('\(\d+\)')
sku_pattern=re.compile('pc-(pc\w+)-')
ram_pattern=re.compile('(\d{3} ?mb|(1|2) ?gb)',re.I)
ram_type_pattern=re.compile('DDR\d?',re.I)
storage_pattern=re.compile('(([3-9]|\d{2}) ?gb)',re.I)
screen_size_pattern=re.compile('\d\.?\d? ?(\'\'|\"|inch|hd)',re.I)
g_pattern=re.compile('\W((3|4) ?g)\W',re.I)
wifi_pattern=re.compile('wifi',re.I)
resolution_pattern=re.compile('\d{3} ?x ?\d{3}')
cpu_clock_speed_pattern=re.compile('\d\.?\d{0,2} ?ghz',re.I)
android_pattern=re.compile('android',re.I)
windows_pattern=re.compile('windows|w7',re.I)

def getBrands():
    html=requests.get(url=referer,proxies=proxy).content
    doc=dom.DOM(string=html)
    brand_path='//div[@id="divBrands"]/ul/li/a'
    brands=dict((brand_junk_pattern.sub('',link[0]).strip().lower(),'http://www.indiaplaza.com'+link[1]) for link in doc.getLinksWithXpath(brand_path))
    util.replaceKey(brands,'sony vaio','sony')
    return brands

def getTabletsFromBrandPage(url=None,string=None):
    tablets=[]
    tablet_block_path='//div[@class="skuRow"]'
    if string:
        page=dom.DOM(string=string)
    else:
        page=dom.DOM(url=url)
    tablet_blocks=page.getNodesWithXpath(tablet_block_path)
    img_path='.//div[@class="skuImg"]/a/img'
    name_path='.//div[@class="skuName"]/a'
    price_path='.//div[@class="ourPrice"]/span'
    shipping_path='.//span[@class="delDateQuest"]'
    ajax_specs_path='//div[@id="litDesc"]'
    features_path='.//div[@class="col2"]/ul/li'
    for tablet_block in tablet_blocks:
        tablet={}
        tablet['img_url']={'0':tablet_block.xpath(img_path)[0].get('src')}
        name=tablet_block.xpath(name_path)[0].text.encode('ascii','ignore').strip()
        junk=name_junk_pattern.search(name)
        if junk:
            junk=junk.group(1)
            name=name.replace(junk,'').strip()
        tablet['name']=name
        tablet['url']=siteurl+tablet_block.xpath(name_path)[0].get('href')
        price_string=tablet_block.xpath(price_path)[0].text
        tablet['price']=int(re.search('(\D)+(\d+)',price_string).group(2))
        shipping=shipping_pattern.search(tablet_block.xpath(shipping_path)[0].text)
        if shipping:
            tablet['shipping']=(shipping.group(1),)
        feature_nodes=tablet_block.xpath(features_path)
        features=[]
        tablet['specification']={}

        print tablet['url']
        sku=sku_pattern.search(tablet['url']).group(1)
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
        if len(specs_node)>0:
            specs=specs_node[0].text_content()
            m=ram_pattern.search(specs)
            if m:
                tablet['specification']['ram']=m.group()
            m=ram_type_pattern.search(specs)
            if m:
                tablet['specification']['memory type']=m.group()
            m=storage_pattern.search(specs)
            if m:
                tablet['specification']['storage']=m.group()
            m=screen_size_pattern.search(specs)
            if m:
                tablet['specification']['screen size']=m.group()
            m=g_pattern.search(specs)
            if m:
                tablet['specification']['g']=m.group(1).strip()
	    else:
		 m=g_pattern.search(name)
		 if m:
		    tablet['specification']['g']=m.group(1).strip()
            m=cpu_clock_speed_pattern.search(specs)
            if m:
                tablet['specification']['clock speed']=m.group()
            m=resolution_pattern.search(specs)
            if m:
                tablet['specification']['resolotion']=m.group()
            m=wifi_pattern.search(specs)
            if m:
                tablet['specification']['wifi']='yes'
            m=android_pattern.search(specs)
            if m:
                tablet['specification']['os']='Android'
            m=windows_pattern.search(specs)
            if m:
                tablet['specification']['os']='Windows'

        tablet['last_modified_datetime']=datetime.datetime.now()
        tablet['availability']=1
        product_history={}
        if 'price' in tablet:
            product_history['price']=tablet['price']
        if 'shipping' in tablet:
            product_history['shipping']=tablet['shipping']
        product_history['availability']=tablet['availability']
        product_history['datetime']=tablet['last_modified_datetime']
        tablet['product_history']=[product_history,]
        tablet['site']='indiaplaza'
        tablets.append(tablet)
    print len(tablets)
    return tablets

def getTabletsOfBrand(brand,get_details=False):
    html=requests.get(url=brand[1],proxies=proxy).content
    first_page=dom.DOM(string=html)
    tablets=[]
    tablets.extend(getTabletsFromBrandPage(string=first_page.html))
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
                tablets.extend(getTabletsFromBrandPage(string=html))
    for tablet in tablets:
        tablet['brand']=brand[0]
    print "%d tablets of brand %s"%(len(tablets),brand[0])
    return tablets

def scrapAllTablets():
    f=open('indiaplaza_tablets_log.txt','w')
    tablets=[]
    brands=getBrands()
    for brand in brands:
        tablets.extend(getTabletsOfBrand((brand,brands[brand])))
        f.write("Got tablets of brand %s\n"%brand)
        f.flush()
    return tablets

def insertIntoDB(log=True):
    con=pymongo.Connection('localhost',27017)
    db=con['abhiabhi']
    tablet_coll=db['scraped_tablets']
    inserted_count=0
    updated_count=0
    inserted_urls=[]
    updated_urls=[]
    tablets=scrapAllTablets()
    for tablet in tablets:
        try:
            tablet_coll.insert(tablet,safe=True)
            inserted_count+=1
            inserted_urls.append(tablet['url'])
        except pymongo.errors.DuplicateKeyError:
            upd={'last_modified_datetime':datetime.datetime.now()}
            if 'availability'in tablet:
                upd['availability']=tablet['availability']
            if 'price' in tablet:
                upd['price']=tablet['price']
            if 'shipping' in tablet:
                upd['shipping']=tablet['shipping']
	    if 'offer' in tablet:
		upd['offer']=tablet['offer']
	    else:
		upd['offer']=''
            tablet_coll.update({'url':tablet['url']},{'$push':{'product_history':tablet['product_history'][0]},'$set':upd})
            updated_count+=1
            updated_urls.append(tablet['url'])
    if log:
        scrap_log=db['scrap_log']
        log={'siteurl':siteurl,'datetime':datetime.datetime.now(),'product':'tablet','products_updated_count':updated_count,'products_inserted_count':inserted_count,'products_updated_urls':updated_urls,'products_inserted_urls':inserted_urls}
        scrap_log.insert(log)

    print "%d inserted and %d updated"%(inserted_count,updated_count)
    
if __name__=='__main__':
    insertIntoDB()
