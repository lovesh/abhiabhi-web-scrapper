from gevent import monkey
monkey.patch_all()
import dom
import downloader
import re
import pymongo
import datetime
import util

siteurl = 'http://www.gsmarena.com/'

dl=downloader.Downloader()
dl.addHeaders({'Host':'www.gsmarena.com','Referer':siteurl})

def getAllBrands():
	brand_path = '//div[@id="brandmenu"]/ul/li/a'
	doc = dom.DOM(url = siteurl)
	brands = [[link[0], siteurl + link[1]] for link in doc.getLinksWithXpath(brand_path)]
	return brands;

def getMobileFromPage(url = None, string = None):
	mobile = {}
	
	if url:
		doc=dom.DOM(url=url)
	else:
		doc=dom.DOM(string=string)
        
	name_path = '//div[@id="ttl"]/h1'
	img_url_path = '//div[@id="specs-cp-pic"]//img'
	specs_path = '//div[@id="specs-list"]/table'
	
	mobile['name'] = doc.getNodesWithXpath(name_path)[0].text_content()
	mobile['img_url'] = {'0' : doc.getImgUrlWithXpath(img_url_path)[0]}
	mobile['site'] = 'gsmarena'
	mobile['root_category'] = 'mobile'
	mobile['categories'] = ['mobile',]
	specification = {}
	specs_tables = doc.getNodesWithXpath(specs_path)
	for specs_table in specs_tables: 
		heading = specs_table.xpath('./tr[1]/th')[0].text_content().lower()
		specification[heading] = {}
		trs = specs_table.xpath('.//tr')
		for tr in trs:
			tds = tr.xpath('.//td')
			if len(tds) > 1:
				specification[heading][tds[0].text_content().strip().lower()] = tds[1].text_content().strip()
	
	util.replaceKey(specification['sound'], '3.5mm jack', '35 mm jack')
	mobile['specification'] = specification
	
	mobile['upcoming'] = 1
	mobile['added_datetime'] = datetime.datetime.now()
	mobile['status'] = 1 
	
	return mobile
				
	
def getMobilesOfBrand(brand, upcoming = True):
	mobiles = []
	pattern = re.compile('-(\d+).php')
	brand_id = pattern.search(brand[1]).group(1).strip()
	upcoming_url = pattern.sub('', brand[1]) + '-f-' + brand_id + '-3.php'
	print upcoming_url
	doc = dom.DOM(url = upcoming_url)
	mobile_path = '//div[@class="makers"]/ul/li/a'
	mobile_urls = [siteurl + link[1] for link in doc.getLinksWithXpath(mobile_path)]
	#print mobile_urls
	dl.putUrls(mobile_urls, 5)
	pages = dl.download()
	for p in pages:
		print p
		status=pages[p][0]
		html=pages[p][1]
		if status > 199 and status < 400:
			mobile = getMobileFromPage(string = html)
			mobile['url'] = p
			mobile['brand'] = brand[0].lower()
			mobiles.append(mobile)
	print "%d mobiles of brand %s"%(len(mobiles), brand[0])
	return mobiles
	
	 
def scrapAllMobiles():
	mobiles = []
	brands = getAllBrands()
	for brand in brands:
		mobiles.extend(getMobilesOfBrand(brand))
	print "found %d mobiles "%len(mobiles)
	return mobiles
	
def insertIntoDB():
	mobiles = scrapAllMobiles()
	con = pymongo.Connection('localhost',27017)
	db = con['new_final']
	mobile_coll = db['products']
	for mobile in mobiles:
		try:
			mobile_coll.insert(mobile, safe = True)
		except:
			pass
	print "inserted %d mobiles "%len(mobiles)
	
if __name__ == '__main__':
	insertIntoDB()
	
